package ql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Parser represents a samql parser.
type Parser struct {
	s *bufScanner
	// Params, if provided, will be used to replace any bound parameters.
	Params map[string]interface{}
}

// NewParser returns a new Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{s: newBufScanner(r)}
}

// NewParserFromStr returns a new Parser that reads from s.
func NewParserFromStr(s string) *Parser {
	return NewParser(strings.NewReader(s))
}

// ParseStatement parses an samql string and returns a Statement AST object.
func (p *Parser) ParseStatement() (Statement, error) {
	tok, pos, lit := p.scanIgnoreWhiteSpace()
	if tok == SELECT {
		return p.parseSelectStatement()
	}

	return nil, newParseError(tokstr(tok, lit), []string{"SELECT"}, pos)
}

// ParseExpr parses an expression.
func (p *Parser) ParseExpr() (Expr, error) {
	var err error
	// Dummy root node.
	root := &BinaryExpr{}

	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	root.RHS, err = p.parseUnaryExpr()
	if err != nil {
		return nil, err
	}

	// Loop over operations and unary exprs and build a tree based on
	// precedence.
	for {
		// If the next token is NOT an operator then return the expression.
		op, _, _ := p.scanIgnoreWhiteSpace()
		if !op.isOperator() {
			p.unscan()
			return root.RHS, nil
		}

		// Otherwise parse the next expression.
		var rhs Expr
		if op == EQREGEX || op == NEQREGEX {
			// RHS of a regex operator must be a regular expression.
			if rhs, err = p.parseRegex(); err != nil {
				return nil, err
			}
			// parseRegex can return an empty type, but we need it to be
			// present
			if rhs.(*RegexLiteral) == nil {
				tok, pos, lit := p.scanIgnoreWhiteSpace()
				return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
			}
		} else {
			if rhs, err = p.parseUnaryExpr(); err != nil {
				return nil, err
			}
		}

		// Find the right spot in the tree to add the new expression by
		// descending the RHS of the expression tree until we reach the last
		// BinaryExpr or a BinaryExpr whose RHS has an operator with
		// precedence >= the operator being added.
		for node := root; ; {
			r, ok := node.RHS.(*BinaryExpr)
			if !ok || r.Op.Precedence() >= op.Precedence() {
				// Add the new expression here and break.
				node.RHS = &BinaryExpr{LHS: node.RHS, RHS: rhs, Op: op}
				break
			}
			node = r
		}
	}
}

// parseIdent parses an identifier.
func (p *Parser) parseIdent() (string, error) {
	tok, pos, lit := p.scanIgnoreWhiteSpace()
	if tok != IDENT {
		return "", newParseError(
			tokstr(tok, lit), []string{"identifier"}, pos)
	}
	return lit, nil
}

// parseVarRef parses a reference to a table or field.
func (p *Parser) parseVarRef() (*VarRef, error) {
	// Parse the segments of the variable ref.
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}

	return &VarRef{Val: ident}, nil
}

// parseSelectStatement parses a select string and returns a Statement AST
// object. This function assumes the SELECT token has already been consumed.
func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}
	var err error

	// Parse fields: "FIELD+".
	if stmt.Fields, err = p.parseFields(); err != nil {
		return nil, err
	}

	// Parse source: "FROM".
	if tok, pos, lit := p.scanIgnoreWhiteSpace(); tok != FROM {
		return nil, newParseError(tokstr(tok, lit), []string{"FROM"}, pos)
	}
	if stmt.Source, err = p.parseSource(); err != nil {
		return nil, err
	}

	// Parse condition: "WHERE EXPR".
	if stmt.Condition, err = p.parseCondition(); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseFields parses a list of one or more fields.
func (p *Parser) parseFields() (Fields, error) {
	var fields Fields

	for {
		// Parse the field.
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}

		// Add new field.
		fields = append(fields, f)

		// If there's not a comma next then stop parsing fields.
		if tok, _, _ := p.scan(); tok != COMMA {
			p.unscan()
			break
		}
	}
	return fields, nil
}

// parseField parses a single field.
func (p *Parser) parseField() (*Field, error) {
	f := &Field{}

	// Attempt to parse a regex.
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		f.Expr = re
	} else {
		_, pos, _ := p.scanIgnoreWhiteSpace()
		p.unscan()
		// Parse the expression first.
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		var c fieldValidator
		Walk(&c, expr)
		if c.foundInvalid {
			return nil, fmt.Errorf("invalid operator %s in SELECT clause at line %d, char %d; operator is intended for WHERE clause", c.badToken, pos.Line+1, pos.Char+1)
		}
		f.Expr = expr
	}

	// Parse the alias if the current and next tokens are "WS AS".
	alias, err := p.parseAlias()
	if err != nil {
		return nil, err
	}
	f.Alias = alias

	// Consume all trailing whitespace.
	p.consumeWhitespace()

	return f, nil
}

// parseAlias parses the "AS IDENT" alias for fields and dimensions.
func (p *Parser) parseAlias() (string, error) {
	// Check if the next token is "AS". If not, then Unscan and exit.
	if tok, _, _ := p.scanIgnoreWhiteSpace(); tok != AS {
		p.unscan()
		return "", nil
	}

	// Then we should have the alias identifier.
	lit, err := p.parseIdent()
	if err != nil {
		return "", err
	}
	return lit, nil
}

func (p *Parser) parseSource() (Source, error) {
	ident, err := p.parseIdent()
	if err != nil {
		return nil, err
	}

	return &Table{Name: ident}, nil
}

// parseCondition parses the "WHERE" clause of the query, if it exists.
func (p *Parser) parseCondition() (Expr, error) {
	// Check if the WHERE token exists.
	if tok, _, _ := p.scanIgnoreWhiteSpace(); tok != WHERE {
		p.unscan()
		return nil, nil
	}

	// Scan the identifier for the source.
	expr, err := p.ParseExpr()
	if err != nil {
		return nil, err
	}

	return expr, nil
}

// parseUnaryExpr parses an non-binary expression.
func (p *Parser) parseUnaryExpr() (Expr, error) {
	// If the first token is a LPAREN then parse it as its own grouped
	// expression.
	if tok, _, _ := p.scanIgnoreWhiteSpace(); tok == LPAREN {
		expr, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}

		// Expect an RPAREN at the end.
		if tok, pos, lit := p.scanIgnoreWhiteSpace(); tok != RPAREN {
			return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
		}

		return &ParenExpr{Expr: expr}, nil
	}
	p.unscan()

	// Read next token.
	tok, pos, lit := p.scanIgnoreWhiteSpace()
	switch tok {
	case IDENT:
		// If the next immediate token is a left parentheses, parse as
		// function call. Otherwise parse as a variable reference.
		if tok0, _, _ := p.scan(); tok0 == LPAREN {
			return p.parseCall(lit)
		}

		p.unscan() // Unscan the last token (wasn't an LPAREN)
		p.unscan() // Unscan the IDENT token

		// Parse it as a VarRef.
		return p.parseVarRef()
	case STRING:
		return &StringLiteral{Val: lit}, nil
	case NUMBER:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, &ParseError{Message: "unable to parse number", Pos: pos}
		}
		return &NumberLiteral{Val: v}, nil
	case INTEGER:
		v, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			// The literal may be too large to fit into an int64. If it is,
			// use an unsigned integer.  The check for negative numbers is
			// handled somewhere else so this should always be a positive
			// number.
			if v, err := strconv.ParseUint(lit, 10, 64); err == nil {
				return &UnsignedLiteral{Val: v}, nil
			}
			return nil, &ParseError{Message: "unable to parse integer", Pos: pos}
		}
		return &IntegerLiteral{Val: v}, nil
	case TRUE, FALSE:
		return &BooleanLiteral{Val: (tok == TRUE)}, nil
	case MUL:
		return &Wildcard{}, nil
	case REGEX:
		re, err := regexp.Compile(lit)
		if err != nil {
			return nil, &ParseError{Message: err.Error(), Pos: pos}
		}
		return &RegexLiteral{Val: re}, nil
	case BOUNDPARAM:
		k := strings.TrimPrefix(lit, "$")
		if len(k) == 0 {
			return nil, errors.New("empty bound parameter")
		}

		v := p.Params[k]
		if v == nil {
			return nil, fmt.Errorf("missing parameter: %s", k)
		}

		switch v := v.(type) {
		case float64:
			return &NumberLiteral{Val: v}, nil
		case int64:
			return &IntegerLiteral{Val: v}, nil
		case string:
			return &StringLiteral{Val: v}, nil
		case bool:
			return &BooleanLiteral{Val: v}, nil
		default:
			return nil, fmt.Errorf("unable to bind parameter with type %T", v)
		}
	case ADD, SUB:
		mul := 1
		if tok == SUB {
			mul = -1
		}

		tok0, pos0, lit0 := p.scanIgnoreWhiteSpace()
		switch tok0 {
		case NUMBER, INTEGER, LPAREN, IDENT:
			// Unscan the token and use parseUnaryExpr.
			p.unscan()

			lit, err := p.parseUnaryExpr()
			if err != nil {
				return nil, err
			}

			switch lit := lit.(type) {
			case *NumberLiteral:
				lit.Val *= float64(mul)
			case *IntegerLiteral:
				lit.Val *= int64(mul)
			case *UnsignedLiteral:
				if tok == SUB {
					// Because of twos-complement integers and the method we
					// parse, math.MinInt64 will be parsed as an
					// UnsignedLiteral because it overflows an int64, but it
					// fits into int64 if it were parsed as a negative number
					// instead.
					if lit.Val == uint64(math.MaxInt64+1) {
						return &IntegerLiteral{Val: int64(-lit.Val)}, nil
					}
					return nil, fmt.Errorf("constant -%d underflows int64", lit.Val)
				}
			case *VarRef, *Call, *ParenExpr:
				// Multiply the variable.
				return &BinaryExpr{
					Op:  MUL,
					LHS: &IntegerLiteral{Val: int64(mul)},
					RHS: lit,
				}, nil
			default:
				panic(fmt.Sprintf("unexpected literal: %T", lit))
			}
			return lit, nil
		default:
			return nil, newParseError(tokstr(tok0, lit0), []string{"identifier", "number", "duration", "("}, pos0)
		}
	default:
		return nil, newParseError(tokstr(tok, lit), []string{"identifier", "string", "number", "bool"}, pos)
	}
}

// parseRegex parses a regular expression.
func (p *Parser) parseRegex() (*RegexLiteral, error) {
	nextRune := p.peekRune()
	if isWhitespace(nextRune) {
		p.consumeWhitespace()
	}

	// If the next character is not a '/', then return nils.
	nextRune = p.peekRune()
	if nextRune != '/' {
		return nil, nil
	}

	tok, pos, lit := p.s.ScanRegex()

	if tok == BADESCAPE {
		msg := fmt.Sprintf("bad escape: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok == BADREGEX {
		msg := fmt.Sprintf("bad regex: %s", lit)
		return nil, &ParseError{Message: msg, Pos: pos}
	} else if tok != REGEX {
		return nil, newParseError(tokstr(tok, lit), []string{"regex"}, pos)
	}

	re, err := regexp.Compile(lit)
	if err != nil {
		return nil, &ParseError{Message: err.Error(), Pos: pos}
	}

	return &RegexLiteral{Val: re}, nil
}

// parseCall parses a function call.
// This function assumes the function name and LPAREN have been consumed.
func (p *Parser) parseCall(name string) (*Call, error) {
	name = strings.ToLower(name)

	// Parse first function argument if one exists.
	var args []Expr
	re, err := p.parseRegex()
	if err != nil {
		return nil, err
	} else if re != nil {
		args = append(args, re)
	} else {
		// If there's a right paren then just return immediately.
		if tok, _, _ := p.scan(); tok == RPAREN {
			return &Call{Cmd: name}, nil
		}
		p.unscan()

		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// Parse additional function arguments if there is a comma.
	for {
		// If there's not a comma, stop parsing arguments.
		if tok, _, _ := p.scanIgnoreWhiteSpace(); tok != COMMA {
			p.unscan()
			break
		}

		re, err := p.parseRegex()
		if err != nil {
			return nil, err
		} else if re != nil {
			args = append(args, re)
			continue
		}

		// Parse an expression argument.
		arg, err := p.ParseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	// There should be a right parentheses at the end.
	if tok, pos, lit := p.scan(); tok != RPAREN {
		return nil, newParseError(tokstr(tok, lit), []string{")"}, pos)
	}

	return &Call{Cmd: name, Args: args}, nil
}

// fieldValidator checks if the Expr is a valid field. We disallow all binary
// expression that return a boolean.
type fieldValidator struct {
	foundInvalid bool
	badToken     Token
}

func (c *fieldValidator) Visit(n Node) Visitor {
	e, ok := n.(*BinaryExpr)
	if !ok {
		return c
	}

	switch e.Op {
	case EQ, NEQ, EQREGEX,
		NEQREGEX, LT, LTE, GT, GTE,
		AND, OR:
		c.foundInvalid = true
		c.badToken = e.Op
		return nil
	}
	return c
}

// peekRune returns the next rune that would be read by the scanner.
func (p *Parser) peekRune() rune {
	r, _, _ := p.s.s.r.ReadRune()
	if r != eof {
		_ = p.s.s.r.UnreadRune()
	}

	return r
}

// scan returns the next token from the underlying scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) {
	return p.s.Scan()
}

// scanIgnoreWhiteSpace scans the next non-whitespace and non-comment token.
func (p *Parser) scanIgnoreWhiteSpace() (tok Token, pos Pos, lit string) {
	for {
		tok, pos, lit = p.scan()
		if tok == WS || tok == COMMENT {
			continue
		}
		return
	}
}

// consumeWhitespace scans the next token if it's whitespace.
func (p *Parser) consumeWhitespace() {
	if tok, _, _ := p.scan(); tok != WS {
		p.unscan()
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() {
	p.s.Unscan()
}

var (
	// Quote String replacer.
	qsReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `'`, `\'`)

	// Quote Ident replacer.
	qiReplacer = strings.NewReplacer("\n", `\n`, `\`, `\\`, `"`, `\"`)
)

// quoteString returns a quoted string.
func quoteString(s string) string {
	return `'` + qsReplacer.Replace(s) + `'`
}

// quoteIdent returns a quoted identifier from multiple bare identifiers.
func quoteIdent(segments ...string) string {
	var buf bytes.Buffer
	for i, segment := range segments {
		needQuote := identNeedsQuotes(segment) ||
			// not last segment && not ""
			((i < len(segments)-1) && segment != "") ||
			// the first or last segment and an empty string
			((i == 0 || i == len(segments)-1) && segment == "")

		if needQuote {
			_ = buf.WriteByte('"')
		}

		_, _ = buf.WriteString(qiReplacer.Replace(segment))

		if needQuote {
			_ = buf.WriteByte('"')
		}

		if i < len(segments)-1 {
			_ = buf.WriteByte('.')
		}
	}
	return buf.String()
}

// identNeedsQuotes returns true if ident requires quotes.
func identNeedsQuotes(ident string) bool {
	// check if this identifier is a keyword
	tok := Lookup(ident)
	if tok != IDENT {
		return true
	}
	for i, r := range ident {
		if i == 0 && !isIdentFirstChar(r) {
			return true
		} else if i > 0 && !isIdentChar(r) {
			return true
		}
	}
	return false
}

// ParseError represents an error that occurred during parsing.
type ParseError struct {
	Message  string
	Found    string
	Expected []string
	Pos      Pos
}

// newParseError returns a new instance of ParseError.
func newParseError(found string, expected []string, pos Pos) *ParseError {
	return &ParseError{Found: found, Expected: expected, Pos: pos}
}

// Error returns the string representation of the error.
func (e *ParseError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s at line %d, char %d",
			e.Message, e.Pos.Line+1, e.Pos.Char+1)
	}
	return fmt.Sprintf("found %s, expected %s at line %d, char %d",
		e.Found, strings.Join(e.Expected, ", "), e.Pos.Line+1, e.Pos.Char+1)
}
