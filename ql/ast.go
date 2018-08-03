package ql

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// DataType represents the primitive data types available in samql.
type DataType int

const (
	// Float means the data type is a float.
	Float DataType = 0
	// Integer means the data type is an integer.
	Integer DataType = 1
	// Unsigned means the data type is an unsigned integer.
	Unsigned DataType = 2
	// String means the data type is a string of text.
	String DataType = 3
	// Boolean means the data type is a boolean.
	Boolean DataType = 4
	// AnyField means the data type is any field.
	AnyField DataType = 5
	// Unknown primitive data type.
	Unknown DataType = 6
)

var (
	zeroFloat64 interface{} = float64(0)
	zeroInt64   interface{} = int64(0)
	zeroUint64  interface{} = uint64(0)
	zeroString  interface{}
	zeroBoolean interface{} = false
)

// String returns the human-readable string representation of the DataType.
func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case Integer:
		return "integer"
	case Unsigned:
		return "unsigned"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	case AnyField:
		return "field"
	}
	return "unknown"
}

// Named represents anything that has a name.
type Named interface {
	Name() string
}

// Node represents a node in the samql abstract syntax tree.
type Node interface {
	// node is unexported to ensure implementations of Node
	// can only originate in this package.
	node()
	String() string
}

// types that implement Node.
func (*SelectStatement) node() {}
func (*BinaryExpr) node()      {}
func (*BooleanLiteral) node()  {}
func (*Call) node()            {}
func (*IntegerLiteral) node()  {}
func (*UnsignedLiteral) node() {}
func (*Field) node()           {}
func (Fields) node()           {}
func (*Table) node()           {}
func (*NilLiteral) node()      {}
func (*NumberLiteral) node()   {}
func (*ParenExpr) node()       {}
func (*RegexLiteral) node()    {}
func (*StringLiteral) node()   {}
func (*VarRef) node()          {}
func (*Wildcard) node()        {}

// Statement represents a single command in samql.
type Statement interface {
	Node
	// stmt is unexported to ensure implementations of Statement
	// can only originate in this package.
	stmt()
}

// types that implement Statement.
func (*SelectStatement) stmt() {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	// expr is unexported to ensure implementations of Expr
	// can only originate in this package.
	expr()
}

func (*BinaryExpr) expr()      {}
func (*BooleanLiteral) expr()  {}
func (*Call) expr()            {}
func (*IntegerLiteral) expr()  {}
func (*UnsignedLiteral) expr() {}
func (*NilLiteral) expr()      {}
func (*NumberLiteral) expr()   {}
func (*ParenExpr) expr()       {}
func (*RegexLiteral) expr()    {}
func (*StringLiteral) expr()   {}
func (*VarRef) expr()          {}
func (*Wildcard) expr()        {}

// Literal represents a static literal.
type Literal interface {
	Expr
	// literal is unexported to ensure implementations of Literal
	// can only originate in this package.
	literal()
}

func (*BooleanLiteral) literal()  {}
func (*IntegerLiteral) literal()  {}
func (*UnsignedLiteral) literal() {}
func (*NilLiteral) literal()      {}
func (*NumberLiteral) literal()   {}
func (*RegexLiteral) literal()    {}
func (*StringLiteral) literal()   {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	// source is unexported to ensure implementations of Source
	// can only originate in this package.
	source()
}

// Types that implement Source
func (*Table) source() {}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Data sources (tables) that fields are extracted from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr
}

// ColumnNames will walk all fields and functions and return the appropriate
// field names for the select statement while maintaining order of the field
// names.
func (s *SelectStatement) ColumnNames() []string {
	// First walk each field to determine the number of columns.
	columnFields := Fields{}
	for _, field := range s.Fields {
		columnFields = append(columnFields, field)
	}

	columnNames := make([]string, len(columnFields))

	// Keep track of the encountered column names.
	names := make(map[string]int)

	// Resolve aliases first.
	for i, col := range columnFields {
		if col.Alias != "" {
			columnNames[i] = col.Alias
			names[col.Alias] = 1
		}
	}

	// Resolve any generated names and resolve conflicts.
	for i, col := range columnFields {
		if columnNames[i] != "" {
			continue
		}

		name := col.Name()
		count, conflict := names[name]
		if conflict {
			for {
				resolvedName := fmt.Sprintf("%s_%d", name, count)
				_, conflict = names[resolvedName]
				if !conflict {
					names[name] = count + 1
					name = resolvedName
					break
				}
				count++
			}
		}
		names[name]++
		columnNames[i] = name
	}
	return columnNames
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())

	if s.Source != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	return buf.String()
}

// Fields represents a list of fields.
type Fields []*Field

// Names returns a list with the name of all fields.
func (a Fields) Names() []string {
	names := []string{}
	for _, f := range a {
		names = append(names, f.Name())
	}
	return names
}

// String returns a string representation of the fields.
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns the alias, if set. Otherwise
// returns the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	if n, ok := f.Expr.(Named); ok {
		return n.Name()
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	str := f.Expr.String()

	if f.Alias == "" {
		return str
	}
	return fmt.Sprintf("%s AS %s", str, quoteIdent(f.Alias))
}

// Table represents a data source.
type Table struct {
	Name string
}

// String returns a string representation of the table.
func (m *Table) String() string {
	return quoteIdent(m.Name)
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val  string
	Type DataType
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string {
	return quoteIdent(r.Val)
}

// Name returns the name of the variable reference.
func (r *VarRef) Name() string {
	return r.Val
}

// Call represents a function call.
type Call struct {
	Cmd  string
	Args []Expr
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Cmd, strings.Join(str, ", "))
}

// Name returns the name of the call.
func (c *Call) Name() string {
	return c.Cmd
}

// NumberLiteral represents a numeric float64 literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string {
	return strconv.FormatFloat(l.Val, 'f', 3, 64)
}

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Val int64
}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string {
	return fmt.Sprintf("%d", l.Val)
}

// UnsignedLiteral represents an unsigned integer literal. The parser will
// only use an unsigned literal if the parsed integer is greater than
// math.MaxInt64.
type UnsignedLiteral struct {
	Val uint64
}

// String returns a string representation of the literal.
func (l *UnsignedLiteral) String() string {
	return strconv.FormatUint(l.Val, 10)
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string {
	return quoteString(l.Val)
}

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string {
	if r.Val != nil {
		return fmt.Sprintf("/%s/",
			strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}
	return ""
}

// NilLiteral represents a nil literal. This is not available to the query
// language itself. It's only used internally.
type NilLiteral struct{}

// String returns a string representation of the literal.
func (l *NilLiteral) String() string {
	return `nil`
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s",
		e.LHS.String(), e.Op.String(), e.RHS.String())
}

// Name returns the name of a binary expression by concatenating
// the variables in the binary expression with underscores.
func (e *BinaryExpr) Name() string {
	names := make([]string, 0)
	WalkFunc(e, func(n Node) bool {
		switch n := n.(type) {
		case *VarRef:
			names = append(names, n.Val)
		case *Call:
			names = append(names, n.Cmd)
			return false
		}
		return true
	})
	return strings.Join(names, "_")
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr.String())
}

// Name returns the name of the parenthesized expression.
func (e *ParenExpr) Name() string {
	if n, ok := e.Expr.(Named); ok {
		return n.Name()
	}

	return ""
}

// Wildcard represents a wild card expression.
type Wildcard struct {
	Type Token
}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string {
	return "*"
}

// A Visitor is called by Walk to traverse an AST hierarchy. The visitor's
// Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order and calls the
// visitor's Visit function once per node. Traversing terminates when
// v.Visit() returns nil.
func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Source)
		Walk(v, n.Condition)

	}
}

// WalkFunc traverses a node hierarchy in depth-first order and calls fn at
// each node. Traversing terminates if fn returns false.
func WalkFunc(node Node, fn func(Node) bool) {
	Walk(walkFuncVisitor(fn), node)
}

// walkFuncVisitor wraps a Visit function.
type walkFuncVisitor func(Node) bool

// Visit applies fn to n and returns nil if traversing should stop or fn
// otherwise.
func (fn walkFuncVisitor) Visit(n Node) Visitor {
	if ok := fn(n); ok {
		return fn
	}
	return nil
}
