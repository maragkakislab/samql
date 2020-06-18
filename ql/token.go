package ql

import (
	"strings"
)

// Token is a lexical token of the samql language.
type Token int

// This is a comprehensive list of the samql language tokens.
const (
	// Special tokens.
	ILLEGAL Token = iota
	EOF
	WS
	COMMENT

	// Literal tokens.
	literalBeg
	IDENT      // main
	BOUNDPARAM // $param
	NUMBER     // 12345.67
	INTEGER    // 12345
	STRING     // "abc"
	BADSTRING  // "abc
	BADESCAPE  // \q
	TRUE       // true
	FALSE      // false
	REGEX      // Regular expressions
	BADREGEX   // `.*
	literalEnd

	// Operators
	operatorBeg
	ADD        // +
	SUB        // -
	MUL        // *
	DIV        // /
	MOD        // %
	BITWISEAND // &
	BITWISEOR  // |
	BITWISEXOR // ^
	AND        // AND
	OR         // OR
	EQ         // =
	NEQ        // !=
	EQREGEX    // =~
	NEQREGEX   // !~
	LT         // <
	LTE        // <=
	GT         // >
	GTE        // >=
	operatorEnd

	// Structure
	LPAREN // (
	RPAREN // )
	COMMA  // ,
	//	COLON     // :
	SEMICOLON // ;
	DOT       // .

	// Keywords
	keywordBeg
	AS
	FROM
	SELECT
	WHERE
	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:     "IDENT",
	NUMBER:    "NUMBER",
	STRING:    "STRING",
	BADSTRING: "BADSTRING",
	BADESCAPE: "BADESCAPE",
	TRUE:      "TRUE",
	FALSE:     "FALSE",
	REGEX:     "REGEX",

	ADD:        "+",
	SUB:        "-",
	MUL:        "*",
	DIV:        "/",
	MOD:        "%",
	BITWISEAND: "&",
	BITWISEOR:  "|",
	BITWISEXOR: "^",
	AND:        "AND",
	OR:         "OR",
	EQ:         "=",
	NEQ:        "!=",
	EQREGEX:    "=~",
	NEQREGEX:   "!~",
	LT:         "<",
	LTE:        "<=",
	GT:         ">",
	GTE:        ">=",

	LPAREN: "(",
	RPAREN: ")",
	COMMA:  ",",
	// COLON:     ":",
	SEMICOLON: ";",
	DOT:       ".",

	AS:     "AS",
	FROM:   "FROM",
	SELECT: "SELECT",
	WHERE:  "WHERE",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Precedence returns the precedence for the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB, BITWISEOR, BITWISEXOR:
		return 4
	case MUL, DIV, MOD, BITWISEAND:
		return 5
	}
	return 0
}

// isOperator returns true for operator tokens.
func (tok Token) isOperator() bool {
	return tok > operatorBeg && tok < operatorEnd
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
}

// Pos specifies the line and character position of a token. The Char and Line
// are zero-based.
type Pos struct {
	Line int
	Char int
}
