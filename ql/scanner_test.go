package ql

import (
	"reflect"
	"strings"
	"testing"
)

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok Token
		lit string
		pos Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: EOF},
		{s: `#`, tok: ILLEGAL, lit: `#`},
		{s: ` `, tok: WS, lit: " "},
		{s: "\t", tok: WS, lit: "\t"},
		{s: "\n", tok: WS, lit: "\n"},
		{s: "\r", tok: WS, lit: "\n"},
		{s: "\r\n", tok: WS, lit: "\n"},
		{s: "\rX", tok: WS, lit: "\n"},
		{s: "\n\r", tok: WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: WS, lit: " \n\t \n\t"},
		{s: " foo", tok: WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: ADD},
		{s: `-`, tok: SUB},
		{s: `*`, tok: MUL},
		{s: `/`, tok: DIV},
		{s: `%`, tok: MOD},
		{s: `&`, tok: BITWISEAND},
		{s: `|`, tok: BITWISEOR},
		{s: `^`, tok: BITWISEXOR},

		// Logical operators
		{s: `AND`, tok: AND},
		{s: `and`, tok: AND},
		{s: `OR`, tok: OR},
		{s: `or`, tok: OR},

		// Comparison operators
		{s: `=`, tok: EQ},
		{s: `!=`, tok: NEQ},
		{s: `=~`, tok: EQREGEX},
		{s: `!~`, tok: NEQREGEX},
		{s: `<`, tok: LT},
		{s: `<=`, tok: LTE},
		{s: `>`, tok: GT},
		{s: `>=`, tok: GTE},
		{s: `! `, tok: ILLEGAL, lit: "!"},

		// Misc tokens
		{s: `(`, tok: LPAREN},
		{s: `)`, tok: RPAREN},
		{s: `,`, tok: COMMA},
		{s: `;`, tok: SEMICOLON},
		{s: `.`, tok: DOT},

		// Identifiers
		{s: `foo`, tok: IDENT, lit: `foo`},
		{s: `_foo`, tok: IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: BADESCAPE, lit: `\b`, pos: Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: BADSTRING, lit: "", pos: Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: BADSTRING, lit: `test`},
		{s: `$host`, tok: BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: TRUE},
		{s: `false`, tok: FALSE},

		// Strings
		{s: `'testing 123!'`, tok: STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: STRING, lit: "foo\\bar"},
		{s: `'test`, tok: BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: BADESCAPE, lit: `\g`, pos: Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: INTEGER, lit: `100`},
		{s: `100.23`, tok: NUMBER, lit: `100.23`},
		{s: `.23`, tok: NUMBER, lit: `.23`},
		{s: `10.3s`, tok: NUMBER, lit: `10.3`},

		// Keywords
		{s: `FROM`, tok: FROM},
		{s: `SELECT`, tok: SELECT},
		{s: `WHERE`, tok: WHERE},
		{s: `seLECT`, tok: SELECT}, // case insensitive
	}

	for i, tt := range tests {
		s := NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>",
				i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v",
				i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q",
				i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok Token
		pos Pos
		lit string
	}
	exp := []result{
		{tok: SELECT, pos: Pos{Line: 0, Char: 0}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 6}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: WS, pos: Pos{Line: 0, Char: 12}, lit: " "},
		{tok: FROM, pos: Pos{Line: 0, Char: 13}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 17}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: WS, pos: Pos{Line: 0, Char: 26}, lit: " "},
		{tok: WHERE, pos: Pos{Line: 0, Char: 27}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 32}, lit: " "},
		{tok: IDENT, pos: Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: WS, pos: Pos{Line: 0, Char: 34}, lit: " "},
		{tok: EQ, pos: Pos{Line: 0, Char: 35}, lit: ""},
		{tok: WS, pos: Pos{Line: 0, Char: 36}, lit: " "},
		{tok: STRING, pos: Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: EOF, pos: Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v",
				i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // with newline
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := scanString(strings.NewReader(tt.in))
		if tt.err != errstring(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.foo\.com/`, tok: REGEX, lit: `http\://www\.foo\.com`},
	}

	for i, tt := range tests {
		s := NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.scanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n",
				i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n",
				i, tt.in, tt.lit, lit)
		}
	}
}
