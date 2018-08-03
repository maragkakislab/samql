package ql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// ParseStatement parses a statement string and returns its AST
// representation.
func ParseStatement(s string) (Statement, error) {
	return NewParserFromStr(s).ParseStatement()
}

// ParseExpr parses an expression string and returns its AST representation.
func ParseExpr(s string) (Expr, error) {
	return NewParserFromStr(s).ParseExpr()
}

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	var tests = []struct {
		s      string
		params map[string]interface{}
		stmt   Statement
		err    string
	}{
		// SELECT * statement
		{
			s: `SELECT * FROM myseries;`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &Wildcard{}},
				},
				Source: Source(&Table{Name: "myseries"}),
			},
		},
		{
			s: `SELECT field1, * FROM myseries`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &VarRef{Val: "field1"}},
					{Expr: &Wildcard{}},
				},
				Source: Source(&Table{Name: "myseries"}),
			},
		},
		{
			s: `SELECT *, field1 FROM myseries`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &Wildcard{}},
					{Expr: &VarRef{Val: "field1"}},
				},
				Source: Source(&Table{Name: "myseries"}),
			},
		},

		// SELECT statement
		{
			s: fmt.Sprintf(`SELECT mean(field1), count(field2) AS C FROM foo WHERE host = 'bar';`),
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &Call{
						Cmd: "mean", Args: []Expr{&VarRef{Val: "field1"}}}},
					{Expr: &Call{
						Cmd: "count", Args: []Expr{&VarRef{Val: "field2"}}},
						Alias: "C"},
				},
				Source: Source(&Table{Name: "foo"}),
				Condition: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "host"},
					RHS: &StringLiteral{Val: "bar"},
				},
			},
		},
		{
			s: `SELECT "foo.bar.baz" AS foo FROM myseries`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &VarRef{Val: "foo.bar.baz"}, Alias: "foo"},
				},
				Source: Source(&Table{Name: "myseries"}),
			},
		},
		{
			s: `SELECT "foo.bar.baz" AS foo FROM foo`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &VarRef{Val: "foo.bar.baz"}, Alias: "foo"},
				},
				Source: Source(&Table{Name: "foo"}),
			},
		},

		// SELECT statement (lowercase) with quoted field
		{
			s: `select 'my_field' from myseries`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &StringLiteral{Val: "my_field"}}},
				Source: Source(&Table{Name: "myseries"}),
			},
		},

		// SELECT with regex.
		{
			s: `SELECT * FROM cpu WHERE host = 'C' AND region =~ /.*west.*/`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op: AND,
					LHS: &BinaryExpr{
						Op:  EQ,
						LHS: &VarRef{Val: "host"},
						RHS: &StringLiteral{Val: "C"},
					},
					RHS: &BinaryExpr{
						Op:  EQREGEX,
						LHS: &VarRef{Val: "region"},
						RHS: &RegexLiteral{Val: regexp.MustCompile(".*west.*")},
					},
				},
			},
		},

		// SELECT * FROM WHERE field comparisons
		{
			s: `SELECT * FROM cpu WHERE load > 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  GT,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load >= 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  GTE,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load = 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load <= 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  LTE,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load < 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  LT,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},
		{
			s: `SELECT * FROM cpu WHERE load != 100`,
			stmt: &SelectStatement{
				Fields: []*Field{{Expr: &Wildcard{}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  NEQ,
					LHS: &VarRef{Val: "load"},
					RHS: &IntegerLiteral{Val: 100},
				},
			},
		},

		// SELECT statement with a bound parameter
		{
			s: `SELECT value FROM cpu WHERE value > $value`,
			params: map[string]interface{}{
				"value": int64(2),
			},
			stmt: &SelectStatement{
				Fields: []*Field{{
					Expr: &VarRef{Val: "value"}}},
				Source: Source(&Table{Name: "cpu"}),
				Condition: &BinaryExpr{
					Op:  GT,
					LHS: &VarRef{Val: "value"},
					RHS: &IntegerLiteral{Val: 2},
				},
			},
		},

		// select statements with intertwined comments
		{
			s: `SELECT "user" /*, system, idle */ FROM cpu`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &VarRef{Val: "user"}},
				},
				Source: Source(&Table{Name: "cpu"}),
			},
		},

		{
			s: `SELECT /foo\/*bar/ FROM "foo bar" WHERE x = 1`,
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &RegexLiteral{Val: regexp.MustCompile(`foo/*bar`)}},
				},
				Source: Source(&Table{Name: "foo bar"}),
				Condition: &BinaryExpr{
					Op:  EQ,
					LHS: &VarRef{Val: "x"},
					RHS: &IntegerLiteral{Val: 1},
				},
			},
		},

		// SELECT statement with function call in WHERE
		{
			s: fmt.Sprintf(`SELECT bar FROM foo WHERE distance() = 1`),
			stmt: &SelectStatement{
				Fields: []*Field{
					{Expr: &VarRef{Val: "bar"}},
				},
				Source: Source(&Table{Name: "foo"}),
				Condition: &BinaryExpr{
					Op:  EQ,
					LHS: &Call{Cmd: "distance"},
					RHS: &IntegerLiteral{Val: 1},
				},
			},
		},

		// Errors
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
		{s: `UNKNOWN`, err: `found UNKNOWN, expected SELECT at line 1, char 1`},
		{s: `SELECT field1 X`, err: `found X, expected FROM at line 1, char 15`},
		{s: `SELECT field1 FROM "series" WHERE X +;`, err: `found ;, expected identifier, string, number, bool at line 1, char 38`},
		{s: `SELECT field1 AS`, err: `found EOF, expected identifier at line 1, char 18`},
		{s: `SELECT field1 FROM 12`, err: `found 12, expected identifier at line 1, char 20`},
		{s: `SELECT 10000000000000000000000000000000000000000000 FROM myseries`, err: `unable to parse integer at line 1, char 8`},
		{s: `SELECT 10.5h FROM myseries`, err: `found h, expected FROM at line 1, char 12`},
		{s: `SELECT value > 2 FROM cpu`, err: `invalid operator > in SELECT clause at line 1, char 8; operator is intended for WHERE clause`},
		{s: `SELECT value = 2 FROM cpu`, err: `invalid operator = in SELECT clause at line 1, char 8; operator is intended for WHERE clause`},
		{s: `SELECT s =~ /foo/ FROM cpu`, err: `invalid operator =~ in SELECT clause at line 1, char 8; operator is intended for WHERE clause`},
	}

	for i, tt := range tests {
		p := NewParser(strings.NewReader(tt.s))
		if tt.params != nil {
			p.Params = tt.params
		}
		stmt, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n",
				i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.stmt, stmt) {
				t.Logf("\n# %s\nexp=%s\ngot=%s\n",
					tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt))
				t.Logf("\nSQL exp=%s\nSQL got=%s\n",
					tt.stmt.String(), stmt.String())
				t.Errorf("%d. %q\n\nstmt mismatch:\nexp=%#v\ngot=%#v\n\n",
					i, tt.s, tt.stmt, stmt)
			} else {
				// Attempt to reparse the statement as a string and confirm it
				// parses the same.
				stmt2, err := ParseStatement(stmt.String())
				if err != nil {
					t.Errorf("%d. %q: unable to parse statement string: %s",
						i, stmt.String(), err)
				} else if !reflect.DeepEqual(tt.stmt, stmt2) {
					t.Logf("\n# %s\nexp=%s\ngot=%s\n",
						tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt2))
					t.Logf("\nSQL exp=%s\nSQL got=%s\n",
						tt.stmt.String(), stmt2.String())
					t.Errorf("%d. %q\n\nstmt reparse mismatch:\nexp=%#v\ngot=%#v\n\n",
						i, tt.s, tt.stmt, stmt2)
				}
			}
		}
	}
}

// Ensure the parser can parse expressions into an AST.
func TestParser_ParseExpr(t *testing.T) {
	var tests = []struct {
		s    string
		expr Expr
		err  string
	}{
		// Primitives
		{s: `100.0`, expr: &NumberLiteral{Val: 100}},
		{s: `100`, expr: &IntegerLiteral{Val: 100}},
		{s: `9223372036854775808`, expr: &UnsignedLiteral{Val: 9223372036854775808}},
		{s: `-9223372036854775808`, expr: &IntegerLiteral{Val: -9223372036854775808}},
		{s: `-9223372036854775809`, err: `constant -9223372036854775809 underflows int64`},
		{s: `-100.0`, expr: &NumberLiteral{Val: -100}},
		{s: `-100`, expr: &IntegerLiteral{Val: -100}},
		{s: `100.`, expr: &NumberLiteral{Val: 100}},
		{s: `-100.`, expr: &NumberLiteral{Val: -100}},
		{s: `.23`, expr: &NumberLiteral{Val: 0.23}},
		{s: `-.23`, expr: &NumberLiteral{Val: -0.23}},
		{s: `-+1`, err: `found +, expected identifier, number, duration, ( at line 1, char 2`},
		{s: `'foo bar'`, expr: &StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &BooleanLiteral{Val: true}},
		{s: `false`, expr: &BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &VarRef{Val: "my_ident"}},
		{s: `'2000-01-01 00:00:00'`, expr: &StringLiteral{Val: "2000-01-01 00:00:00"}},
		{s: `'2000-01-01'`, expr: &StringLiteral{Val: "2000-01-01"}},

		// Simple binary expression
		{
			s: `1 + 2`,
			expr: &BinaryExpr{
				Op:  ADD,
				LHS: &IntegerLiteral{Val: 1},
				RHS: &IntegerLiteral{Val: 2},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 * 2 + 3`,
			expr: &BinaryExpr{
				Op: ADD,
				LHS: &BinaryExpr{
					Op:  MUL,
					LHS: &IntegerLiteral{Val: 1},
					RHS: &IntegerLiteral{Val: 2},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 * 3`,
			expr: &BinaryExpr{
				Op:  ADD,
				LHS: &IntegerLiteral{Val: 1},
				RHS: &BinaryExpr{
					Op:  MUL,
					LHS: &IntegerLiteral{Val: 2},
					RHS: &IntegerLiteral{Val: 3},
				},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 / 2 + 3`,
			expr: &BinaryExpr{
				Op: ADD,
				LHS: &BinaryExpr{
					Op:  DIV,
					LHS: &IntegerLiteral{Val: 1},
					RHS: &IntegerLiteral{Val: 2},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 / 3`,
			expr: &BinaryExpr{
				Op:  ADD,
				LHS: &IntegerLiteral{Val: 1},
				RHS: &BinaryExpr{
					Op:  DIV,
					LHS: &IntegerLiteral{Val: 2},
					RHS: &IntegerLiteral{Val: 3},
				},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 % 2 + 3`,
			expr: &BinaryExpr{
				Op: ADD,
				LHS: &BinaryExpr{
					Op:  MOD,
					LHS: &IntegerLiteral{Val: 1},
					RHS: &IntegerLiteral{Val: 2},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 % 3`,
			expr: &BinaryExpr{
				Op:  ADD,
				LHS: &IntegerLiteral{Val: 1},
				RHS: &BinaryExpr{
					Op:  MOD,
					LHS: &IntegerLiteral{Val: 2},
					RHS: &IntegerLiteral{Val: 3},
				},
			},
		},

		// Binary expression with LHS paren group.
		{
			s: `(1 + 2) * 3`,
			expr: &BinaryExpr{
				Op: MUL,
				LHS: &ParenExpr{
					Expr: &BinaryExpr{
						Op:  ADD,
						LHS: &IntegerLiteral{Val: 1},
						RHS: &IntegerLiteral{Val: 2},
					},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with no precedence, tests left associativity.
		{
			s: `1 * 2 * 3`,
			expr: &BinaryExpr{
				Op: MUL,
				LHS: &BinaryExpr{
					Op:  MUL,
					LHS: &IntegerLiteral{Val: 1},
					RHS: &IntegerLiteral{Val: 2},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Addition and subtraction without whitespace.
		{
			s: `1+2-3`,
			expr: &BinaryExpr{
				Op: SUB,
				LHS: &BinaryExpr{
					Op:  ADD,
					LHS: &IntegerLiteral{Val: 1},
					RHS: &IntegerLiteral{Val: 2},
				},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Simple unary expression.
		{
			s: `-value`,
			expr: &BinaryExpr{
				Op:  MUL,
				LHS: &IntegerLiteral{Val: -1},
				RHS: &VarRef{Val: "value"},
			},
		},

		{
			s: `-mean(value)`,
			expr: &BinaryExpr{
				Op:  MUL,
				LHS: &IntegerLiteral{Val: -1},
				RHS: &Call{
					Cmd: "mean",
					Args: []Expr{
						&VarRef{Val: "value"}},
				},
			},
		},

		// Unary expressions with parenthesis.
		{
			s: `-(-4)`,
			expr: &BinaryExpr{
				Op:  MUL,
				LHS: &IntegerLiteral{Val: -1},
				RHS: &ParenExpr{
					Expr: &IntegerLiteral{Val: -4},
				},
			},
		},

		// Multiplication with leading subtraction.
		{
			s: `-2 * 3`,
			expr: &BinaryExpr{
				Op:  MUL,
				LHS: &IntegerLiteral{Val: -2},
				RHS: &IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with regex.
		{
			s: `region =~ /us.*/`,
			expr: &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "region"},
				RHS: &RegexLiteral{Val: regexp.MustCompile(`us.*`)},
			},
		},

		// Binary expression with quoted '/' regex.
		{
			s: `url =~ /http\:\/\/www\.example\.com/`,
			expr: &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "url"},
				RHS: &RegexLiteral{Val: regexp.MustCompile(`http\://www\.example\.com`)},
			},
		},

		// Binary expression with quoted '/' regex, no space around operator
		{
			s: `url=~/http\:\/\/www\.example\.com/`,
			expr: &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "url"},
				RHS: &RegexLiteral{Val: regexp.MustCompile(`http\://www\.example\.com`)},
			},
		},

		// Complex binary expression.
		{
			s: `value + 3 < 30 AND 1 + 2 OR true`,
			expr: &BinaryExpr{
				Op: OR,
				LHS: &BinaryExpr{
					Op: AND,
					LHS: &BinaryExpr{
						Op: LT,
						LHS: &BinaryExpr{
							Op:  ADD,
							LHS: &VarRef{Val: "value"},
							RHS: &IntegerLiteral{Val: 3},
						},
						RHS: &IntegerLiteral{Val: 30},
					},
					RHS: &BinaryExpr{
						Op:  ADD,
						LHS: &IntegerLiteral{Val: 1},
						RHS: &IntegerLiteral{Val: 2},
					},
				},
				RHS: &BooleanLiteral{Val: true},
			},
		},

		// Function call (empty)
		{
			s: `my_func()`,
			expr: &Call{
				Cmd: "my_func",
			},
		},

		// Function call (multi-arg)
		{
			s: `my_func(1, 2 + 3)`,
			expr: &Call{
				Cmd: "my_func",
				Args: []Expr{
					&IntegerLiteral{Val: 1},
					&BinaryExpr{
						Op:  ADD,
						LHS: &IntegerLiteral{Val: 2},
						RHS: &IntegerLiteral{Val: 3},
					},
				},
			},
		},
	}

	for i, tt := range tests {
		expr, err := NewParser(strings.NewReader(tt.s)).ParseExpr()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n",
				i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.expr, expr) {
			t.Errorf("%d. %q\n\nexpr mismatch:\n\nexp=%#v\n\ngot=%#v\n\n",
				i, tt.s, tt.expr, expr)
		} else if err == nil {
			// Attempt to reparse the expr as a string and confirm it parses
			// the same.
			expr2, err := ParseExpr(expr.String())
			if err != nil {
				t.Errorf("%d. %q: unable to parse expr string: %s",
					i, expr.String(), err)
			} else if !reflect.DeepEqual(tt.expr, expr2) {
				t.Logf("\n# %s\nexp=%s\ngot=%s\n",
					tt.s, mustMarshalJSON(tt.expr), mustMarshalJSON(expr2))
				t.Logf("\nSQL exp=%s\nSQL got=%s\n",
					tt.expr.String(), expr2.String())
				t.Errorf("%d. %q\n\nexpr reparse mismatch:\n\nexp=%#v\n\ngot=%#v\n\n",
					i, tt.s, tt.expr, expr2)
			}
		}
	}
}

// Ensure a string can be quoted.
func TestQuote(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		{``, `''`},
		{`foo`, `'foo'`},
		{"foo\nbar", `'foo\nbar'`},
		{`foo bar\\`, `'foo bar\\\\'`},
		{`'foo'`, `'\'foo\''`},
	} {
		if out := quoteString(tt.in); tt.out != out {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.in, tt.out, out)
		}
	}
}

// Ensure an identifier's segments can be quoted.
func TestQuoteIdent(t *testing.T) {
	for i, tt := range []struct {
		ident []string
		s     string
	}{
		{[]string{``}, `""`},
		{[]string{`select`}, `"select"`},
		{[]string{`in-bytes`}, `"in-bytes"`},
		{[]string{`foo bar`}, `"foo bar"`},
	} {
		if s := quoteIdent(tt.ident...); tt.s != s {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.ident, tt.s, s)
		}
	}
}

func BenchmarkParserParseStatement(b *testing.B) {
	b.ReportAllocs()
	s := `SELECT "field" FROM "series" WHERE value > 10`
	for i := 0; i < b.N; i++ {
		if stmt, err := NewParser(
			strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		}
	}
	b.SetBytes(int64(len(s)))
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// mustMarshalJSON encodes a value to JSON.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}
