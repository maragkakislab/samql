package ql

import (
	"reflect"
	"strings"
	"testing"
)

func TestDataType_String(t *testing.T) {
	for i, tt := range []struct {
		typ DataType
		v   string
	}{
		{Float, "float"},
		{Integer, "integer"},
		{String, "string"},
		{Boolean, "boolean"},
		{AnyField, "field"},
		{Unknown, "unknown"},
	} {
		if v := tt.typ.String(); tt.v != v {
			t.Errorf("%d. %v (%s): unexpected string: %s", i, tt.typ, tt.v, v)
		}
	}
}

// Ensure binary expression names can be evaluated.
func TestBinaryExprName(t *testing.T) {
	for i, tt := range []struct {
		expr string
		name string
	}{
		{expr: `value + 1`, name: `value`},
		{expr: `"user" / total`, name: `user_total`},
		{expr: `("user" + total) / total`, name: `user_total_total`},
	} {
		expr := MustParseExpr(tt.expr)
		switch expr := expr.(type) {
		case *BinaryExpr:
			name := expr.Name()
			if name != tt.name {
				t.Errorf("%d. unexpected name %s, got %s", i, name, tt.name)
			}
		default:
			t.Errorf("%d. unexpected expr type: %T", i, expr)
		}
	}
}

// Ensure that the String() value of a statement is parseable.
func TestParseString(t *testing.T) {
	var tests = []struct {
		stmt string
	}{
		{stmt: `SELECT "cpu load" FROM myseries`},
		{stmt: `SELECT "cpu load" FROM "my series"`},
		{stmt: `SELECT "cpu\"load" FROM myseries`},
		{stmt: `SELECT "cpu'load" FROM myseries`},
		{stmt: `SELECT "cpu load" FROM "my\"series"`},
		{stmt: `SELECT * FROM myseries`},
		{stmt: `SELECT "cpu load" FROM "db_with_spaces"`},
	}

	for _, tt := range tests {
		// Parse statement.
		stmt, err := NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}

		stmtCopy, err := NewParser(
			strings.NewReader(stmt.String())).ParseStatement()
		if err != nil {
			t.Fatalf("failed to parse string: %v\norig: %v\ngot: %v",
				err, tt.stmt, stmt.String())
		}

		if !reflect.DeepEqual(stmt, stmtCopy) {
			t.Fatalf("statement changed after stringifying and re-parsing:\noriginal : %v\nre-parsed: %v\n", tt.stmt, stmtCopy.String())
		}
	}
}

func Test_fieldsNames(t *testing.T) {
	for _, test := range []struct {
		in  []string
		out []string
	}{
		{ //case: binary expr(varRef)
			in:  []string{"value+value"},
			out: []string{"value_value"},
		},
		{ //case: binary expr + varRef
			in:  []string{"value+value", "temperature"},
			out: []string{"value_value", "temperature"},
		},
		{ //case: aggregate expr
			in:  []string{"mean(value)"},
			out: []string{"mean"},
		},
		{ //case: binary expr(aggregate expr)
			in:  []string{"mean(value) + max(value)"},
			out: []string{"mean_max"},
		},
		{ //case: binary expr(aggregate expr) + varRef
			in:  []string{"mean(value) + max(value)", "temperature"},
			out: []string{"mean_max", "temperature"},
		},
		{ //case: mixed aggregate and varRef
			in:  []string{"mean(value) + temperature"},
			out: []string{"mean_temperature"},
		},
		{ //case: ParenExpr(varRef)
			in:  []string{"(value)"},
			out: []string{"value"},
		},
		{ //case: ParenExpr(varRef + varRef)
			in:  []string{"(value + value)"},
			out: []string{"value_value"},
		},
		{ //case: ParenExpr(aggregate)
			in:  []string{"(mean(value))"},
			out: []string{"mean"},
		},
		{ //case: ParenExpr(aggregate + aggregate)
			in:  []string{"(mean(value) + max(value))"},
			out: []string{"mean_max"},
		},
	} {
		fields := Fields{}
		for _, s := range test.in {
			expr := MustParseExpr(s)
			fields = append(fields, &Field{Expr: expr})
		}
		alias := fields.Names()
		if !reflect.DeepEqual(alias, test.out) {
			t.Errorf("get fields alias name:\nexp=%v\ngot=%v\n", test.out, alias)
		}
	}

}

func TestSelect_ColumnNames(t *testing.T) {
	for i, tt := range []struct {
		stmt    *SelectStatement
		columns []string
	}{
		{
			stmt: &SelectStatement{
				Fields: Fields([]*Field{
					{Expr: &VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value"},
		},
		{
			stmt: &SelectStatement{
				Fields: Fields([]*Field{
					{Expr: &VarRef{Val: "value"}},
					{Expr: &VarRef{Val: "value"}},
					{Expr: &VarRef{Val: "value_1"}},
				}),
			},
			columns: []string{"value", "value_1", "value_1_1"},
		},
		{
			stmt: &SelectStatement{
				Fields: Fields([]*Field{
					{Expr: &VarRef{Val: "value"}},
					{Expr: &VarRef{Val: "value_1"}},
					{Expr: &VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value", "value_1", "value_2"},
		},
		{
			stmt: &SelectStatement{
				Fields: Fields([]*Field{
					{Expr: &VarRef{Val: "value"}},
					{Expr: &VarRef{Val: "total"}, Alias: "value"},
					{Expr: &VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value_1", "value", "value_2"},
		},
	} {
		columns := tt.stmt.ColumnNames()
		if !reflect.DeepEqual(columns, tt.columns) {
			t.Errorf("%d. expected %s, got %s", i, tt.columns, columns)
		}
	}
}

func MustParseExpr(s string) Expr {
	expr, err := ParseExpr(s)
	if err != nil {
		panic(err.Error())
	}
	return expr
}

func BenchmarkQueryString(b *testing.B) {
	p := NewParser(strings.NewReader(
		`SELECT foo AS zoo, a AS b FROM bar WHERE value > 10 AND q = 'sth'`))
	q, _ := p.ParseStatement()
	for i := 0; i < b.N; i++ {
		_ = q.String()
	}
}
