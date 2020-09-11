package samql

import (
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
	"github.com/maragkakislab/samql/bamx"
	"github.com/maragkakislab/samql/ql"
)

// Keyword corresponds to reserved words that have a special meaning in samql
// and samql queries.
type Keyword int

const (
	// QNAME corresponds to the SAM record query name.
	QNAME Keyword = iota
	// FLAG corresponds to the SAM record alignment flag.
	FLAG
	// RNAME corresponds to the SAM record reference name.
	RNAME
	// POS corresponds to the SAM record position.
	POS
	// MAPQ corresponds to the SAM record mapping quality.
	MAPQ
	// CIGAR corresponds to the SAM record CIGAR string.
	CIGAR
	// RNEXT corresponds to the reference name of the mate read.
	RNEXT
	// PNEXT corresponds to the position of the mate read.
	PNEXT
	// TLEN corresponds to SAM record template length.
	TLEN
	// SEQ corresponds to SAM record segment sequence.
	SEQ
	// QUAL corresponds to SAM record quality.
	QUAL
	// LENGTH corresponds to the alignment length.
	LENGTH
)

var keywords = map[string]Keyword{
	"QNAME":  QNAME,
	"FLAG":   FLAG,
	"RNAME":  RNAME,
	"POS":    POS,
	"MAPQ":   MAPQ,
	"CIGAR":  CIGAR,
	"RNEXT":  RNEXT,
	"PNEXT":  PNEXT,
	"TLEN":   TLEN,
	"SEQ":    SEQ,
	"QUAL":   QUAL,
	"LENGTH": LENGTH,
}

// readerSAM is a common interface for SAM/BAM readers and is used as
// input to Reader.
type readerSAM interface {
	Header() *sam.Header
	Read() (*sam.Record, error)
}

// The github.com/biogo/hts SAM/BAM readers satisfy ReaderInt.
var _ readerSAM = (*sam.Reader)(nil)
var _ readerSAM = (*bam.Reader)(nil)
var _ readerSAM = (*bamx.Reader)(nil)

// FilterFunc is a function that returns true for a SAM record that passes the
// filter and false otherwise.
type FilterFunc func(*sam.Record) bool

// Reader is a filtering-enabled SAM reader. Provided filters are applied to
// each record and only records that pass the filters are returned.
type Reader struct {
	r       readerSAM
	Filters []FilterFunc
}

// NewReader returns a new samql Reader that reads from r.
func NewReader(r readerSAM) *Reader {
	return &Reader{
		r:       r,
		Filters: make([]FilterFunc, 0),
	}
}

// Header returns the Header of the underlying reader r.
func (r *Reader) Header() *sam.Header {
	return r.r.Header()
}

// Read returns the next *sam.Record from r that passes all filters. Returns
// nil and io.EOF when r is exhausted.
func (r *Reader) Read() (*sam.Record, error) {
	for {
		rec, err := r.r.Read()
		if err != nil {
			return rec, err
		}

		if !allTrue(rec, r.Filters) {
			continue
		}

		return rec, nil
	}
}

// ReadAll returns all remaining records from r that pass all filters. It
// returns an error if it encounters one except io.EOF that it treats as
// proper termination and returns nil.
func (r *Reader) ReadAll() ([]*sam.Record, error) {
	records := make([]*sam.Record, 0)
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return records, nil
			}
			return records, err
		}

		records = append(records, rec)
	}
}

// allTrue applies all filters to rec and returns true if all return true.
func allTrue(rec *sam.Record, fs []FilterFunc) bool {
	for _, f := range fs {
		if !f(rec) {
			return false
		}
	}
	return true
}

// Logical combines two FilterFunc with the operator op and returns a new
// FilterFunc.
func Logical(f, ff FilterFunc, op ql.Token) FilterFunc {
	switch op {
	case ql.AND:
		return func(rec *sam.Record) bool {
			return f(rec) && ff(rec)
		}
	case ql.OR:
		return func(rec *sam.Record) bool {
			return f(rec) || ff(rec)
		}
	}
	panic(fmt.Sprintf("operator %v not supported for filter combination", op))
}

// Qname returns a FilterFunc that compares the given value to the sam
// record query name.
func Qname(val string, op ql.Token) FilterFunc {
	f := getPlaceholderStr[QNAME]
	return func(rec *sam.Record) bool {
		return CompStr(f(rec), val, op)
	}
}

// Rname returns a FilterFunc that compares the given value to the sam
// record reference name.
func Rname(val string, op ql.Token) FilterFunc {
	f := getPlaceholderStr[RNAME]
	return func(rec *sam.Record) bool {
		return CompStr(f(rec), val, op)
	}
}

// Pos returns a FilterFunc that compares the given value to the sam
// record alignment position.
func Pos(val int, op ql.Token) FilterFunc {
	f := getPlaceholderInt[POS]
	return func(rec *sam.Record) bool {
		return CompInt(f(rec), val, op)
	}
}

// Length returns a FilterFunc that compares the given value to the sam
// record alignment length.
func Length(val int, op ql.Token) FilterFunc {
	f := getPlaceholderInt[LENGTH]
	return func(rec *sam.Record) bool {
		return CompInt(f(rec), val, op)
	}
}

// Where returns a FilterFunc that is constructed from an SQL WHERE statement.
// The function assumes the WHERE keyword is not part of query.
func Where(query string) (FilterFunc, error) {
	// A select statement is appended to the query for compatibility with ql
	// parser. The appended statement is discarded after parsing.
	query = "SELECT * FROM foo WHERE " + query

	// Create a ql.Parser from query.
	p := ql.NewParserFromStr(query)

	// Build the Abstract Syntax Tree.
	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, err
	}

	// Visit all nodes in the AST to build FilterFunc.
	var v evalVisitor
	ql.Walk(&v, stmt)
	if v.Err() != nil {
		return nil, v.Err()
	}

	// After the tree walk, v.filters should only contain one filter.
	if len(v.nodes) > 1 {
		panic("samql: filter creation failed for " + query)
	}

	fil, ok := v.nodes[0].(FilterFunc)
	if !ok {
		panic("samql: filterFunc creation failed for " + query)
	}

	return fil, nil
}

type evalVisitor struct {
	nodes []interface{}
	err   error
}

func (v *evalVisitor) Err() error {
	return v.err
}

func (v *evalVisitor) pop2Nodes() (lhs, rhs interface{}) {
	if len(v.nodes) < 2 {
		panic("exprToFilterFuncVisitor: nodes stack empty")
	}

	rhs = v.nodes[len(v.nodes)-1]
	lhs = v.nodes[len(v.nodes)-2]
	v.nodes = v.nodes[:len(v.nodes)-2]
	return
}

func (v *evalVisitor) Visit(node ql.Node) ql.Visitor {
	// log.Printf("%#v\n", node)
	switch n := node.(type) {
	case *ql.BinaryExpr:

		// Resolve the LHS.
		ql.Walk(v, n.LHS)
		if v.err != nil {
			return nil
		}

		// Resolve the RHS.
		ql.Walk(v, n.RHS)
		if v.err != nil {
			return nil
		}

		// When this point is reached 3 nodes need to resolved (i.e. operand,
		// lhs, rhs). The lhs and rhs have already been resolved to their
		// final values. The operand is either a comparison (handled in case 1
		// or logical (handled in case 2).
		switch n.Op {
		case ql.EQ, ql.NEQ, ql.LT, ql.LTE, ql.GT, ql.GTE, ql.EQREGEX,
			ql.NEQREGEX:
			lhs, rhs := v.pop2Nodes()
			v.nodes = append(v.nodes, eval(lhs, rhs, n.Op))

		case ql.AND, ql.OR:
			lhs, rhs := v.pop2Nodes()
			l, ok := lhs.(FilterFunc)
			if !ok {
				panic("AND and OR can only combine two FilterFuncs")
			}
			r, ok := rhs.(FilterFunc)
			if !ok {
				panic("AND and OR can only combine two FilterFuncs")
			}
			if n.Op == ql.AND {
				v.nodes = append(v.nodes, eval(l, r, n.Op))
			} else {
				v.nodes = append(v.nodes, eval(l, r, n.Op))
			}

		case ql.BITWISEAND:
			lhs, rhs := v.pop2Nodes()
			v.nodes = append(v.nodes, eval(lhs, rhs, n.Op))

		default:
			v.err = fmt.Errorf("unsupported operator, %s", n.Op)
		}

		return nil

	case *ql.VarRef:
		v.nodes = append(v.nodes, n.Val)
		return nil

	case *ql.ParenExpr:
		ql.Walk(v, n.Expr)
		if v.err != nil {
			return nil
		}
		return nil

	case *ql.StringLiteral:
		v.nodes = append(v.nodes, n.Val)
		return nil

	case *ql.NumberLiteral:
		v.nodes = append(v.nodes, n.Val)
		return nil

	case *ql.IntegerLiteral:
		v.nodes = append(v.nodes, n.Val)
		return nil

	case *ql.UnsignedLiteral:
		v.nodes = append(v.nodes, n.Val)
		return nil

	case *ql.RegexLiteral:
		v.nodes = append(v.nodes, n.Val)
		return nil

	default:
		//v.err = fmt.Errorf("unsupported expression %T", n)
		return v
	}
}

// placeholderInt is a function that returns an integer given a sam.Record.
type placeholderInt func(*sam.Record) int

// placeholderFloat is a function that returns a float32 given a sam.Record.
type placeholderFloat func(*sam.Record) float32

// placeholderStr is a function that returns a string given a sam.Record.
type placeholderStr func(*sam.Record) string

// getPlaceholderStr associates a SamField with a placeholderStr.
var getPlaceholderStr = map[Keyword]placeholderStr{
	QNAME: func(rec *sam.Record) string { return rec.Name },
	RNAME: func(rec *sam.Record) string { return rec.Ref.Name() },
	CIGAR: func(rec *sam.Record) string { return rec.Cigar.String() },
	RNEXT: func(rec *sam.Record) string { return rec.MateRef.Name() },
	SEQ:   func(rec *sam.Record) string { return string(rec.Seq.Expand()) },
	QUAL:  func(rec *sam.Record) string { return string(rec.Qual) },
}

// getPlaceholderInt associates a SamField with a placeholderInt.
var getPlaceholderInt = map[Keyword]placeholderInt{
	FLAG:   func(rec *sam.Record) int { return int(rec.Flags) },
	POS:    func(rec *sam.Record) int { return rec.Pos },
	MAPQ:   func(rec *sam.Record) int { return int(rec.MapQ) },
	PNEXT:  func(rec *sam.Record) int { return rec.MatePos },
	TLEN:   func(rec *sam.Record) int { return rec.TempLen },
	LENGTH: func(rec *sam.Record) int { return rec.Len() },
}

func getPlaceholderTag(aval string) interface{} {
	switch typ := aval[3]; typ {
	case 'i':
		return placeholderInt(func(rec *sam.Record) int {
			if aux, ok := rec.Tag([]byte(aval[0:2])); ok {
				switch v := aux.Value().(type) {
				case uint8:
					return int(v)
				case uint16:
					return int(v)
				case uint32:
					return int(v)
				case int8:
					return int(v)
				case int16:
					return int(v)
				case int32:
					return int(v)
				}
			}
			return 0
		})
	case 'Z':
		return placeholderStr(func(rec *sam.Record) string {
			if aux, ok := rec.Tag([]byte(aval[0:2])); ok {
				switch v := aux.Value().(type) {
				case string:
					return v
				}
			}
			return ""
		})
	case 'A':
		return placeholderStr(func(rec *sam.Record) string {
			if aux, ok := rec.Tag([]byte(aval[0:2])); ok {
				switch v := aux.Value().(type) {
				case byte:
					return string(v)
				}
			}
			return ""
		})
	case 'f':
		return placeholderFloat(func(rec *sam.Record) float32 {
			if aux, ok := rec.Tag([]byte(aval[0:2])); ok {
				switch v := aux.Value().(type) {
				case float32:
					return float32(v)
				}
			}
			return 0.0
		})
	default:
		panic("type " + string(typ) + " in " + aval + " is not supported")
	}
}

// eval evaluates the inferred values of a and b using the operator op. eval
// returns a concrete value, a placeholder or a FilterFunc.
func eval(a, b interface{}, op ql.Token) interface{} {
	var validTag = regexp.MustCompile(`^[A-Za-z][A-Za-z]:[AifZHB]`)
	switch aval := a.(type) {
	case string:
		if sf, ok := keywords[aval]; ok {
			if fn, ok := getPlaceholderInt[sf]; ok {
				a = fn
			} else if fn, ok := getPlaceholderStr[sf]; ok {
				a = fn
			}
		} else if validTag.MatchString(aval) {
			a = getPlaceholderTag(aval)
		}
	}

	switch bval := b.(type) {
	case string:
		if sf, ok := keywords[bval]; ok {
			if fn, ok := getPlaceholderInt[sf]; ok {
				b = fn
			} else if fn, ok := getPlaceholderStr[sf]; ok {
				b = fn
			}
		} else if validTag.MatchString(bval) {
			b = getPlaceholderTag(bval)
		}
	}

	switch a := a.(type) {
	case FilterFunc:
		switch b := b.(type) {
		case FilterFunc:
			return Logical(a, b, op)
		default:
			panic("filters can only be evaluated to themselves")
		}

	case string:
		switch b := b.(type) {
		case string:
			return a == b
		}

	case placeholderInt:
		switch b := b.(type) {
		case int64:
			switch op {
			case ql.BITWISEAND:
				return placeholderInt(func(rec *sam.Record) int {
					return a(rec) & int(b)
				})
			case ql.BITWISEOR:
				return placeholderInt(func(rec *sam.Record) int {
					return a(rec) | int(b)
				})
			default:
				return FilterFunc(func(rec *sam.Record) bool {
					return CompInt(a(rec), int(b), op)
				})
			}
		case placeholderInt:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompInt(a(rec), b(rec), op)
			})
		case placeholderFloat:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompInt(a(rec), int(b(rec)), op)
			})
		case float64:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompInt(a(rec), int(b), op)
			})
		default:
			panic("integer placeholder can only be evaluated to int or another integer placeholder")
		}

	case placeholderFloat:
		switch b := b.(type) {
		case float64:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompFloat(a(rec), float32(b), op)
			})
		case int64:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompFloat(a(rec), float32(b), op)
			})
		case placeholderInt:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompFloat(a(rec), float32(b(rec)), op)
			})
		case placeholderFloat:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompFloat(a(rec), b(rec), op)
			})
		default:
			panic("float placeholder can only be evaluated to float or another float placeholder")
		}

	case placeholderStr:
		switch b := b.(type) {
		case string:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompStr(a(rec), b, op)
			})
		case placeholderStr:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompStr(a(rec), b(rec), op)
			})
		case *regexp.Regexp:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompStr(a(rec), b.String(), op)
			})
		case int64:
			return FilterFunc(func(rec *sam.Record) bool {
				return CompStr(a(rec), strconv.FormatInt(b, 10), op)
			})
		default:
			panic("string placeholder can only be evaluated to string or another string placeholder")
		}
	}

	panic("unknown value type")
}

// CompInt compares two integers using the provided operator op.
func CompInt(a, b int, op ql.Token) bool {
	switch op {
	case ql.EQ:
		return a == b
	case ql.NEQ:
		return a != b
	case ql.LT:
		return a < b
	case ql.LTE:
		return a <= b
	case ql.GT:
		return a > b
	case ql.GTE:
		return a >= b
	default:
		return false
	}
}

// CompFloat compares two float32 using the provided operator op.
func CompFloat(a, b float32, op ql.Token) bool {
	switch op {
	case ql.EQ:
		return a == b
	case ql.NEQ:
		return a != b
	case ql.LT:
		return a < b
	case ql.LTE:
		return a <= b
	case ql.GT:
		return a > b
	case ql.GTE:
		return a >= b
	default:
		return false
	}
}

// CompStr compares two strings using the provided operator op.
func CompStr(a, b string, op ql.Token) bool {
	switch op {
	case ql.EQ:
		return a == b
	case ql.NEQ:
		return a != b
	case ql.LT:
		return a < b
	case ql.LTE:
		return a <= b
	case ql.GT:
		return a > b
	case ql.GTE:
		return a >= b
	case ql.EQREGEX:
		re, err := regexp.Compile(b)
		if err != nil {
			panic(err) //TODO error handling
		}
		return re.MatchString(a)
	case ql.NEQREGEX:
		re, err := regexp.Compile(b)
		if err != nil {
			panic(err) //TODO error handling
		}
		return !re.MatchString(a)
	default:
		return false
	}
}
