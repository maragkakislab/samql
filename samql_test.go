package samql

import (
	"io"
	"strings"
	"testing"

	"github.com/biogo/hts/sam"
	"github.com/mnsmar/samql/ql"
)

// Must is a helper that wraps a call to a function returning (FilterFunc,
// error) and panics if the error is non-nil.
func Must(f FilterFunc, err error) FilterFunc {
	if err != nil {
		panic(err)
	}
	return f
}

const samData = `@HD	VN:1.5	SO:coordinate
@SQ	SN:chr1	LN:45
@SQ	SN:chr2	LN:100
@SQ	SN:1	LN:45
r001	99	chr1	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	chr1	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	chr1	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r001	147	chr1	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
r004	0	chr2	40	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r005	0	1	40	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
`

var readTests = []struct {
	Test    string
	Data    string
	RecCnt  int
	Filters []FilterFunc
}{
	{
		Test:   "Test1",
		Data:   samData,
		RecCnt: 6,
	},
	{
		Test:   "Test2",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Rname("chr1", ql.EQ),
		},
	},
	{
		Test:   "Test3",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("RNAME=\"chr1\" AND QNAME = \"r001\"")),
		},
	},
	{
		Test:   "Test4",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("(RNAME=\"chr1\" AND QNAME = \"r001\") OR RNAME=\"chr2\"")),
		},
	},
	{
		Test:   "Test5",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("RNAME=\"chr1\" AND POS=15")),
		},
	},
	{
		Test:   "Test6",
		Data:   samData,
		RecCnt: 5,
		Filters: []FilterFunc{
			Must(Where("RNAME!=\"chr2\"")),
		},
	},
	{
		Test:   "Test7",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("RNAME=~/^chr1/")),
		},
	},
	{
		Test:   "Test8",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("RNAME!~/^chr1/")),
		},
	},
	{
		Test:   "Test9",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("POS < 15")),
		},
	},
	{
		Test:   "Test10",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("POS <= 15")),
		},
	},
	{
		Test:   "Test11",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("POS > 15")),
		},
	},
	{
		Test:   "Test12",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("POS >= 15")),
		},
	},
	{
		Test:   "Test13",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("RNAME = 'chr1'")),
		},
	},
	{
		Test:   "Test14",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("POS > 15.0")),
		},
	},
	{
		Test:   "Test15",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("RNAME = \"chr2\" AND POS = 6 OR QNAME = \"r002\"")),
		},
	},
	{
		Test:   "Test16",
		Data:   samData,
		RecCnt: 0,
		Filters: []FilterFunc{
			Must(Where("RNAME = \"chr2\" AND (POS = 6 OR QNAME = \"r002\")")),
		},
	},
	{
		Test:   "Test17",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("FLAG & 1 = 1")),
		},
	},
	{
		Test:   "Test17foo",
		Data:   samData,
		RecCnt: 0,
		Filters: []FilterFunc{
			Must(Where("POS = 1")),
		},
	},
	{
		Test:   "Test18",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("RNAME = 1")),
		},
	},
}

// const samData = `@HD	VN:1.5	SO:coordinate
// @SQ	SN:chr1	LN:45
// @SQ	SN:chr2	LN:100
// r001	99	chr1	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
// r002	0	chr1	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
// r003	0	chr1	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
// r001	147	chr1	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
// r004	0	chr2	40	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
// `

func TestRead(t *testing.T) {
	for _, tt := range readTests {
		// Open a SAM reader.
		sr, err := sam.NewReader(strings.NewReader(tt.Data))
		if err != nil {
			t.Errorf("%s: unexpected error %q", tt.Test, err.Error())
			continue
		}

		// Create a samql Reader.
		r := NewReader(sr)

		// Apply filter functions.
		r.Filters = append(r.Filters, tt.Filters...)

		// Loop on the records.
		records := make([]*sam.Record, 0)
		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				t.Errorf("%s: unexpected error %q", tt.Test, err.Error())
				continue
			}

			records = append(records, rec)
		}

		if l := len(records); l != tt.RecCnt {
			t.Errorf("%s: record count=%d want %d", tt.Test, l, tt.RecCnt)
		}
	}
}
