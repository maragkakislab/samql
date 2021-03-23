package samql

import (
	"io"
	"strings"
	"testing"

	"github.com/biogo/hts/sam"
	"github.com/maragkakislab/samql/ql"
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
r001	147	chr1	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1	MD:Z:TAT	de:f:0.0903
r004	3840	chr2	40	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r005	0	1	40	29	6M14N5M	*	0	0	ATAGCTTCAGC	*	NM:i:60000	MD:A:T
r006	77	*	0	0	*	*	0	0	CAGCGTGCATGCTACGATAGCAT	*
r006	141	*	0	0	*	*	0	0	CGATCGATCGAGCTAGCTAGCT	*
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
		RecCnt: 8,
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
		RecCnt: 7,
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
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("RNAME!~/^chr1/")),
		},
	},
	{
		Test:   "Test9",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("POS < 15")),
		},
	},
	{
		Test:   "Test10",
		Data:   samData,
		RecCnt: 5,
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
		RecCnt: 4,
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
	{
		Test:   "Test18b",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("RNAME = RNEXT")),
		},
	},
	{
		Test:   "Test19-Tag1",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("NM:i = 1")),
		},
	},
	{
		Test:   "Test19-Tag2",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("NM:i >= 60000")),
		},
	},
	{
		Test:   "Test19-Tag3",
		Data:   samData,
		RecCnt: 8,
		Filters: []FilterFunc{
			Must(Where("NM:i = NM:i")),
		},
	},
	{
		Test:   "Test19-Tag4",
		Data:   samData,
		RecCnt: 6,
		Filters: []FilterFunc{
			Must(Where("NM:i = de:f")),
		},
	},
	{
		Test:   "Test19-Tag6",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("de:f = 0.0903")),
		},
	},
	{
		Test:   "Test19-Tag7",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("de:f > 0.0900000000")),
		},
	},
	{
		Test:   "Test19-Tag8",
		Data:   samData,
		RecCnt: 8,
		Filters: []FilterFunc{
			Must(Where("de:f <= de:f")),
		},
	},
	{
		Test:   "Test19-Tag9",
		Data:   samData,
		RecCnt: 8,
		Filters: []FilterFunc{
			Must(Where("de:f != -60000")),
		},
	},
	{
		Test:   "Test19-Tag10",
		Data:   samData,
		RecCnt: 6,
		Filters: []FilterFunc{
			Must(Where("de:f >= NM:i")),
		},
	},
	{
		Test:   "Test19-Tag11",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("MD:A = T")),
		},
	},
	{
		Test:   "Test19-Tag12",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("MD:Z = TAT")),
		},
	},
	{
		Test:   "Test20",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("MAPQ < 30")),
		},
	},
	{
		Test:   "Test21",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("PNEXT >= 36")),
		},
	},
	{
		Test:   "Test22",
		Data:   samData,
		RecCnt: 7,
		Filters: []FilterFunc{
			Must(Where("TLEN != 39")),
		},
	},
	{
		Test:   "Test23",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("LENGTH <= 9")),
		},
	},
	{
		Test:   "Test24",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("CIGAR =~ /^[68]M/")),
		},
	},
	{
		Test:   "Test25",
		Data:   samData,
		RecCnt: 3,
		Filters: []FilterFunc{
			Must(Where("SEQ =~ /^AT/")),
		},
	},
	{
		Test:   "Test26",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("PAIRED = TRUE")),
		},
	},
	{
		Test:   "Test27a",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("PAIRED")),
		},
	},
	{
		Test:   "Test27b",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("PAIRED AND PROPERPAIR AND REVERSE AND READ2")),
		},
	},
	{
		Test:   "Test27c",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("PAIRED AND MATEREVERSE AND READ1")),
		},
	},
	{
		Test:   "Test27d",
		Data:   samData,
		RecCnt: 1,
		Filters: []FilterFunc{
			Must(Where("SECONDARY AND QCFAIL AND DUPLICATE AND SUPPLEMENTARY")),
		},
	},
	{
		Test:   "Test27e",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("PAIRED AND UNMAPPED AND MATEUNMAPPED")),
		},
	},
	{
		Test:   "Test28",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("PAIRED AND PAIRED")),
		},
	},
	{
		Test:   "Test29",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("QNAME = r001 AND PAIRED")),
		},
	},
	{
		Test:   "Test30",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("PAIRED AND QNAME = r001")),
		},
	},
	{
		Test:   "Test31",
		Data:   samData,
		RecCnt: 2,
		Filters: []FilterFunc{
			Must(Where("TRUE AND QNAME = r001 AND TRUE")),
		},
	},
	{
		Test:   "Test32",
		Data:   samData,
		RecCnt: 8,
		Filters: []FilterFunc{
			Must(Where("TRUE")),
		},
	},
	{
		Test:   "Test33",
		Data:   samData,
		RecCnt: 4,
		Filters: []FilterFunc{
			Must(Where("PAIRED = FALSE")),
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
