package bamx

import (
	"io"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
)

type query struct {
	rname      string
	start, end int
}

// Reader holds bam index and the Bam Reader.
// Because Reader holds the underlying os.File open, it is not
// safe to query from multiple go routines.
type Reader struct {
	*bam.Reader
	idx   *bam.Index
	refs  map[string]*sam.Reference
	query query
	iter  *bam.Iterator
}

// New returns a new Reader that encapsulates a bam reader r and an index read
// from idxio.
func New(br *bam.Reader, idxio io.Reader) (*Reader, error) {
	idx, err := bam.ReadIndex(idxio)
	if err != nil {
		return nil, err
	}
	bx := &Reader{Reader: br, idx: idx}

	bx.refs = make(map[string]*sam.Reference)
	for _, r := range br.Header().Refs() {
		bx.refs[r.Name()] = r
	}
	return bx, nil
}

// Read returns the next *sam.Record from r that passes all filters. Returns
// nil and io.EOF when r is exhausted.
func (b *Reader) Read() (*sam.Record, error) {
	if b.iter == nil {
		return b.Reader.Read()
	}
	if !b.iter.Next() {
		return nil, io.EOF
	}
	return b.iter.Record(), b.iter.Error()
}

// AddQuery adds a new range query to the indexed BAM.
func (b *Reader) AddQuery(rname string, start, end int) error {
	b.query = query{rname, start, end}

	ref := b.refs[rname]
	if start < 0 {
		start = 0
	}
	if end <= 0 {
		end = ref.Len() - 1
	}
	chunks, err := b.idx.Chunks(ref, start, end)
	if err != nil {
		return err
	}
	b.iter, err = bam.NewIterator(b.Reader, chunks)
	return err
}
