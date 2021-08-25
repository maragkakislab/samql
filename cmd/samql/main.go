package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"

	arg "github.com/alexflint/go-arg"
	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
	"github.com/maragkakislab/samql"
	"github.com/maragkakislab/samql/bamx"
)

// VERSION defines the program version.
const VERSION = "1.6"

// Opts is the struct with the options that the program accepts.
// Opts encapsulates common command line options.
type Opts struct {
	Input []string `arg:"positional,required" help:"file (- for STDIN)"`
	Where string   `arg:"" help:"SQL clause to match records"`
	Count bool     `arg:"-c" help:"print only the count of matching records"`
	Sam   bool     `arg:"-S" help:"interpret input as SAM, otherwise BAM"`
	Parr  int      `arg:"-p" help:"Number of cores for parallelization. Uses all available, if not provided."`
	OBam  bool     `arg:"-b" help:"Output BAM"`
}

// Version returns the program name and version.
func (Opts) Version() string { return "samql " + VERSION }

// Description returns an extended description of the program.
func (Opts) Description() string { return "Filters a SAM/BAM file using the SQL clause provided" }

// Range describes a range query.
type Range struct {
	Rname      string
	Start, End int
}

func main() {
	var opts Opts
	arg.MustParse(&opts)

	// Distribute threads to IO.
	if opts.Parr == 0 {
		opts.Parr = runtime.GOMAXPROCS(0)
	}
	IParr, OParr := distributeParrToIO(opts.Parr, opts.Sam, opts.OBam)

	// Capture potential range queries early to inform readers creation.
	rquery := captureRangeQuery(opts.Where)

	// Create samql readers that read from the inputs.
	readers := getSamqlReaders(opts.Input, opts.Sam, IParr, rquery)
	defer func() { // Close all samql readers at the end.
		for _, r := range readers {
			if err := r.Close(); err != nil {
				log.Fatalf("cannot close samql reader: %v", err)
			}
		}
	}()

	// Create new filter based on provided where clause and add it to the
	// samql readers.
	if opts.Where != "" {
		filter, err := samql.Where(opts.Where)
		if err != nil {
			log.Fatalf("filter creation from where clause failed: %v", err)
		}
		for _, r := range readers {
			r.AppendFilter(filter)
		}
	}

	// If only counting is requested do just that.
	if opts.Count {
		cnt := 0
		for _, r := range readers {
			for {
				_, err := r.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Fatalf("filtering failed: %v", err)
				}
				cnt++
			}
		}
		fmt.Println(cnt)
		os.Exit(0)
	}

	// Create new header by merging all headers.
	headers := make([]*sam.Header, len(readers))
	for i, r := range readers {
		headers[i] = r.Header()
	}
	mergedHeader, _, err := sam.MergeHeaders(headers)
	if err != nil {
		log.Fatalf("cannot merge headers: %v", err)
	}

	// Open a writer that prints to STDOUT.
	stdout := bufio.NewWriter(os.Stdout)
	defer func() {
		if err := stdout.Flush(); err != nil {
			log.Fatalf("flashing of stdout cache failed: %v", err)
		}
	}()

	// Open a new SAM/BAM writer.
	var w writer
	if opts.OBam {
		w, err = bam.NewWriter(stdout, mergedHeader, OParr)
	} else {
		w, err = sam.NewWriter(stdout, mergedHeader, sam.FlagDecimal)
	}
	if err != nil {
		log.Fatalf("cannot open SAM/BAM writer: %v", err)
	}

	// Loop on the filtered records and output.
	for _, r := range readers {
		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("filtering failed: %v", err)
			}

			if err := w.Write(rec); err != nil {
				log.Fatalf("write failed: %v for %s", err, rec.Name)
			}
		}

	}
	// Close w if it is a bam writer
	if temp, ok := w.(*bam.Writer); ok {
		temp.Close()
	}
}

// distributeParrToIO distributes the threads P to the SAM/BAM
// readers/writers. There is no performance benefit for threads higher than 4
// on the input so the excess threads are allocated to BAM output, if
// applicable.
func distributeParrToIO(P int, ISam, OBam bool) (IParr, OParr int) {
	if !OBam { // If output not BAM, no allocation is required.
		return P, 0
	}

	if ISam { // If input is SAM, allocate everything to output BAM.
		IParr = 0
		OParr = P
	} else if P > 7 {
		IParr = 4
		OParr = P - 4
	} else if P < 2 {
		IParr = 1
		OParr = 1
	} else if P <= 7 {
		IParr = int(P / 2)
		OParr = P - IParr
	}

	return IParr, OParr
}

func captureRangeQuery(where string) *Range {
	m := regexp.MustCompile(`RNAME\s*=\s*['"]?(.+?)['"]?\b`).FindStringSubmatch(where)
	if m == nil { // no range query found
		return nil
	}

	rng := &Range{Rname: m[1]}

	m = regexp.MustCompile(`POS\s*(>|>=|=)\s*(\d+)`).FindStringSubmatch(where)
	if m != nil {
		rng.Start, _ = strconv.Atoi(m[2])
	}

	m = regexp.MustCompile(`POS\s*(<|<=)\s*(\d+)`).FindStringSubmatch(where)
	if m == nil {
		rng.End = -1
	} else {
		rng.End, _ = strconv.Atoi(m[2])
	}

	return rng
}

// getFileDescriptor returns a file descriptor that reads from src. It returns
// os.Stdin if src is "-".
func getFileDescriptor(src string) (*os.File, error) {
	if src == "-" {
		return os.Stdin, nil
	}

	return os.Open(src)
}

// getSamqlReaders returns a slice of samql readers that read from the inputs.
func getSamqlReaders(inputs []string, isSam bool, parr int, rquery *Range) []*samql.Reader {

	readers := make([]*samql.Reader, len(inputs))
	for i, in := range inputs {
		// Open input SAM/BAM file descriptor for reading.
		fh, err := getFileDescriptor(in)
		if err != nil {
			log.Fatalf("cannot open file: %v", err)
		}

		// Create a samql Reader that reads from a SAM, BAM or indexed BAM file.
		var r *samql.Reader
		if isSam { // SAM
			sr, err := sam.NewReader(fh)
			if err != nil {
				log.Fatalf("cannot create sam reader: %v", err)
			}
			r = samql.NewReader(sr)
		} else { // BAM or Indexed BAM
			br, err := bam.NewReader(fh, parr)
			if err != nil {
				log.Fatalf("cannot create bam reader: %v", err)
			}
			// Check if BAM is indexed. Look for file with .bai suffix.
			if len(in) > 4 {
				idxf, err := os.Open(in + ".bai")
				if err != nil {
					idxf, err = os.Open(in[:len(in)-4] + ".bai")
				}
				if err == nil { // if index is found
					idxbr, err := bamx.New(br, bufio.NewReader(idxf))
					if err != nil {
						log.Fatalf("opening file failed: %v", err)
					}
					if rquery != nil {
						_ = idxbr.AddQuery(rquery.Rname, rquery.Start, rquery.End)
					}
					r = samql.NewReader(idxbr)
				}
			}
			if r == nil {
				r = samql.NewReader(br)
			}
		}
		readers[i] = r
	}
	return readers
}

// writer defines a common interface for a bam and sam writer.
type writer interface {
	Write(*sam.Record) error
}
