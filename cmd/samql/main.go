package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"

	arg "github.com/alexflint/go-arg"
	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
	"github.com/maragkakislab/samql"
	"github.com/maragkakislab/samql/bamx"
)

// VERSION defines the program version.
const VERSION = "1.3"

// Opts is the struct with the options that the program accepts.
// Opts encapsulates common command line options.
type Opts struct {
	Input string `arg:"positional,required" help:"file (- for STDIN)"`
	Where string `arg:"" help:"SQL clause to match records"`
	Count bool   `arg:"-c" help:"print only the count of matching records"`
	Sam   bool   `arg:"-S" help:"interpret input as SAM, otherwise BAM"`
	Parr  int    `arg:"-p" help:"Number of cores for parallelization"`
}

// Version returns the program name and version.
func (Opts) Version() string { return "samql " + VERSION }

// Description returns an extended description of the program.
func (Opts) Description() string { return "Filters a SAM/BAM file using the SQL clause provided" }

func main() {
	var opts Opts
	arg.MustParse(&opts)

	var err error
	rname, start, end := captureRangeQuery(opts.Where)

	// Open input SAM/BAM file descriptor for reading.
	var fh *os.File
	if opts.Input == "-" {
		fh = os.Stdin
	} else {
		if fh, err = os.Open(opts.Input); err != nil {
			log.Fatalf("cannot open file: %v", err)
		}
	}
	defer func() { // Safely close fh at the end.
		if err = fh.Close(); err != nil {
			log.Fatalf("cannot close input file: %v", err)
		}
	}()

	// Create a samql Reader that reads from a SAM, BAM or indexed BAM file.
	var r *samql.Reader
	if opts.Sam { // SAM
		sr, err := sam.NewReader(fh)
		if err != nil {
			log.Fatalf("cannot create sam reader: %v", err)
		}
		r = samql.NewReader(sr)
	} else { // BAM or Indexed BAM
		br, err := bam.NewReader(fh, opts.Parr)
		if err != nil {
			log.Fatalf("cannot create bam reader: %v", err)
		}
		// Check if BAM is indexed. Look for file with .bai suffix.
		if len(opts.Input) > 4 {
			idxf, err := os.Open(opts.Input + ".bai")
			if err != nil {
				idxf, err = os.Open(opts.Input[:len(opts.Input)-4] + ".bai")
			}
			if err == nil { // if index is found
				idxbr, err := bamx.New(br, bufio.NewReader(idxf))
				if err != nil {
					log.Fatalf("opening file failed: %v", err)
				}
				if rname != "" {
					_ = idxbr.AddQuery(rname, start, end)
				}
				r = samql.NewReader(idxbr)
			}
		}
		if r == nil {
			r = samql.NewReader(br)
		}
		defer func() { // Safely close the samql reader at the end.
			if err = r.Close(); err != nil {
				log.Fatalf("cannot close samql reader: %v", err)
			}
		}()
	}

	// Create new filter based on provided where clause and add it to
	// the samql reader.
	if opts.Where != "" {
		filter, err := samql.Where(opts.Where)
		if err != nil {
			log.Fatalf("filter creation from where clause failed: %v", err)
		}
		r.Filters = append(r.Filters, filter)
	}

	// If only counting is requested do just that.
	if opts.Count {
		cnt := 0
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
		fmt.Println(cnt)
		os.Exit(0)
	}

	// Open a SAM writer that prints to STDOUT.
	stdout := bufio.NewWriter(os.Stdout)
	defer func() {
		if err := stdout.Flush(); err != nil {
			log.Fatalf("flashing of stdout cache failed: %v", err)
		}
	}()
	w, err := sam.NewWriter(stdout, r.Header(), sam.FlagDecimal)
	if err != nil {
		log.Fatalf("cannot open SAM writer: %v", err)
	}

	// Loop on the filtered records and output.
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

func captureRangeQuery(where string) (rname string, start, end int) {
	m := regexp.MustCompile(`RNAME\s*=\s*['"]?(.+?)['"]?\b`).FindStringSubmatch(where)
	if m == nil {
		return "", 0, 0
	}
	rname = m[1]

	m = regexp.MustCompile(`POS\s*(>|>=|=)\s*(\d+)`).FindStringSubmatch(where)
	if m == nil {
		start = 0
	} else {
		start, _ = strconv.Atoi(m[2])
	}

	m = regexp.MustCompile(`POS\s*(<|<=)\s*(\d+)`).FindStringSubmatch(where)
	if m == nil {
		end = -1
	} else {
		end, _ = strconv.Atoi(m[2])
	}

	return rname, start, end
}
