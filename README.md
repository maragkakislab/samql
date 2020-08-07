# samql [![Go Report Card](https://goreportcard.com/badge/github.com/maragkakislab/samql)](https://goreportcard.com/report/github.com/maragkakislab/samql) [![Build Status](https://travis-ci.org/maragkakislab/samql.svg?branch=master)](https://travis-ci.org/maragkakislab/samql) [![GoDoc](https://godoc.org/github.com/maragkakislab/samql?status.svg)](https://godoc.org/github.com/maragkakislab/samql)
SQL-like query language for the SAM/BAM file format

## Install

Download the latest executable for your system [here](https://github.com/maragkakislab/samql/releases/latest/).

Otherwise, to install the Go library and executable:

`go get github.com/maragkakislab/samql/...`


## Objective
To provide a command line utility and a clean API for filtering SAM/BAM files
using simple SQL-like commands. The samql command showcases the envisioned
functionality.

## Usage
```bash
# Simple
samql --where "RNAME = chr1" test.bam    # Reference name is "chr1"
samql --where "QNAME = read1" test.bam   # Query name is "read1"
samql --where "POS > 100" test.bam       # Position (0-based) greater than 1
samql --where "FLAG & 16 = 16" test.bam  # Flag arithmetics (Negative strand)

# Regex
samql --where "CIGAR =~ /^15M/" test.bam # Alignment starts with 15 matches

# More complex
samql --where "RNAME = chr1 OR QNAME = read1 AND POS > 100" test.bam

# Counting
samql -c --where "RNAME = chr1" test.bam

# Very complex
# Uniquely mapped reads, with both mate pairs on chr1 or
# chrX that start with 15 matches/mismatches, are shorter
# than 75 nts or begin with an ATG and are located on the
# reverse strand.
samql --where "(RNAME = 'chr1' OR RNAME = 'chrX') AND \
               (RNEXT = 'chr1' OR RNEXT = 'chrX') AND \
	       NH:i = 1 AND \
	       CIGAR =~ /15M/ AND \
	       (LENGTH < 75 OR SEQ =~ /^ATG/) AND \
	       FLAG & 16 = 16"
```

## API example
```Go
// Open github.com/biogo/hts/sam reader
f := os.Open("test.sam")
sr, _ := sam.NewReader(f)

// Do the filtering
r := samql.NewReader(sr)
filter, _ := samql.Where("POS = 1")
r.Filters = append(r.Filters, filter)
for {
	rec, err := r.Read()
	if err != nil {
		if err == io.EOF {
			break
		}
		panic(err)
	}

	// Do sth with rec
}
```
