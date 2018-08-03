# samql [![Go Report Card](https://goreportcard.com/badge/github.com/mnsmar/samql)](https://goreportcard.com/report/github.com/mnsmar/samql) [![Build Status](https://travis-ci.org/mnsmar/samql.svg?branch=master)](https://travis-ci.org/mnsmar/samql)
SQL-like query language for the SAM/BAM file format

**The library is under active development and the API is considered unstable**

## Install
`go get github.com/mnsmar/samql`

## Objective
To provide a command line utility and a clean API for filtering SAM/BAM files
using simple SQL-like commands. The samql command already showcases part of
the envisioned functionality.

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
