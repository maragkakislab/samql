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
Filters a SAM/BAM file using the provided SQL clause
Usage: samql [--where WHERE] [--count] [--sam] [--parr PARR] [--obam] [--oparr OPARR] INPUT [INPUT ...]

Positional arguments:
  INPUT                  file (- for STDIN)

Options:
  --where WHERE          SQL clause to match records
  --count, -c            print only the count of matching records
  --sam, -S              interpret input as SAM, otherwise BAM
  --parr PARR, -p PARR   Number of cores for parallelization
  --obam, -b             Output BAM
  --oparr OPARR, -t OPARR
                         Number of cores for output compression parallelization
  --help, -h             display this help and exit
  --version              display version and exit
```

## Examples

```bash
# Simple
samql --where "RNAME = chr1" test.bam    # Reference name is "chr1"
samql --where "QNAME = read1" test.bam   # Query name is "read1"
samql --where "POS > 100" test.bam       # Position (0-based) greater than 100
samql --where "REVERSE" test.bam         # Negative strand
samql --where "FLAG & 16 = 16" test.bam  # Ditto using flag arithmetics

# More than one files
samql --where "REVERSE" test1.bam test2.bam # Reads are returned in the order of the files

# Regex
samql --where "CIGAR =~ /^15M/" test.bam # Alignment starts with 15 matches

# More complex
samql --where "RNAME = chr1 OR QNAME = read1 AND POS > 100" test.bam

# Just counting
samql -c --where "RNAME = chr1" test.bam

# Very complex
# Uniquely mapped reads, with first pair on chr1 after
# position 1000000 and second pair on chr1 or chrX that
# start with 15 matches/mismatches, are shorter than 75 nts
# or begin with an ATG and are located on the reverse strand.

samql --where "(RNAME = 'chr1' AND POS > 1000000) AND \
               (RNEXT = 'chr1' OR RNEXT = 'chrX') AND \
               NH:i = 1 AND \
               CIGAR =~ /^15M/ AND \
               (LENGTH < 75 OR SEQ =~ /^ATG/) AND \
               PAIRED AND REVERSE"
```

## Keywords

```Go
QNAME         // QNAME corresponds to the SAM record query name.
FLAG          // FLAG corresponds to the SAM record alignment flag.
RNAME         // RNAME corresponds to the SAM record reference name
POS           // POS corresponds to the SAM record position (0-based).
MAPQ          // MAPQ corresponds to the SAM record mapping quality.
CIGAR         // CIGAR corresponds to the SAM record CIGAR string.
RNEXT         // RNEXT corresponds to the reference name of the mate read.
PNEXT         // PNEXT corresponds to the position of the mate read.
TLEN          // TLEN corresponds to SAM record template length.
SEQ           // SEQ corresponds to SAM record segment sequence.
QUAL          // QUAL corresponds to SAM record quality.
LENGTH        // LENGTH corresponds to the alignment length.
PAIRED        // PAIRED corresponds to SAM flag 0x1.
PROPERPAIR    // PROPERPAIR corresponds to SAM flag 0x2.
UNMAPPED      // UNMAPPED corresponds to SAM flag 0x4.
MATEUNMAPPED  // MATEUNMAPPED corresponds to SAM flag 0x8.
REVERSE       // REVERSE corresponds to SAM flag 0x10.
MATEREVERSE   // MATEREVERSE corresponds to SAM flag 0x20.
READ1         // READ1 corresponds to SAM flag 0x40.
READ2         // READ2 corresponds to SAM flag 0x80.
SECONDARY     // SECONDARY corresponds to SAM flag 0x100.
QCFAIL        // QCFAIL corresponds to SAM flag 0x200.
DUPLICATE     // DUPLICATE corresponds to SAM flag 0x400.
SUPPLEMENTARY // SUPPLEMENTARY corresponds to SAM flag 0x800.
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
