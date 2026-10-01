// Package benchgate enforces a performance-regression budget on the CSV output
// of `benchstat -format csv`.
//
// It is the threshold half of the CI benchmark gate. benchstat decides whether
// a change is statistically significant and prints a "~" when it is noise;
// benchgate decides whether a significant slowdown is large enough to fail the
// build. A row is a regression when its "vs base" column is a positive change
// greater than the threshold. Rows benchstat marked as noise ("~"),
// improvements (a negative change), and benchmarks present in only one input
// (no "vs base" cell) are ignored, so run-to-run jitter alone cannot block a
// merge.
package benchgate

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Finding is one pinned benchmark whose measured value regressed by more than
// the allowed threshold.
type Finding struct {
	Metric string
	Name   string
	Change float64
}

func (f Finding) String() string {
	return fmt.Sprintf("%s %s: +%.2f%%", f.Metric, f.Name, f.Change)
}

// Evaluate parses the benchstat CSV in r and returns every regression that
// exceeds threshold among the selected metrics. metrics is the set of metric
// names to enforce (for example "sec/op", "B/op", "allocs/op"); an empty set
// enforces every metric table present.
func Evaluate(r io.Reader, metrics map[string]bool, threshold float64) ([]Finding, error) {
	reader := csv.NewReader(r)
	// Metric tables and single-input rows have differing column counts.
	reader.FieldsPerRecord = -1

	var (
		metric   string
		findings []Finding
	)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse benchstat CSV: %w", err)
		}
		// Each metric table is introduced by its header, whose last two columns
		// are "vs base" and "P".
		if len(record) >= 6 && record[5] == "vs base" {
			metric = record[1]
			continue
		}
		// Data rows carry the benchmark name in column 0. A benchmark present in
		// only one input has no "vs base" cell and therefore no change to judge.
		if len(record) < 6 || record[0] == "" || record[0] == "geomean" {
			continue
		}
		if len(metrics) > 0 && !metrics[metric] {
			continue
		}
		change, ok := positiveChange(record[5])
		if !ok || change <= threshold {
			continue
		}
		findings = append(findings, Finding{Metric: metric, Name: record[0], Change: change})
	}
	return findings, nil
}

// positiveChange returns the magnitude of a benchstat "vs base" cell when it is
// a positive percentage change such as "+25.00%". It returns false for noise
// ("~"), improvements ("-10.00%"), and any cell it does not parse as a positive
// percentage.
func positiveChange(cell string) (float64, bool) {
	if !strings.HasPrefix(cell, "+") || !strings.HasSuffix(cell, "%") {
		return 0, false
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(cell[1:], "%"), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// Run is the command's testable core: it reads benchstat CSV from the named
// files, or from stdin when none are given, and returns the process exit code
// (0 no regression, 1 regression, 2 usage or I/O error).
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("benchgate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	threshold := fs.Float64("threshold", 20, "maximum allowed regression, in percent")
	metricsFlag := fs.String("metrics", "", "comma-separated metric names to enforce (default: all)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	metrics := make(map[string]bool)
	for _, m := range strings.Split(*metricsFlag, ",") {
		if m = strings.TrimSpace(m); m != "" {
			metrics[m] = true
		}
	}

	paths := fs.Args()
	readers := make([]io.Reader, 0, len(paths))
	if len(paths) == 0 {
		readers = append(readers, stdin)
	} else {
		for _, path := range paths {
			// The path is an operator-supplied command-line argument.
			f, err := os.Open(path) //nolint:gosec // path is a CLI argument, not attacker-controlled
			if err != nil {
				_, _ = fmt.Fprintf(stderr, "benchgate: %v\n", err)
				return 2
			}
			defer func() { _ = f.Close() }()
			readers = append(readers, f)
		}
	}

	var findings []Finding
	for _, r := range readers {
		got, err := Evaluate(bufio.NewReader(r), metrics, *threshold)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "benchgate: %v\n", err)
			return 2
		}
		findings = append(findings, got...)
	}

	if len(findings) == 0 {
		_, _ = fmt.Fprintf(stdout, "benchgate: no regression over %.2f%%\n", *threshold)
		return 0
	}
	for _, f := range findings {
		_, _ = fmt.Fprintln(stdout, f)
	}
	_, _ = fmt.Fprintf(stderr, "benchgate: %d benchmark(s) regressed by more than %.2f%%\n", len(findings), *threshold)
	return 1
}
