package benchgate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// gateCSV is a representative `benchstat -format csv` document: three metric
// tables (sec/op, B/op, allocs/op), a header preamble and the single-input rows
// benchstat emits for a benchmark absent from the baseline.
const gateCSV = `goos: linux
goarch: amd64
pkg: github.com/lorenzo-vecchio/gorch
cpu: test

,baseline.txt,,current.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
BenchmarkFast-8,1.0000e-06,1%,9.0000e-07,1%,-10.00%,p=0.001 n=6
BenchmarkNoise-8,1.0000e-06,1%,1.0500e-06,1%,~,p=0.700 n=6
BenchmarkSmall-8,1.0000e-06,1%,1.1000e-06,1%,+10.00%,p=0.001 n=6
BenchmarkBoundary-8,1.0000e-06,1%,1.2000e-06,1%,+20.00%,p=0.001 n=6
BenchmarkBig-8,1.0000e-06,1%,1.2500e-06,1%,+25.00%,p=0.001 n=6
BenchmarkNew-8,,,5.0000e-07,∞
geomean,1.0000e-06,,1.1000e-06,,+10.00%,

,alloc.txt,,cur.txt,,,
,B/op,CI,B/op,CI,vs base,P
BenchmarkBig-8,100,0%,130,0%,+30.00%,p=0.001 n=6
BenchmarkSmall-8,100,0%,110,0%,+10.00%,p=0.001 n=6
BenchmarkNew-8,,,10,∞
geomean,,,,,+0.00%,

,allocs/op,CI,allocs/op,CI,vs base,P
BenchmarkBig-8,3,0%,4,0%,+33.33%,p=0.001 n=6
`

func TestEvaluate_AllMetricsAboveThreshold(t *testing.T) {
	got, err := Evaluate(strings.NewReader(gateCSV), nil, 20)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	want := []Finding{
		{Metric: "sec/op", Name: "BenchmarkBig-8", Change: 25},
		{Metric: "B/op", Name: "BenchmarkBig-8", Change: 30},
		{Metric: "allocs/op", Name: "BenchmarkBig-8", Change: 33.33},
	}
	if len(got) != len(want) {
		t.Fatalf("findings = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("finding[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestEvaluate_MetricFilter(t *testing.T) {
	got, err := Evaluate(strings.NewReader(gateCSV), map[string]bool{"sec/op": true}, 20)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(got) != 1 || got[0] != (Finding{Metric: "sec/op", Name: "BenchmarkBig-8", Change: 25}) {
		t.Fatalf("findings = %v, want only the sec/op Big regression", got)
	}
}

func TestEvaluate_EnforcesSelectedMetricOnly(t *testing.T) {
	got, err := Evaluate(strings.NewReader(gateCSV), map[string]bool{"B/op": true, "allocs/op": true}, 20)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(got) != 2 || got[0].Metric != "B/op" || got[1].Metric != "allocs/op" {
		t.Fatalf("findings = %v, want the B/op and allocs/op regressions", got)
	}
}

func TestEvaluate_ThresholdIsExclusive(t *testing.T) {
	// +20.00% is exactly at the threshold and must not fail; +20.01% must.
	at := ",base,,cur,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkEdge-8,1.0e-06,,1.2e-06,,+20.00%,p=0.001 n=6\n"
	got, err := Evaluate(strings.NewReader(at), nil, 20)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("exact-threshold change produced %v, want none", got)
	}

	over := strings.Replace(at, "+20.00%", "+20.01%", 1)
	got, err = Evaluate(strings.NewReader(over), nil, 20)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(got) != 1 || got[0].Change != 20.01 {
		t.Fatalf("above-threshold change produced %v, want one at 20.01", got)
	}
}

func TestEvaluate_NoRegressions(t *testing.T) {
	cases := map[string]string{
		"empty":             "",
		"preamble only":     "goos: linux\ncpu: test\n",
		"improvement":       ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,9.0e-07,,-10.00%,p=0.001 n=6\n",
		"noise":             ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,1.1e-06,,~,p=0.900 n=6\n",
		"single input only": ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,,,5.0e-07,∞\n",
		"geomean only":      ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\ngeomean,1.0e-06,,1.1e-06,,+50.00%,\n",
		"unparseable cell":  ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,1.1e-06,,?,p=0.001 n=6\n",
	}
	for name, csv := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := Evaluate(strings.NewReader(csv), nil, 20)
			if err != nil {
				t.Fatalf("Evaluate: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("findings = %v, want none", got)
			}
		})
	}
}

func TestEvaluate_MalformedCSV(t *testing.T) {
	_, err := Evaluate(strings.NewReader("a,\"b\n"), nil, 20)
	if err == nil {
		t.Fatal("Evaluate accepted a malformed CSV, want an error")
	}
	if !strings.Contains(err.Error(), "parse benchstat CSV") {
		t.Fatalf("error = %v, want it to name the benchstat CSV parse", err)
	}
}

func TestPositiveChange(t *testing.T) {
	cases := []struct {
		cell string
		want float64
		ok   bool
	}{
		{"+25.00%", 25, true},
		{"+0.00%", 0, true},
		{"+20.01%", 20.01, true},
		{"~", 0, false},
		{"-10.00%", 0, false},
		{"+abc%", 0, false},
		{"25.00%", 0, false},
		{"+25.00", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := positiveChange(c.cell)
		if got != c.want || ok != c.ok {
			t.Errorf("positiveChange(%q) = (%v, %v), want (%v, %v)", c.cell, got, ok, c.want, c.ok)
		}
	}
}

func TestFinding_String(t *testing.T) {
	got := Finding{Metric: "sec/op", Name: "BenchmarkA-8", Change: 25.5}.String()
	if want := "sec/op BenchmarkA-8: +25.50%"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "delta.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return path
}

func TestRun_NoRegression(t *testing.T) {
	path := writeTemp(t, ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,9.0e-07,,-10.00%,p=0.001 n=6\n")
	var out, errBuf bytes.Buffer
	if code := Run([]string{path}, nil, &out, &errBuf); code != 0 {
		t.Fatalf("Run exit = %d, want 0; stdout=%q stderr=%q", code, out.String(), errBuf.String())
	}
	if !strings.Contains(out.String(), "no regression over 20.00%") {
		t.Fatalf("stdout = %q, want the success line", out.String())
	}
}

func TestRun_Regression(t *testing.T) {
	path := writeTemp(t, ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,1.5e-06,,+50.00%,p=0.001 n=6\n")
	var out, errBuf bytes.Buffer
	if code := Run([]string{"-threshold", "25", path}, nil, &out, &errBuf); code != 1 {
		t.Fatalf("Run exit = %d, want 1; stdout=%q stderr=%q", code, out.String(), errBuf.String())
	}
	if !strings.Contains(out.String(), "sec/op BenchmarkA-8: +50.00%") {
		t.Fatalf("stdout = %q, want the finding", out.String())
	}
	if !strings.Contains(errBuf.String(), "1 benchmark(s) regressed by more than 25.00%") {
		t.Fatalf("stderr = %q, want the summary", errBuf.String())
	}
}

func TestRun_MultipleFilesAndMetricFilter(t *testing.T) {
	timeCSV := ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,1.5e-06,,+50.00%,p=0.001 n=6\n"
	allocCSV := ",b,,c,,,\n,B/op,CI,B/op,CI,vs base,P\nBenchmarkA-8,10,,20,,+100.00%,p=0.001 n=6\n"
	var out, errBuf bytes.Buffer
	code := Run([]string{"-threshold", "20", "-metrics", " sec/op ,", writeTemp(t, timeCSV), writeTemp(t, allocCSV)}, nil, &out, &errBuf)
	if code != 1 {
		t.Fatalf("Run exit = %d, want 1; stdout=%q stderr=%q", code, out.String(), errBuf.String())
	}
	// The B/op regression must be filtered out; only the sec/op one remains.
	if strings.Contains(out.String(), "B/op") {
		t.Fatalf("stdout = %q, want the B/op regression filtered out", out.String())
	}
	if !strings.Contains(out.String(), "sec/op BenchmarkA-8: +50.00%") {
		t.Fatalf("stdout = %q, want the sec/op finding", out.String())
	}
}

func TestRun_ReadsStdin(t *testing.T) {
	regression := ",b,,c,,,\n,sec/op,CI,sec/op,CI,vs base,P\nBenchmarkA-8,1.0e-06,,1.5e-06,,+50.00%,p=0.001 n=6\n"
	var out, errBuf bytes.Buffer
	if code := Run(nil, strings.NewReader(regression), &out, &errBuf); code != 1 {
		t.Fatalf("Run exit = %d, want 1; stdout=%q stderr=%q", code, out.String(), errBuf.String())
	}
}

func TestRun_OpenError(t *testing.T) {
	var out, errBuf bytes.Buffer
	path := filepath.Join(t.TempDir(), "missing.csv")
	if code := Run([]string{path}, nil, &out, &errBuf); code != 2 {
		t.Fatalf("Run exit = %d, want 2", code)
	}
	if !strings.Contains(errBuf.String(), "benchgate:") {
		t.Fatalf("stderr = %q, want the open error", errBuf.String())
	}
}

func TestRun_EvaluateError(t *testing.T) {
	path := writeTemp(t, "a,\"b\n")
	var out, errBuf bytes.Buffer
	if code := Run([]string{path}, nil, &out, &errBuf); code != 2 {
		t.Fatalf("Run exit = %d, want 2; stderr=%q", code, errBuf.String())
	}
}

func TestRun_FlagError(t *testing.T) {
	var out, errBuf bytes.Buffer
	if code := Run([]string{"-not-a-flag"}, nil, &out, &errBuf); code != 2 {
		t.Fatalf("Run exit = %d, want 2", code)
	}
}
