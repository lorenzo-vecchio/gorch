package gorch

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// This file keeps README.md honest (issue #53). It treats the README as a
// first-class artifact with a checked contract:
//
//   - every complete Go program in it compiles, and every illustrative fragment
//     at least parses as Go;
//   - every heading is reachable from the table of contents and vice versa;
//   - the examples index matches the examples/ tree on disk;
//   - every exported sentinel, every ServiceStatus and every Metrics counter is
//     documented;
//   - no relative link points at a deleted file;
//   - the "README is the only markdown document" constraint still holds.
//
// A complete program is a ```go block that starts with a package clause. Every
// other Go block must carry an <!-- fragment --> marker on the line directly
// above its opening fence; the marker is how the README distinguishes a
// copy-and-run program from an illustrative fragment.

const readmePath = "README.md"

// readmeGoBlock is one fenced Go code block from the README.
type readmeGoBlock struct {
	line     int    // 1-based line of the opening fence
	fragment bool   // marked as an illustrative fragment
	body     string // the block's text, without the fences
}

// readReadme returns the README's content.
func readReadme(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("read %s: %v", readmePath, err)
	}
	return string(data)
}

// readmeGoBlocks extracts every `go` fenced block, recording whether the line
// immediately above the fence is the fragment marker.
func readmeGoBlocks(t *testing.T, readme string) []readmeGoBlock {
	t.Helper()
	lines := strings.Split(readme, "\n")
	var blocks []readmeGoBlock
	for i := 0; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != "```go" {
			continue
		}
		start := i
		i++
		var body []string
		for i < len(lines) && strings.TrimSpace(lines[i]) != "```" {
			body = append(body, lines[i])
			i++
		}
		marked := start > 0 && strings.TrimSpace(lines[start-1]) == "<!-- fragment -->"
		blocks = append(blocks, readmeGoBlock{line: start + 1, fragment: marked, body: strings.Join(body, "\n")})
	}
	return blocks
}

// TestReadme_GoCodeBlocksCompile is the load-bearing check from issue #53: every
// code block a reader might copy compiles, and every fragment is explicitly
// marked as illustrative. Complete programs are built together in one throwaway
// module that replaces the published import path with this working tree, so the
// build exercises the real API; a fragment is only parsed, because a fragment
// intentionally references identifiers defined outside it.
func TestReadme_GoCodeBlocksCompile(t *testing.T) {
	blocks := readmeGoBlocks(t, readReadme(t))
	if len(blocks) == 0 {
		t.Fatal("README has no go code blocks")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	var programs []readmeGoBlock
	for _, b := range blocks {
		if b.fragment {
			if err := parsesAsGo(b.body); err != nil {
				t.Errorf("%s:%d: marked as a fragment but is not valid Go: %v", readmePath, b.line, err)
			}
			continue
		}
		if !strings.HasPrefix(b.body, "package ") && !strings.Contains(b.body, "\npackage ") {
			t.Errorf("%s:%d: block is neither a complete program nor marked <!-- fragment -->", readmePath, b.line)
			continue
		}
		if err := parsesAsGo(b.body); err != nil {
			t.Errorf("%s:%d: complete program does not parse: %v", readmePath, b.line, err)
			continue
		}
		programs = append(programs, b)
	}
	if len(programs) == 0 {
		t.Fatal("README has no complete Go program; the quickstart must be copy-and-run")
	}

	buildReadmePrograms(t, root, programs)
}

// parsesAsGo reports whether src is syntactically valid Go, either as a whole
// file or as the body of a function. The second form covers statement fragments.
func parsesAsGo(src string) error {
	fset := token.NewFileSet()
	if _, err := parser.ParseFile(fset, "snippet.go", src, parser.AllErrors); err == nil {
		return nil
	}
	if _, err := parser.ParseFile(fset, "snippet.go", "package main\n"+src, parser.AllErrors); err == nil {
		return nil
	}
	_, err := parser.ParseFile(fset, "snippet.go", "package main\nfunc _() {\n"+src+"\n}\n", parser.AllErrors)
	return err
}

// buildReadmePrograms writes every complete program into a temporary module and
// builds it. One module for all programs keeps the cost to a single `go build`.
func buildReadmePrograms(t *testing.T, root string, programs []readmeGoBlock) {
	t.Helper()
	dir := t.TempDir()
	gomod := "module readmetest\n\ngo 1.25\n\nrequire github.com/lorenzo-vecchio/gorch v0.0.0\n\nreplace github.com/lorenzo-vecchio/gorch => " + root + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}
	for i, p := range programs {
		sub := filepath.Join(dir, "prog"+string(rune('0'+i)))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(sub, "main.go"), []byte(p.body+"\n"), 0o644); err != nil {
			t.Fatalf("write program: %v", err)
		}
	}

	cmd := exec.Command("go", "build", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
	cmd.Stdin = nil
	done := make(chan struct{})
	var out []byte
	var runErr error
	go func() {
		out, runErr = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Minute):
		_ = cmd.Process.Kill()
		t.Fatal("go build of README programs timed out")
	}
	if runErr != nil {
		t.Fatalf("README complete programs do not compile:\n%s", out)
	}
}

// TestReadme_TableOfContents pins the navigability requirement: every section
// heading has a table-of-contents entry and every entry points at a real
// heading.
func TestReadme_TableOfContents(t *testing.T) {
	readme := readReadme(t)
	headings := readmeHeadings(readme)
	if len(headings) == 0 {
		t.Fatal("README has no ## / ### headings")
	}

	anchorSet := map[string]string{}
	for _, h := range headings {
		anchorSet[githubAnchor(h)] = h
	}

	body := stripFencedBlocks(readme)
	tocRe := regexp.MustCompile(`(?m)^\s*-\s+\[[^\]]+\]\(#([^)]+)\)`)
	toc := map[string]bool{}
	for _, m := range tocRe.FindAllStringSubmatch(body, -1) {
		toc[m[1]] = true
	}

	for anchor, heading := range anchorSet {
		if anchor == "table-of-contents" {
			continue
		}
		if !toc[anchor] {
			t.Errorf("heading %q (#%s) is missing from the table of contents", heading, anchor)
		}
	}
	for anchor := range toc {
		if _, ok := anchorSet[anchor]; !ok {
			t.Errorf("table of contents links to #%s, which is not a heading", anchor)
		}
	}
}

// readmeHeadings returns the text of every ## and ### heading, excluding fenced
// code.
func readmeHeadings(readme string) []string {
	var headings []string
	for _, line := range strings.Split(stripFencedBlocks(readme), "\n") {
		if strings.HasPrefix(line, "## ") {
			headings = append(headings, strings.TrimSpace(strings.TrimPrefix(line, "## ")))
		} else if strings.HasPrefix(line, "### ") {
			headings = append(headings, strings.TrimSpace(strings.TrimPrefix(line, "### ")))
		}
	}
	return headings
}

// githubAnchor approximates GitHub's heading-anchor algorithm: lower-case, keep
// letters, digits, spaces, hyphens and underscores, then spaces become hyphens.
func githubAnchor(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == ' ', r == '-', r == '_':
			b.WriteRune(r)
		}
	}
	return strings.ReplaceAll(b.String(), " ", "-")
}

// stripFencedBlocks blanks out fenced code so heading and link scans never see
// examples.
func stripFencedBlocks(readme string) string {
	var b strings.Builder
	inFence := false
	for _, line := range strings.Split(readme, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			inFence = !inFence
			b.WriteString("\n")
			continue
		}
		if inFence {
			b.WriteString("\n")
			continue
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

// TestReadme_ExamplesIndexMatchesTree pins that the examples index lists every
// runnable example and invents none.
func TestReadme_ExamplesIndexMatchesTree(t *testing.T) {
	readme := stripFencedBlocks(readReadme(t))
	re := regexp.MustCompile(`\]\(examples/([A-Za-z0-9_-]+)/?\)`)
	documented := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(readme, -1) {
		documented[m[1]] = true
	}

	entries, err := os.ReadDir("examples")
	if err != nil {
		t.Fatalf("read examples dir: %v", err)
	}
	actual := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			actual[e.Name()] = true
		}
	}

	if len(actual) == 0 {
		t.Fatal("examples/ has no directories")
	}
	for name := range actual {
		if !documented[name] {
			t.Errorf("examples/%s/ is not in the README examples index", name)
		}
	}
	for name := range documented {
		if !actual[name] {
			t.Errorf("README examples index names examples/%s/, which does not exist", name)
		}
	}
}

// TestReadme_DocumentsStableSurface pins that every exported sentinel, every
// ServiceStatus and every Metrics counter appears in the README, so the code and
// the reference cannot drift.
func TestReadme_DocumentsStableSurface(t *testing.T) {
	readme := readReadme(t)

	for _, name := range sourceNames(t, "service.go", `(Err[A-Z][A-Za-z0-9]+)\s*=\s*errors\.New`) {
		if !strings.Contains(readme, name) {
			t.Errorf("sentinel %s is undocumented in the README", name)
		}
	}
	for _, name := range sourceNames(t, "health.go", `(Status[A-Z][A-Za-z0-9]+)\s+ServiceStatus`) {
		if !strings.Contains(readme, name) {
			t.Errorf("ServiceStatus %s is undocumented in the README", name)
		}
	}
	for _, name := range sourceNames(t, "metrics.go", `^\s*([A-Z][A-Za-z0-9]+)\s+int64`) {
		if !strings.Contains(readme, name) {
			t.Errorf("Metrics counter %s is undocumented in the README", name)
		}
	}

	if !strings.Contains(readme, "doc.go") {
		t.Error("README does not state its split of responsibilities with doc.go")
	}
}

// sourceNames returns the capture groups matched by pattern in file.
func sourceNames(t *testing.T, file, pattern string) []string {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	re := regexp.MustCompile(pattern)
	seen := map[string]bool{}
	var names []string
	for _, m := range re.FindAllStringSubmatch(string(data), -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			names = append(names, m[1])
		}
	}
	sort.Strings(names)
	return names
}

// TestReadme_RelativeLinksExist pins that no relative markdown link points at a
// file this repository deleted (the v0.9 milestone removed the migration guide).
func TestReadme_RelativeLinksExist(t *testing.T) {
	body := stripFencedBlocks(readReadme(t))
	re := regexp.MustCompile(`\]\(([^)]+)\)`)
	for _, m := range re.FindAllStringSubmatch(body, -1) {
		target := m[1]
		if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
			strings.HasPrefix(target, "#") || strings.HasPrefix(target, "mailto:") {
			continue
		}
		if i := strings.IndexByte(target, '#'); i >= 0 {
			target = target[:i]
		}
		if target == "" {
			continue
		}
		if _, err := os.Stat(target); err != nil {
			t.Errorf("README links to %q, which does not exist: %v", target, err)
		}
	}
}

// TestReadme_OnlyMarkdownDoc pins the documented policy: until v1.0 the README
// is the only public markdown document.
func TestReadme_OnlyMarkdownDoc(t *testing.T) {
	for _, forbidden := range []string{"CHANGELOG.md", "MIGRATION.md", "ROADMAP.md", "CONTRIBUTING.md"} {
		if _, err := os.Stat(forbidden); err == nil {
			t.Errorf("%s exists; the README is the only markdown document until v1.0", forbidden)
		}
	}
	if info, err := os.Stat("docs"); err == nil && info.IsDir() {
		t.Error("docs/ exists; long-form guides belong in the README until v1.0")
	}
}
