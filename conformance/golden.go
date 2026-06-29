package conformance

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

// Save writes results to path as indented JSON, sorted by case name so diffs
// are stable across runs.
func Save(path string, results []CaseResult) error {
	sorted := make([]CaseResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	// Drop per-variant detail from the committed snapshot; the aggregate is the
	// contract. Keep variants only in ad-hoc dumps if a caller wants them.
	for i := range sorted {
		sorted[i].Variants = nil
	}

	b, err := json.MarshalIndent(sorted, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return ioutil.WriteFile(path, append(b, '\n'), 0644)
}

// Load reads a golden snapshot keyed by case name.
func Load(path string) (map[string]CaseResult, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var results []CaseResult
	if err := json.Unmarshal(b, &results); err != nil {
		return nil, err
	}
	m := make(map[string]CaseResult, len(results))
	for _, r := range results {
		m[r.Name] = r
	}
	return m, nil
}

// Diff describes one mismatch between a recorded result and the golden.
type Diff struct {
	Case    string
	Field   string
	Want    string // golden
	Got     string // recorded
}

func (d Diff) String() string {
	return fmt.Sprintf("%s: %s\n  want (real): %q\n  got  (emu):  %q", d.Case, d.Field, d.Want, d.Got)
}

// Compare checks recorded results against a golden snapshot, returning a Diff
// per mismatched code or template. Cases present in one set but not the other
// are reported too.
func Compare(golden map[string]CaseResult, got []CaseResult) []Diff {
	var diffs []Diff
	seen := make(map[string]bool, len(got))

	for _, g := range got {
		seen[g.Name] = true
		want, ok := golden[g.Name]
		if !ok {
			diffs = append(diffs, Diff{Case: g.Name, Field: "presence", Want: "absent in golden", Got: "recorded"})
			continue
		}
		if want.Code != g.Code {
			diffs = append(diffs, Diff{Case: g.Name, Field: "code", Want: want.Code, Got: g.Code})
		}
		if want.Template != g.Template {
			diffs = append(diffs, Diff{Case: g.Name, Field: "template", Want: want.Template, Got: g.Template})
		}
	}
	for name := range golden {
		if !seen[name] {
			diffs = append(diffs, Diff{Case: name, Field: "presence", Want: "recorded", Got: "missing"})
		}
	}
	return diffs
}
