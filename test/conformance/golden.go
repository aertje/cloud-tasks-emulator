package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// This file holds the golden plumbing shared by every battery: the Diff type and
// the generic save/load/compare cores. Each battery (errors, happy-path) wraps
// these with its own record type and per-case comparison - see SaveErrors /
// CompareErrors (record.go) and SaveHappyPath / CompareHappyPath (snapshot.go).

// Diff describes one mismatch between a recorded result and the golden.
type Diff struct {
	Case  string
	Field string
	Want  string // golden
	Got   string // recorded
}

func (d Diff) String() string {
	return fmt.Sprintf("%s: %s\n  want (real): %q\n  got  (emu):  %q", d.Case, d.Field, d.Want, d.Got)
}

// saveGolden writes items to path as indented JSON, sorted by name so diffs are
// stable across runs. It is the shared core of the per-battery Save* helpers.
func saveGolden[T any](path string, items []T, name func(T) string) error {
	sorted := make([]T, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool { return name(sorted[i]) < name(sorted[j]) })

	b, err := json.MarshalIndent(sorted, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

// loadGolden reads a golden file into a map keyed by each item's name. It is the
// shared core of the per-battery Load* helpers.
func loadGolden[T any](path string, name func(T) string) (map[string]T, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var items []T
	if err := json.Unmarshal(b, &items); err != nil {
		return nil, err
	}
	m := make(map[string]T, len(items))
	for _, it := range items {
		m[name(it)] = it
	}
	return m, nil
}

// compareByName diffs recorded items against a golden by name. It handles the
// presence bookkeeping shared by every battery - absent-in-golden and
// missing-from-recording - and delegates each matched case's field comparison to
// cmp. It is the shared core of the per-battery Compare* helpers.
func compareByName[T any](golden map[string]T, got []T, name func(T) string, cmp func(want, got T) []Diff) []Diff {
	var diffs []Diff
	seen := make(map[string]bool, len(got))

	for _, g := range got {
		n := name(g)
		seen[n] = true
		want, ok := golden[n]
		if !ok {
			diffs = append(diffs, Diff{Case: n, Field: "presence", Want: "absent in golden", Got: "recorded"})
			continue
		}
		diffs = append(diffs, cmp(want, g)...)
	}
	for n := range golden {
		if !seen[n] {
			diffs = append(diffs, Diff{Case: n, Field: "presence", Want: "recorded", Got: "missing"})
		}
	}
	return diffs
}
