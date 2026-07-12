package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "google.golang.org/genproto/googleapis/rpc/errdetails" // register error-detail protos for status.Details
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// perStepTimeout bounds each Setup/Invoke/Teardown step. gRPC propagates the
// context deadline to the server, and real Cloud Tasks rejects any request
// whose deadline is more than 30s in the future - so this must stay under 30s.
// The run's overall context (see cmd/record) remains the total budget.
const perStepTimeout = 25 * time.Second

// DetailRecord is one entry from a gRPC status' details, with its message text
// normalized the same way as the top-level message.
type DetailRecord struct {
	Type     string `json:"type"`
	Template string `json:"template"`
}

// Result is the captured outcome of a single (case, variant) invocation.
type Result struct {
	Params   Params         `json:"params"`
	SetupErr string         `json:"setupErr,omitempty"`
	Code     string         `json:"code"`
	Message  string         `json:"message"`
	Template string         `json:"template"`
	Details  []DetailRecord `json:"details,omitempty"`
}

// CaseResult aggregates a case's variants into the canonical record that goes
// into the golden file. Stable is true when every variant agreed on code and
// template after normalization; when false the variants diverged and the
// template should not be trusted as-is.
type CaseResult struct {
	Name     string         `json:"name"`
	RPC      string         `json:"rpc"`
	Category string         `json:"category"`
	Code     string         `json:"code"`
	Template string         `json:"template"`
	Details  []DetailRecord `json:"details,omitempty"`
	Stable   bool           `json:"stable"`
	Variants []Result       `json:"variants,omitempty"`
}

// RunOptions configure a recording run.
type RunOptions struct {
	Project  string
	Location string
	Prefix   string // run-scoped resource-name prefix, keeps re-runs from colliding
	Variants int    // number of differing-input variants per case (>=2 to detect templates)
}

func (o RunOptions) variants() int {
	if o.Variants < 2 {
		return 2
	}
	return o.Variants
}

// paramsFor builds the resource names for a given case/variant. Project and
// Location are fixed (single real project); QueueID/TaskID vary so the
// normalizer can spot interpolated names.
func (o RunOptions) paramsFor(caseIdx, variant int) Params {
	return Params{
		Project:  o.Project,
		Location: o.Location,
		QueueID:  fmt.Sprintf("%s-c%d-v%d", o.Prefix, caseIdx, variant),
		TaskID:   fmt.Sprintf("%s-c%d-v%d-task", o.Prefix, caseIdx, variant),
	}
}

// RunErrors executes the error battery against the client and returns one
// CaseResult per case. It never aborts on an individual RPC failure - failures
// are the data being collected.
func RunErrors(ctx context.Context, c *Client, opts RunOptions) []CaseResult {
	cases := Cases()
	out := make([]CaseResult, 0, len(cases))

	for ci, cs := range cases {
		var variants []Result
		for v := 0; v < opts.variants(); v++ {
			p := opts.paramsFor(ci, v)
			variants = append(variants, invoke(ctx, c, cs, p))
		}
		out = append(out, aggregate(cs, variants))
	}
	return out
}

func invoke(ctx context.Context, c *Client, cs Case, p Params) Result {
	r := Result{Params: p}

	if cs.Setup != nil {
		if err := withStep(ctx, c, p, cs.Setup); err != nil {
			r.SetupErr = err.Error()
		}
	}

	err := withStep(ctx, c, p, cs.Invoke)

	if cs.Teardown != nil {
		_ = withStep(ctx, c, p, cs.Teardown) // best-effort
	}

	st, _ := status.FromError(err)
	r.Code = st.Code().String()
	r.Message = st.Message()
	r.Template = Normalize(r.Message, p)
	for _, d := range st.Details() {
		msg, ok := d.(proto.Message)
		if !ok {
			continue
		}
		text := prototext.MarshalOptions{}.Format(msg)
		r.Details = append(r.Details, DetailRecord{
			Type:     fmt.Sprintf("%T", d),
			Template: Normalize(text, p),
		})
	}
	return r
}

// withStep runs one case step under a bounded deadline so the deadline gRPC
// sends to the server stays within Cloud Tasks' 30s cap.
func withStep(ctx context.Context, c *Client, p Params, fn func(context.Context, *Client, Params) error) error {
	ctx, cancel := context.WithTimeout(ctx, perStepTimeout)
	defer cancel()
	return fn(ctx, c, p)
}

func aggregate(cs Case, variants []Result) CaseResult {
	res := CaseResult{
		Name:     cs.Name,
		RPC:      cs.RPC,
		Category: cs.Category,
		Variants: variants,
		Stable:   true,
	}
	if len(variants) == 0 {
		res.Stable = false
		return res
	}
	first := variants[0]
	res.Code = first.Code
	res.Template = first.Template
	res.Details = first.Details
	for _, v := range variants[1:] {
		if v.Code != first.Code || v.Template != first.Template {
			res.Stable = false
		}
	}
	return res
}

// SaveErrors writes error-battery results to path (see saveGolden). Per-variant
// detail is dropped from the committed golden - the aggregate is the contract;
// variants are kept only in ad-hoc dumps if a caller wants them.
func SaveErrors(path string, results []CaseResult) error {
	trimmed := make([]CaseResult, len(results))
	copy(trimmed, results)
	for i := range trimmed {
		trimmed[i].Variants = nil
	}
	return saveGolden(path, trimmed, func(r CaseResult) string { return r.Name })
}

// LoadErrors reads an error-battery golden keyed by case name.
func LoadErrors(path string) (map[string]CaseResult, error) {
	return loadGolden(path, func(r CaseResult) string { return r.Name })
}

// CompareErrors checks recorded error-battery results against a golden,
// returning a Diff per mismatched code, template or details.
func CompareErrors(golden map[string]CaseResult, got []CaseResult) []Diff {
	return compareByName(golden, got,
		func(r CaseResult) string { return r.Name },
		func(want, g CaseResult) []Diff {
			var diffs []Diff
			if want.Code != g.Code {
				diffs = append(diffs, Diff{Case: g.Name, Field: "code", Want: want.Code, Got: g.Code})
			}
			if want.Template != g.Template {
				diffs = append(diffs, Diff{Case: g.Name, Field: "template", Want: want.Template, Got: g.Template})
			}
			if dw, dg := formatDetails(want.Details), formatDetails(g.Details); dw != dg {
				diffs = append(diffs, Diff{Case: g.Name, Field: "details", Want: dw, Got: dg})
			}
			return diffs
		})
}

// formatDetails renders a case's error details into a single canonical string
// for comparison: one "type / template" block per detail, sorted so ordering
// differences between real Cloud Tasks and the emulator don't register as a
// mismatch. Returns "" when there are no details.
//
// Detail templates are prototext, whose field separator is deliberately
// unstable - it emits one or two spaces at random to discourage parsing - so
// the same message can serialise differently between the golden recording and
// an emulator capture. canonicalSpace collapses that noise before comparing.
func formatDetails(ds []DetailRecord) string {
	if len(ds) == 0 {
		return ""
	}
	blocks := make([]string, len(ds))
	for i, d := range ds {
		blocks[i] = d.Type + "\n" + canonicalSpace(d.Template)
	}
	sort.Strings(blocks)
	return strings.Join(blocks, "\n---\n")
}

// canonicalSpace collapses every run of whitespace to a single space so
// prototext's unstable separator doesn't register as a difference.
func canonicalSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
