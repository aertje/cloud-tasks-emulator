package conformance

import (
	"fmt"
	"strings"
)

// Normalize replaces request-specific substrings in a message with stable
// placeholders, so messages captured with different inputs compare equal. The
// derived "template" is what we store and diff. Replacements run most-specific
// first so longer resource paths are matched before their components.
//
// Anything that survives normalization is treated as static text. By running a
// case with several differing inputs and checking the templates agree (see
// CaseResult.Stable), we confirm we have correctly identified every
// interpolated slot - any input value we failed to placeholder would leak
// through and make the variants disagree.
func Normalize(msg string, p Params) string {
	replacements := []struct{ from, to string }{
		{p.TaskPath(), "{task_path}"},
		{p.QueuePath(), "{queue_path}"},
		{p.Parent(), "{parent}"},
		// The location the queue/create/parent-mismatch case points its
		// (well-formed but out-of-parent) queue name at. Placeholdered so that
		// if Cloud Tasks echoes the mismatched name, the real project id it
		// carries does not leak into the committed golden template.
		{fmt.Sprintf("projects/%s/locations/%s", p.Project, otherLocation(p.Location)), "{mismatch_parent}"},
		{p.TaskID, "{task_id}"},
		{p.QueueID, "{queue_id}"},
	}
	for _, r := range replacements {
		if r.from == "" {
			continue
		}
		msg = strings.ReplaceAll(msg, r.from, r.to)
	}
	return msg
}
