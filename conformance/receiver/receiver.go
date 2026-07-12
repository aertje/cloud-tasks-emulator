// Package receiver is a tiny HTTP target that records the request headers Cloud
// Tasks attaches when it dispatches (and re-dispatches) a task. It exists to
// capture the two *optional* retry headers whose value format is undocumented:
// X-CloudTasks-TaskPreviousResponse / X-CloudTasks-TaskRetryReason and their
// X-AppEngine-* equivalents.
//
// It backs two consumers from one code path, so what the conformance test drives
// locally is byte-for-byte the same handler that recorded the golden:
//
//   - Deployed to App Engine (see cmd/recv + app.yaml) it observes real Cloud
//     Tasks dispatches. Both target families reach it: an HTTP-target task points
//     its URL at https://PROJECT.appspot.com/recv/http, and an App Engine-target
//     task routes to /recv/appengine on the same default service. This is the
//     only vantage point that can see the X-AppEngine-* retry headers, because
//     App Engine-target tasks route through internal App Engine routing that
//     cannot be pointed at a tunnel.
//   - Run as a plain local server it lets the conformance harness drive the
//     emulator against an identical target, keeping validation hermetic.
//
// # Forcing retries
//
// The optional headers only appear on a *re-dispatch*, so the handler forces two
// retries per task, with a different failure status each time: it fails the
// first delivery with 503 and the second with 404, then succeeds (200) on every
// later attempt, keyed off the task's retry-count header. Two differing failures
// capture the optional headers for both a 5XX and a 4XX prior response, since
// their X-*-TaskRetryReason (and, for HTTP targets, X-*-TaskExecutionCount, which
// excludes 5XX) may differ between the two.
//
// # Capture & readback
//
// Every request's headers are recorded in memory, keyed by task name + attempt.
// In-memory is sufficient because the App Engine deploy is pinned to a single
// instance (basic_scaling: max_instances: 1). A recorder reads back what was
// seen via GET /captures?run=<prefix>, filtered to the run-scoped resource-name
// prefix so concurrent or historical runs do not bleed into each other.
package receiver

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Request paths the two target families are pointed at. They are recorded on the
// Capture so a reader can tell which family a request arrived on even before
// inspecting its headers.
const (
	PathHTTP      = "/recv/http"
	PathAppEngine = "/recv/appengine"
	PathCaptures  = "/captures"
)

// Capture is one received request: which endpoint it hit, the task/queue it
// belonged to, which attempt it was, the status this handler returned, and every
// request header verbatim.
//
// Header keys are canonicalised by net/http on receipt (e.g. the wire's
// X-CloudTasks-TaskName is stored as X-Cloudtasks-Taskname); header *values* are
// preserved exactly, which is what matters for the undocumented retry-reason
// format. The documented header names remain authoritative for spelling.
type Capture struct {
	Endpoint  string            `json:"endpoint"`  // request path: /recv/http | /recv/appengine
	Family    string            `json:"family"`    // cloudtasks | appengine | unknown
	QueueName string            `json:"queueName"` // X-*-QueueName (short queue id)
	TaskName  string            `json:"taskName"`  // X-*-TaskName (short task id)
	Attempt   int               `json:"attempt"`   // X-*-TaskRetryCount (0 on first delivery)
	Status    int               `json:"status"`    // status this handler returned
	Headers   map[string]string `json:"headers"`   // all request headers, values comma-joined
}

// store is the in-memory capture log, appended to on every dispatch and read
// back through /captures. A single mutex is ample: dispatch volume is a handful
// of requests per run.
type store struct {
	mu       sync.Mutex
	captures []Capture
}

func (s *store) add(c Capture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captures = append(s.captures, c)
}

// filter returns a copy of the captures whose queue or task name contains run,
// sorted by (endpoint, attempt) for a deterministic readback. An empty run
// returns everything.
func (s *store) filter(run string) []Capture {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]Capture, 0, len(s.captures))
	for _, c := range s.captures {
		if run == "" || strings.Contains(c.QueueName, run) || strings.Contains(c.TaskName, run) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Endpoint != out[j].Endpoint {
			return out[i].Endpoint < out[j].Endpoint
		}
		return out[i].Attempt < out[j].Attempt
	})
	return out
}

// NewHandler builds the receiver's HTTP handler: the two dispatch endpoints that
// force one retry and record each request, plus the /captures readback. The same
// handler serves the App Engine deploy (cmd/recv) and the local test server.
func NewHandler() http.Handler {
	s := &store{}
	mux := http.NewServeMux()
	mux.HandleFunc(PathHTTP, s.dispatch)
	mux.HandleFunc(PathAppEngine, s.dispatch)
	mux.HandleFunc(PathCaptures, s.readback)
	return mux
}

// dispatch records the request and applies the forced-retry response policy:
// fail the first delivery (attempt 0) with 503 and the second (attempt 1) with
// 404 so Cloud Tasks retries twice, then succeed with 200 on every later
// attempt. The two retried requests are the ones that carry the optional
// TaskPreviousResponse / TaskRetryReason headers - for a 5XX and a 4XX prior
// response respectively.
func (s *store) dispatch(w http.ResponseWriter, r *http.Request) {
	family, queue, task, attempt := identify(r)

	status := forcedStatus(attempt)

	c := Capture{
		Endpoint:  r.URL.Path,
		Family:    family,
		QueueName: queue,
		TaskName:  task,
		Attempt:   attempt,
		Status:    status,
		Headers:   collectHeaders(r),
	}
	s.add(c)

	// Dump the full raw headers so they can be eyeballed in the deploy logs while
	// recording the golden - the whole point of the capture.
	log.Printf("dispatch %s family=%s queue=%s task=%s attempt=%d -> %d\n%s",
		r.URL.Path, family, queue, task, attempt, status, dumpHeaders(c.Headers))

	w.WriteHeader(status)
	_, _ = w.Write([]byte(strconv.Itoa(status)))
}

// readback serves the recorded captures for a run as JSON. Query param `run` is
// the run-scoped resource-name prefix; omit it to get everything.
func (s *store) readback(w http.ResponseWriter, r *http.Request) {
	out := s.filter(r.URL.Query().Get("run"))
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("readback encode: %v", err)
	}
}

// forcedStatus is the response policy: attempt 0 fails with 503, attempt 1 fails
// with 404, and every later attempt succeeds with 200. The two distinct failure
// codes force two retries that surface the optional headers for both a 5XX and a
// 4XX prior response.
func forcedStatus(attempt int) int {
	switch attempt {
	case 0:
		return http.StatusServiceUnavailable // 503
	case 1:
		return http.StatusNotFound // 404
	default:
		return http.StatusOK // 200
	}
}

// identify pulls the family and the task-identifying fields out of a request's
// headers, checking both header families. App Engine-target requests carry
// X-AppEngine-* headers; HTTP-target requests carry X-CloudTasks-*.
func identify(r *http.Request) (family, queue, task string, attempt int) {
	if v := r.Header.Get("X-AppEngine-TaskName"); v != "" {
		return "appengine",
			r.Header.Get("X-AppEngine-QueueName"),
			v,
			atoiOr(r.Header.Get("X-AppEngine-TaskRetryCount"), 0)
	}
	if v := r.Header.Get("X-CloudTasks-TaskName"); v != "" {
		return "cloudtasks",
			r.Header.Get("X-CloudTasks-QueueName"),
			v,
			atoiOr(r.Header.Get("X-CloudTasks-TaskRetryCount"), 0)
	}
	return "unknown", "", "", 0
}

// collectHeaders flattens the request headers into a plain map, joining
// multi-valued headers with ", " (their standard header representation).
func collectHeaders(r *http.Request) map[string]string {
	h := make(map[string]string, len(r.Header))
	for k, vs := range r.Header {
		h[k] = strings.Join(vs, ", ")
	}
	return h
}

// dumpHeaders renders headers as sorted "key: value" lines for log eyeballing.
func dumpHeaders(h map[string]string) string {
	lines := make([]string, 0, len(h))
	for k, v := range h {
		lines = append(lines, "  "+k+": "+v)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func atoiOr(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
