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
// The optional headers only appear on a *re-dispatch*, so the standard endpoints
// fail each attempt with a different status - 503, 404, 429, 500, 302 - before
// succeeding, capturing the optional headers for a range of prior status codes
// (they may differ by code, and for HTTP targets X-*-TaskExecutionCount excludes
// 5XX). Separate timeout endpoints (/recv/http-timeout, /recv/appengine-timeout)
// instead let the first attempt exceed the task's dispatch deadline, to capture
// what a no-response failure - rather than an error status - produces on the
// retry (real Cloud Tasks enforces a per-task deadline on both the HTTP and App
// Engine paths).
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
	"time"
)

// Request paths the two target families are pointed at. They are recorded on the
// Capture so a reader can tell which family a request arrived on even before
// inspecting its headers.
const (
	PathHTTP             = "/recv/http"
	PathAppEngine        = "/recv/appengine"
	PathHTTPTimeout      = "/recv/http-timeout"
	PathAppEngineTimeout = "/recv/appengine-timeout"
	PathCaptures         = "/captures"
)

// timeoutSleep is how long the timeout endpoint sleeps on the first attempt. It
// must exceed the dispatch deadline the timeout case sets on its task (15s) so
// the caller cancels the request with no response - a deadline failure rather
// than an HTTP status.
const timeoutSleep = 18 * time.Second

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

// NewHandler builds the receiver's HTTP handler: the two standard dispatch
// endpoints that fail a sequence of attempts before succeeding, the timeout
// endpoints that stall the first attempt past its deadline, and the /captures
// readback. The same handler serves the App Engine deploy (cmd/recv) and the
// local test server.
func NewHandler() http.Handler {
	s := &store{}
	mux := http.NewServeMux()
	mux.HandleFunc(PathHTTP, s.dispatch)
	mux.HandleFunc(PathAppEngine, s.dispatch)
	mux.HandleFunc(PathHTTPTimeout, s.dispatchTimeout)
	mux.HandleFunc(PathAppEngineTimeout, s.dispatchTimeout)
	mux.HandleFunc(PathCaptures, s.readback)
	return mux
}

// dispatch records the request and applies the forced-retry response policy:
// each attempt is failed with the next status in forcedStatusSequence before
// the task finally succeeds with 200. Every failure surfaces the optional
// TaskPreviousResponse / TaskRetryReason headers on the following attempt, so a
// single task captures them across a range of prior status codes.
func (s *store) dispatch(w http.ResponseWriter, r *http.Request) {
	family, queue, task, attempt := identify(r)
	status := forcedStatus(attempt)
	s.record(r, family, queue, task, attempt, status)

	w.WriteHeader(status)
	_, _ = w.Write([]byte(strconv.Itoa(status)))
}

// dispatchTimeout records the request, then on the first attempt sleeps past the
// task's dispatch deadline so the caller cancels it with no response - a timeout
// failure rather than an error status. Later attempts succeed with 200, so the
// retry that follows the timeout is captured (with whatever optional headers a
// no-response failure produces). Status 0 marks the timed-out attempt.
func (s *store) dispatchTimeout(w http.ResponseWriter, r *http.Request) {
	family, queue, task, attempt := identify(r)
	if attempt == 0 {
		s.record(r, family, queue, task, attempt, 0)
		time.Sleep(timeoutSleep)
		return // the caller has almost certainly closed the connection by now
	}

	s.record(r, family, queue, task, attempt, http.StatusOK)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(strconv.Itoa(http.StatusOK)))
}

// record appends one request's headers to the capture log and dumps the raw
// headers to the log for eyeballing in the deploy logs while recording.
func (s *store) record(r *http.Request, family, queue, task string, attempt, status int) {
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

	log.Printf("dispatch %s family=%s queue=%s task=%s attempt=%d -> %d\n%s",
		r.URL.Path, family, queue, task, attempt, status, dumpHeaders(c.Headers))
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

// forcedStatusSequence is the per-attempt failure policy for the standard
// endpoints: each attempt is failed with the next code in this list - a 5XX, a
// 4XX, a rate-limit, another 5XX and a redirect - so the optional retry headers
// are captured for a range of prior status codes. The 302 carries no Location,
// so it is treated as a failure rather than followed.
var forcedStatusSequence = []int{
	http.StatusServiceUnavailable,  // 503
	http.StatusNotFound,            // 404
	http.StatusTooManyRequests,     // 429
	http.StatusInternalServerError, // 500
	http.StatusFound,               // 302
}

// forcedStatus returns the status to fail attempt n with, or 200 once the
// sequence is exhausted and the task should succeed.
func forcedStatus(attempt int) int {
	if attempt >= 0 && attempt < len(forcedStatusSequence) {
		return forcedStatusSequence[attempt]
	}
	return http.StatusOK
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
