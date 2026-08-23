package engine

import (
	"strings"
	"time"
)

// Task-size limit enforcement.
//
// Real Cloud Tasks caps task size at CreateTask, and the size it measures is
// the serialized proto size of the *canonicalized stored task* - the
// FULL-view form after server defaults are applied - not the submitted
// request. The law was probed empirically (test/conformance/cmd/probe run of
// 2026-08-23 against project cloudtasksemu) and verified byte-exact by
// reconstruction (test/conformance/cmd/sizecalc):
//
//   - One threshold for both target families: reject when the stored form
//     exceeds 1,048,569 bytes (1MiB - 7). The documented 100KB App Engine cap
//     did not reproduce; App Engine tasks accept ~1MB like HTTP tasks.
//   - Defaults already in the stored form weigh nothing: an explicit method
//     was free on both targets, and an explicit 900s dispatch deadline was
//     free on HTTP (default 600s stored, same encoding). On App Engine the
//     same deadline weighed exactly its 5-byte encoding, so the AE stored
//     form carries no default deadline and one is counted only when the
//     caller set it.
//   - App Engine routing counts as a host-only string without scheme, in the
//     regional format ("[service.]project.<region>.r.appspot.com" - the +5
//     of the region infix vs the legacy format is what balances the AE
//     baseline against the shared threshold): a service weighed exactly
//     len(service)+1, i.e. folded into the host with a "." separator, not
//     stored separately. Version/instance are extrapolated to fold the same
//     way. The emulator counts whatever host it actually stored, so AE sizes
//     are byte-exact against a modern real project when AppEngineRegionID is
//     configured to that project's region and undercount by the host-length
//     difference otherwise.
//   - The stored App Engine headers include the materialized Content-Length
//     and the default Content-Type (per golden/happypath.json), which
//     setInitialTaskState already mirrors, so they are counted for free here.
//
// The functions below compute that stored-form proto size arithmetically from
// the canonical TaskState, keeping the engine free of proto dependencies. All
// relevant field numbers are below 16, so every field key is one byte.

// maxStoredTaskProtoSize is the largest stored-form size real Cloud Tasks
// accepts, measured as the exact create/reject boundary for both target
// families in the 2026-08-23 probe run.
const maxStoredTaskProtoSize = 1<<20 - 7

// validateTaskSize rejects a task whose canonicalized stored form exceeds the
// real Cloud Tasks size limit. s must be the canonical state produced by
// buildTask (defaults applied, name assigned) - the limit is defined over
// that form, so checking the raw creation input would miscount.
// deadlineExplicit says whether the creation input carried a dispatch
// deadline: the canonical state always holds one, but real Cloud Tasks counts
// an App Engine task's deadline only when the caller set it (see the
// stored-form notes above), so the input-presence bit must survive to here.
func validateTaskSize(s TaskState, deadlineExplicit bool) error {
	if storedTaskProtoSize(s, deadlineExplicit) > maxStoredTaskProtoSize {
		return ErrTaskTooLarge
	}
	return nil
}

// storedTaskProtoSize is the serialized size of the canonical stored task as
// real Cloud Tasks counts it.
func storedTaskProtoSize(s TaskState, deadlineExplicit bool) int {
	size := lenFieldSize(len(s.Name))
	if t, ok := s.ScheduleTime.Get(); ok {
		size += timestampFieldSize(t)
	}
	if t, ok := s.CreateTime.Get(); ok {
		size += timestampFieldSize(t)
	}
	if d, ok := s.DispatchDeadline.Get(); ok {
		// HTTP tasks store the 600s default; App Engine tasks store a
		// deadline only when the caller supplied one.
		if s.HTTPRequest.IsPresent() || deadlineExplicit {
			size += durationFieldSize(d)
		}
	}
	if hr, ok := s.HTTPRequest.Get(); ok {
		size += lenFieldSize(httpRequestProtoSize(hr))
	}
	if ae, ok := s.AppEngineHTTPRequest.Get(); ok {
		size += lenFieldSize(appEngineRequestProtoSize(ae))
	}
	return size
}

func httpRequestProtoSize(hr HTTPRequest) int {
	size := lenFieldSize(len(hr.URL.OrZero()))
	// The method enum: always present on a canonical task, and every legal
	// value (POST=1 .. OPTIONS=7) is a one-byte varint after its key.
	size += 2
	size += headersFieldSize(hr.Headers.OrZero())
	if body := hr.Body.OrZero(); len(body) > 0 {
		size += lenFieldSize(len(body))
	}
	if tok, ok := hr.OIDCToken.Get(); ok {
		// Unverified against real Cloud Tasks: the probe's OIDC case was
		// inconclusive because real CreateTask rejects a nonexistent service
		// account with NotFound (an existence check the emulator doesn't
		// perform). Extrapolated from the proto-tracking law.
		payload := 0
		if len(tok.ServiceAccountEmail) > 0 {
			payload += lenFieldSize(len(tok.ServiceAccountEmail))
		}
		if aud := tok.Audience.OrZero(); len(aud) > 0 {
			payload += lenFieldSize(len(aud))
		}
		size += lenFieldSize(payload)
	}
	return size
}

func appEngineRequestProtoSize(ae AppEngineHTTPRequest) int {
	size := 2 // method enum, as in httpRequestProtoSize
	host := storedRoutingHost(ae.AppEngineRouting.OrZero().Host.OrZero())
	size += lenFieldSize(lenFieldSize(len(host))) // routing message holding one host field
	size += lenFieldSize(len(ae.RelativeURI.OrZero()))
	size += headersFieldSize(ae.Headers.OrZero())
	if body := ae.Body.OrZero(); len(body) > 0 {
		size += lenFieldSize(len(body))
	}
	return size
}

// storedRoutingHost translates the emulator's stored routing host into the
// form real Cloud Tasks counts: no scheme, components joined with ".". The
// emulator stores "https://[parts-dot-]project.appspot.com" (or the operator
// emulator-host override, whose components already use ".").
func storedRoutingHost(host string) string {
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	return strings.ReplaceAll(host, "-dot-", ".")
}

// headersFieldSize sums the map-entry encodings of a headers map: each entry
// is a length-delimited message holding a string key and value, with empty
// strings omitted per proto3.
func headersFieldSize(headers map[string]string) int {
	size := 0
	for k, v := range headers {
		payload := 0
		if len(k) > 0 {
			payload += lenFieldSize(len(k))
		}
		if len(v) > 0 {
			payload += lenFieldSize(len(v))
		}
		size += lenFieldSize(payload)
	}
	return size
}

// timestampFieldSize is the encoded size of a google.protobuf.Timestamp field
// (zero seconds/nanos omitted per proto3).
func timestampFieldSize(t time.Time) int {
	payload := 0
	if s := t.Unix(); s != 0 {
		payload += 1 + varintSize(uint64(s))
	}
	if ns := int64(t.Nanosecond()); ns != 0 {
		payload += 1 + varintSize(uint64(ns))
	}
	return lenFieldSize(payload)
}

// durationFieldSize is the encoded size of a google.protobuf.Duration field.
func durationFieldSize(d time.Duration) int {
	payload := 0
	if s := int64(d / time.Second); s != 0 {
		payload += 1 + varintSize(uint64(s))
	}
	if ns := int64(d % time.Second); ns != 0 {
		payload += 1 + varintSize(uint64(ns))
	}
	return lenFieldSize(payload)
}

// lenFieldSize is the encoded size of a length-delimited field (string,
// bytes, submessage) of n payload bytes under a one-byte key.
func lenFieldSize(n int) int {
	return 1 + varintSize(uint64(n)) + n
}

// varintSize is the number of bytes v occupies as a proto varint.
func varintSize(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
