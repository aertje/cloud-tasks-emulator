package engine

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the emulator's task-size boundary to the exact accept/reject
// boundaries measured against real Cloud Tasks (the 2026-08-23 probe run
// documented in tasksize.go). Names, URL, schedule time and clock
// reproduce that run's task shapes byte-for-byte: the probe's project was
// cloudtasksemu, its run prefix was 20 characters, and both its timestamps
// encode as 5-byte varints, so a task here has exactly the stored-form size a
// probe task had there. The body lengths and the routing-service weight are
// the real service's measurements, not values derived from the emulator's own
// formula.

const (
	sizeProbeParent = "projects/cloudtasksemu/locations/us-central1"
	sizeProbePrefix = "cte-probe-1234567890"

	// Largest accepted baseline body per target, as measured against real.
	sizeProbeHTTPMaxBody      = 1048368
	sizeProbeAppEngineMaxBody = 1048256
)

var (
	sizeProbeCreateTime   = time.Unix(1_771_000_000, 0)
	sizeProbeScheduleTime = time.Unix(1_770_000_000, 0)
)

// newSizeProbeEngine returns an engine whose clock is pinned to the probe-era
// instant, plus the created queue's name. The region ID matches the probe
// project's (us-central1 = "uc") because real Cloud Tasks counts the regional
// routing host on App Engine tasks; without it the AE shapes here would be 5
// bytes short of the real run's.
func newSizeProbeEngine(t *testing.T, queueSuffix string) (*Engine, string) {
	t.Helper()
	e := newTestEngineOpts(t, Options{
		Dispatcher:        newFakeDispatcher(200),
		clock:             func() time.Time { return sizeProbeCreateTime },
		AppEngineRegionID: maybe.Some("uc"),
	})
	queueName := sizeProbeParent + "/queues/" + sizeProbePrefix + "-" + queueSuffix
	_, err := e.CreateQueue(t.Context(), sizeProbeParent, QueueState{Name: queueName})
	require.NoError(t, err)
	return e, queueName
}

// sizeProbeTaskName mirrors the probe's fixed-width task IDs so every name has
// the same length regardless of the counter value.
func sizeProbeTaskName(queueName string, n int) string {
	return fmt.Sprintf("%s/tasks/%s-t%06d", queueName, sizeProbePrefix, n)
}

func TestCreateTaskSizeLimitHTTP(t *testing.T) {
	e, queueName := newSizeProbeEngine(t, "http")

	httpTask := func(n, bodyLen int) TaskState {
		return TaskState{
			Name:         sizeProbeTaskName(queueName, n),
			ScheduleTime: maybe.Some(sizeProbeScheduleTime),
			HTTPRequest: maybe.Some(HTTPRequest{
				URL:  maybe.Some("https://example.com/"),
				Body: maybe.Some(bytes.Repeat([]byte("a"), bodyLen)),
			}),
		}
	}

	_, frozen, err := e.CreateTask(t.Context(), queueName, httpTask(1, sizeProbeHTTPMaxBody))
	require.NoError(t, err, "task at the real-measured boundary must be accepted")
	assert.Equal(t, maxStoredTaskProtoSize, storedTaskProtoSize(frozen, false),
		"the accepted boundary task must sit exactly at the threshold")

	_, _, err = e.CreateTask(t.Context(), queueName, httpTask(2, sizeProbeHTTPMaxBody+1))
	assert.ErrorIs(t, err, ErrTaskTooLarge)
}

func TestCreateTaskSizeLimitAppEngine(t *testing.T) {
	e, queueName := newSizeProbeEngine(t, "ae")

	aeTask := func(n, bodyLen int, mod func(*AppEngineHTTPRequest)) TaskState {
		ae := AppEngineHTTPRequest{
			Body: maybe.Some(bytes.Repeat([]byte("a"), bodyLen)),
		}
		if mod != nil {
			mod(&ae)
		}
		return TaskState{
			Name:                 sizeProbeTaskName(queueName, n),
			ScheduleTime:         maybe.Some(sizeProbeScheduleTime),
			AppEngineHTTPRequest: maybe.Some(ae),
		}
	}

	_, frozen, err := e.CreateTask(t.Context(), queueName, aeTask(1, sizeProbeAppEngineMaxBody, nil))
	require.NoError(t, err, "task at the real-measured boundary must be accepted")
	assert.Equal(t, maxStoredTaskProtoSize, storedTaskProtoSize(frozen, false),
		"the accepted boundary task must sit exactly at the threshold")

	_, _, err = e.CreateTask(t.Context(), queueName, aeTask(2, sizeProbeAppEngineMaxBody+1, nil))
	assert.ErrorIs(t, err, ErrTaskTooLarge)

	// An explicit 900s dispatch deadline weighed exactly its 5-byte encoding
	// against real Cloud Tasks (the AE stored form has no default deadline to
	// absorb it, unlike HTTP where the same perturbation weighed zero).
	deadlineTask := func(n, bodyLen int) TaskState {
		ts := aeTask(n, bodyLen, nil)
		ts.DispatchDeadline = maybe.Some(900 * time.Second)
		return ts
	}
	_, _, err = e.CreateTask(t.Context(), queueName, deadlineTask(5, sizeProbeAppEngineMaxBody-5))
	assert.NoError(t, err)
	_, _, err = e.CreateTask(t.Context(), queueName, deadlineTask(6, sizeProbeAppEngineMaxBody-4))
	assert.ErrorIs(t, err, ErrTaskTooLarge)

	// A routing service weighed exactly len(service)+1 against real Cloud
	// Tasks ("size-probe-service" cost 19 bytes): the service folds into the
	// counted host with a "." separator. The emulator stores the host as
	// "https://size-probe-service-dot-cloudtasksemu.appspot.com", so this also
	// pins storedRoutingHost's scheme-strip and -dot- translation.
	withService := func(ae *AppEngineHTTPRequest) {
		ae.AppEngineRouting = maybe.Some(AppEngineRouting{Service: maybe.Some("size-probe-service")})
	}
	_, _, err = e.CreateTask(t.Context(), queueName, aeTask(3, sizeProbeAppEngineMaxBody-19, withService))
	assert.NoError(t, err)
	_, _, err = e.CreateTask(t.Context(), queueName, aeTask(4, sizeProbeAppEngineMaxBody-18, withService))
	assert.ErrorIs(t, err, ErrTaskTooLarge)
}
