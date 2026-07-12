package engine

import (
	"testing"

	"github.com/aertje/cloud-tasks-emulator/v2/internal/maybe"
	"github.com/stretchr/testify/assert"
)

func TestSetInitialTaskStateAppEngineNoEmulatorDefaults(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: maybe.Some(AppEngineHTTPRequest{}),
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq", "")

	assert.Equal(t, "https://bluebook.appspot.com", state.AppEngineHTTPRequest.OrZero().AppEngineRouting.OrZero().Host)
}

func TestInitialTaskStateAppEngineNoEmulatorTargeted(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: maybe.Some(AppEngineHTTPRequest{
			AppEngineRouting: maybe.Some(AppEngineRouting{
				Service:  "worker",
				Version:  "v1",
				Instance: "2",
			}),
		}),
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq", "")

	assert.Equal(t, "https://2-dot-v1-dot-worker-dot-bluebook.appspot.com", state.AppEngineHTTPRequest.OrZero().AppEngineRouting.OrZero().Host)
}

func TestSetInitialTaskStateAppEngineEmulatorDefaults(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: maybe.Some(AppEngineHTTPRequest{}),
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq", "http://localhost:1234")

	assert.Equal(t, "http://localhost:1234", state.AppEngineHTTPRequest.OrZero().AppEngineRouting.OrZero().Host)
}

func TestSetInitialTaskStateAppEngineEmulatorTargeted(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: maybe.Some(AppEngineHTTPRequest{
			AppEngineRouting: maybe.Some(AppEngineRouting{
				Service:  "worker",
				Version:  "v1",
				Instance: "2",
			}),
		}),
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq", "http://nginx")

	assert.Equal(t, "http://2.v1.worker.nginx", state.AppEngineHTTPRequest.OrZero().AppEngineRouting.OrZero().Host)
}
