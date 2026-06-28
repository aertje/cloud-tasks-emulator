package engine

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetInitialTaskStateAppEngineNoEmulatorDefaults(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: &AppEngineHTTPRequest{},
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "https://bluebook.appspot.com", state.AppEngineHTTPRequest.AppEngineRouting.Host)
}

func TestInitialTaskStateAppEngineNoEmulatorTargeted(t *testing.T) {
	state := TaskState{
		AppEngineHTTPRequest: &AppEngineHTTPRequest{
			AppEngineRouting: &AppEngineRouting{
				Service:  "worker",
				Version:  "v1",
				Instance: "2",
			},
		},
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "https://2-dot-v1-dot-worker-dot-bluebook.appspot.com", state.AppEngineHTTPRequest.AppEngineRouting.Host)
}

func TestSetInitialTaskStateAppEngineEmulatorDefaults(t *testing.T) {
	defer os.Unsetenv("APP_ENGINE_EMULATOR_HOST")
	os.Setenv("APP_ENGINE_EMULATOR_HOST", "http://localhost:1234")

	state := TaskState{
		AppEngineHTTPRequest: &AppEngineHTTPRequest{},
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "http://localhost:1234", state.AppEngineHTTPRequest.AppEngineRouting.Host)
}

func TestSetInitialTaskStateAppEngineEmulatorTargeted(t *testing.T) {
	defer os.Unsetenv("APP_ENGINE_EMULATOR_HOST")
	os.Setenv("APP_ENGINE_EMULATOR_HOST", "http://nginx")

	state := TaskState{
		AppEngineHTTPRequest: &AppEngineHTTPRequest{
			AppEngineRouting: &AppEngineRouting{
				Service:  "worker",
				Version:  "v1",
				Instance: "2",
			},
		},
	}
	setInitialTaskState(&state, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "http://2.v1.worker.nginx", state.AppEngineHTTPRequest.AppEngineRouting.Host)
}
