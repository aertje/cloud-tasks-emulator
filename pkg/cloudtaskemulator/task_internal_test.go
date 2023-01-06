package cloudtaskemulator

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

func TestSetInitialTaskStateAppEngineNoEmulatorDefaults(t *testing.T) {
	taskState := &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{},
		},
	}
	setInitialTaskState(taskState, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "https://bluebook.appspot.com", taskState.GetAppEngineHttpRequest().GetAppEngineRouting().GetHost())
}

func TestInitialTaskStateAppEngineNoEmulatorTargeted(t *testing.T) {
	taskState := &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
				AppEngineRouting: &taskspb.AppEngineRouting{
					Service:  "worker",
					Version:  "v1",
					Instance: "2",
				},
			},
		},
	}
	setInitialTaskState(taskState, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "https://2-dot-v1-dot-worker-dot-bluebook.appspot.com", taskState.GetAppEngineHttpRequest().GetAppEngineRouting().GetHost())
}

func TestSetInitialTaskStateAppEngineEmulatorDefaults(t *testing.T) {
	defer os.Unsetenv("APP_ENGINE_EMULATOR_HOST")
	os.Setenv("APP_ENGINE_EMULATOR_HOST", "http://localhost:1234")

	taskState := &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{},
		},
	}
	setInitialTaskState(taskState, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "http://localhost:1234", taskState.GetAppEngineHttpRequest().GetAppEngineRouting().GetHost())
}

func TestSetInitialTaskStateAppEngineEmulatorTargeted(t *testing.T) {
	defer os.Unsetenv("APP_ENGINE_EMULATOR_HOST")
	os.Setenv("APP_ENGINE_EMULATOR_HOST", "http://nginx")

	taskState := &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
				AppEngineRouting: &taskspb.AppEngineRouting{
					Service:  "worker",
					Version:  "v1",
					Instance: "2",
				},
			},
		},
	}
	setInitialTaskState(taskState, "projects/bluebook/locations/us-east1/queues/agentq")

	assert.Equal(t, "http://2.v1.worker.nginx", taskState.GetAppEngineHttpRequest().GetAppEngineRouting().GetHost())
}
