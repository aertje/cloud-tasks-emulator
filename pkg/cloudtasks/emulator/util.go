package emulator

import (
	"regexp"
)

var reTaskName = regexp.MustCompile("(projects/([a-zA-Z0-9:.-]+)/locations/([a-zA-Z0-9-]+)/queues/([a-zA-Z0-9-]+))/tasks/([a-zA-Z0-9_-]+)")

type TaskNameParts struct {
	taskName  string
	queueName string

	projectID  string
	locationID string
	queueID    string
	taskID     string
}

func parseTaskName(taskName string) TaskNameParts {
	matches := reTaskName.FindStringSubmatch(taskName)
	return TaskNameParts{
		taskName:   matches[0],
		queueName:  matches[1],
		projectID:  matches[2],
		locationID: matches[3],
		queueID:    matches[4],
		taskID:     matches[5],
	}
}

func isValidTaskName(taskName string) bool {
	return reTaskName.MatchString(taskName)
}
