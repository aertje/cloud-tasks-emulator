package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTitelize(t *testing.T) {
	headers := make(map[string]string)
	headers["Accept-Encoding"] = "gzip"
	headers["content-type"] = "application/json"

	headers = titelize(headers)
	assert.Contains(t, headers, "Accept-Encoding")
	assert.Contains(t, headers, "Content-Type")
}
