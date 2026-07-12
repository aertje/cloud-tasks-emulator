// The receiver is a lean, standard-library-only module so the App Engine deploy
// stays free of the conformance harness' heavy Cloud Tasks client graph. The
// conformance module pulls it in via a local `replace` (see conformance/go.mod)
// to reuse the Capture type and run the same handler hermetically in tests.
module github.com/aertje/cloud-tasks-emulator/conformance/receiver

go 1.26.4
