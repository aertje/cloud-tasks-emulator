package conformance

// KnownDivergences are cases where the emulator is currently expected to differ
// from real Cloud Tasks for reasons beyond error-message mapping - genuine
// behaviour gaps we have chosen to defer. The validation test reports these as
// KNOWN rather than failing on them, and flags any that have started matching
// (so the entry can be removed once the gap is closed).
//
// Keep this list short and each entry justified; it is the explicit ledger of
// "the emulator is not faithful here, on purpose, for now".
var KnownDivergences = map[string]string{
	"task/create/invalid-name": "engine does not validate the task ID format; real rejects InvalidArgument with a Help detail",
	"task/get/recently-deleted": "engine does not tombstone a singly-deleted task, so GetTask returns it instead of NotFound (purge tombstoning works, delete does not)",
	"queue/create/invalid-parent": "real resolves any parent string to a project and returns PermissionDenied via IAM; the emulator has no project/IAM concept and returns InvalidArgument. Not reproducible by design.",
}
