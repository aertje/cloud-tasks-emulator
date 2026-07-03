package engine

import (
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
)

// toRPCStatusCode maps an HTTP status code returned by a target to the
// google.rpc.Code used in Attempt.ResponseStatus. Only the codes that
// Cloud Tasks documents are mapped; everything else falls back to UNKNOWN.
func toRPCStatusCode(statusCode int) int32 {
	switch statusCode {
	case 200:
		return int32(rpccode.Code_OK)
	case 400:
		return int32(rpccode.Code_INVALID_ARGUMENT)
	case 401:
		return int32(rpccode.Code_UNAUTHENTICATED)
	case 403:
		return int32(rpccode.Code_PERMISSION_DENIED)
	case 404:
		return int32(rpccode.Code_NOT_FOUND)
	case 409:
		return int32(rpccode.Code_ALREADY_EXISTS)
	case 429:
		return int32(rpccode.Code_RESOURCE_EXHAUSTED)
	case 499:
		return int32(rpccode.Code_CANCELLED)
	case 500:
		return int32(rpccode.Code_INTERNAL)
	case 501:
		return int32(rpccode.Code_UNIMPLEMENTED)
	case 503:
		return int32(rpccode.Code_UNAVAILABLE)
	case 504:
		return int32(rpccode.Code_DEADLINE_EXCEEDED)
	default:
		return int32(rpccode.Code_UNKNOWN)
	}
}

func toCodeName(rpcCode int32) string {
	return rpccode.Code_name[rpcCode]
}
