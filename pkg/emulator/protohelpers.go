package emulator

import (
	"net/http"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
)

func toHTTPMethod(taskMethod taskspb.HttpMethod) string {
	switch taskMethod {
	case taskspb.HttpMethod_GET:
		return http.MethodGet
	case taskspb.HttpMethod_POST:
		return http.MethodPost
	case taskspb.HttpMethod_DELETE:
		return http.MethodDelete
	case taskspb.HttpMethod_HEAD:
		return http.MethodHead
	case taskspb.HttpMethod_OPTIONS:
		return http.MethodOptions
	case taskspb.HttpMethod_PATCH:
		return http.MethodPatch
	case taskspb.HttpMethod_PUT:
		return http.MethodPut
	default:
		panic("Unsupported http method")
	}
}

func toRPCStatusCode(statusCode int) int32 {
	switch statusCode {
	case 200:
		return int32(rpccode.Code_OK)
	case 400:
		// TODO: or rpccode.Code_FAILED_PRECONDITION
		// TODO: or rpcCode.Code_OUT_OF_RANGE
		return int32(rpccode.Code_INVALID_ARGUMENT)
	case 401:
		return int32(rpccode.Code_UNAUTHENTICATED)
	case 403:
		return int32(rpccode.Code_PERMISSION_DENIED)
	case 404:
		return int32(rpccode.Code_NOT_FOUND)
	case 409:
		// TODO: or rpccde.Code_ABORTED
		return int32(rpccode.Code_ALREADY_EXISTS)
	case 429:
		return int32(rpccode.Code_RESOURCE_EXHAUSTED)
	case 499:
		return int32(rpccode.Code_CANCELLED)
	case 500:
		//TODO: or rpccode.Code_DATA_LOSS
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
