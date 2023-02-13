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

func toRPCStatusCode(statusCode int) rpccode.Code {
	switch statusCode {
	case 200:
		return rpccode.Code_OK
	case 400:
		// TODO: or rpccode.Code_FAILED_PRECONDITION
		// TODO: or rpcCode.Code_OUT_OF_RANGE
		return rpccode.Code_INVALID_ARGUMENT
	case 401:
		return rpccode.Code_UNAUTHENTICATED
	case 403:
		return rpccode.Code_PERMISSION_DENIED
	case 404:
		return rpccode.Code_NOT_FOUND
	case 409:
		// TODO: or rpccde.Code_ABORTED
		return rpccode.Code_ALREADY_EXISTS
	case 429:
		return rpccode.Code_RESOURCE_EXHAUSTED
	case 499:
		return rpccode.Code_CANCELLED
	case 500:
		//TODO: or rpccode.Code_DATA_LOSS
		return rpccode.Code_INTERNAL
	case 501:
		return rpccode.Code_UNIMPLEMENTED
	case 503:
		return rpccode.Code_UNAVAILABLE
	case 504:
		return rpccode.Code_DEADLINE_EXCEEDED
	default:
		return rpccode.Code_UNKNOWN
	}
}

func toCodeName(rpcCode rpccode.Code) string {
	return rpccode.Code_name[int32(rpcCode)]
}
