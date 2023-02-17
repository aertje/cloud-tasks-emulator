package emulator

import (
	"net/http"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/genproto/googleapis/rpc/code"
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

func toRPCStatusCode(statusCode int) code.Code {
	switch statusCode {
	case 200:
		return code.Code_OK
	case 400:
		// TODO: or rpccode.Code_FAILED_PRECONDITION
		// TODO: or rpcCode.Code_OUT_OF_RANGE
		return code.Code_INVALID_ARGUMENT
	case 401:
		return code.Code_UNAUTHENTICATED
	case 403:
		return code.Code_PERMISSION_DENIED
	case 404:
		return code.Code_NOT_FOUND
	case 409:
		// TODO: or rpccde.Code_ABORTED
		return code.Code_ALREADY_EXISTS
	case 429:
		return code.Code_RESOURCE_EXHAUSTED
	case 499:
		return code.Code_CANCELLED
	case 500:
		//TODO: or rpccode.Code_DATA_LOSS
		return code.Code_INTERNAL
	case 501:
		return code.Code_UNIMPLEMENTED
	case 503:
		return code.Code_UNAVAILABLE
	case 504:
		return code.Code_DEADLINE_EXCEEDED
	default:
		return code.Code_UNKNOWN
	}
}

func toCodeName(rpcCode code.Code) string {
	return code.Code_name[int32(rpcCode)]
}
