package server

import (
	"net/http"

	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/genproto/googleapis/rpc/code"
)

var httpMethodPBLookup = map[string]taskspb.HttpMethod{
	http.MethodGet:     taskspb.HttpMethod_GET,
	http.MethodPost:    taskspb.HttpMethod_POST,
	http.MethodDelete:  taskspb.HttpMethod_DELETE,
	http.MethodHead:    taskspb.HttpMethod_HEAD,
	http.MethodOptions: taskspb.HttpMethod_OPTIONS,
	http.MethodPatch:   taskspb.HttpMethod_PATCH,
	http.MethodPut:     taskspb.HttpMethod_PUT,
}

var httpMethodLookup = map[taskspb.HttpMethod]string{}

var statusCodePBLookup = map[int][]code.Code{
	http.StatusOK:                  {code.Code_OK},
	http.StatusBadRequest:          {code.Code_INVALID_ARGUMENT, code.Code_FAILED_PRECONDITION, code.Code_OUT_OF_RANGE},
	http.StatusUnauthorized:        {code.Code_UNAUTHENTICATED},
	http.StatusForbidden:           {code.Code_PERMISSION_DENIED},
	http.StatusNotFound:            {code.Code_NOT_FOUND},
	http.StatusConflict:            {code.Code_ALREADY_EXISTS, code.Code_ABORTED},
	http.StatusTooManyRequests:     {code.Code_RESOURCE_EXHAUSTED},
	499:                            {code.Code_CANCELLED}, // Does not exist as a http.StatusXx
	http.StatusInternalServerError: {code.Code_INTERNAL, code.Code_DATA_LOSS, code.Code_UNKNOWN},
	http.StatusNotImplemented:      {code.Code_UNIMPLEMENTED},
	http.StatusServiceUnavailable:  {code.Code_UNAVAILABLE},
	http.StatusGatewayTimeout:      {code.Code_DEADLINE_EXCEEDED},
}

var statusCodeLookup = map[code.Code]int{}

func init() {
	for k, v := range httpMethodPBLookup {
		httpMethodLookup[v] = k
	}

	for k, vs := range statusCodePBLookup {
		for _, v := range vs {
			statusCodeLookup[v] = k
		}
	}
}

func toHTTPMethodPB(httpMethod string) taskspb.HttpMethod {
	return httpMethodPBLookup[httpMethod]
}

func toStatusCodePB(httpStatus int) code.Code {
	vs, ok := statusCodePBLookup[httpStatus]
	if !ok {
		return code.Code_UNKNOWN
	}
	return vs[0]
}

func toCodeName(rpcCode code.Code) string {
	return code.Code_name[int32(rpcCode)]
}
