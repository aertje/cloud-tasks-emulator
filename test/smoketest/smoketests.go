package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Duplicated from app source to avoid having to import the project code
type OpenIDConnectClaims struct {
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

func runTaskHttpServer(listenAddr string) <-chan *http.Request {
	receivedRequests := make(chan *http.Request)

	http.HandleFunc("/handler-test", func(w http.ResponseWriter, req *http.Request) {
		// Whatever happens, return 200 to clear the task from the emulator
		_, _ = w.Write([]byte("OK"))

		requestDump, err := httputil.DumpRequest(req, true)
		fatalIfError(err)
		log.Printf("Received HTTP request:\n%s", requestDump)

		receivedRequests <- req
	})

	log.Println("Starting test server on " + listenAddr)

	socket, err := net.Listen("tcp", listenAddr)
	fatalIfError(err)

	go func() {
		if err := http.Serve(socket, nil); err != nil {
			log.Printf("test HTTP server stopped: %v", err)
		}
	}()

	return receivedRequests
}

func purgeQueue(client *cloudtasks.Client, queuePath string) {
	log.Printf("Purging queue %s", queuePath)
	purgeRequest := &taskspb.PurgeQueueRequest{
		Name: queuePath,
	}

	_, err := client.PurgeQueue(context.Background(), purgeRequest)
	fatalIfError(err)
}

func createTasksClient(emulatorAddress string) *cloudtasks.Client {
	log.Printf("Building connection for emulator %s", emulatorAddress)
	conn, err := grpc.NewClient(emulatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	fatalIfError(err)

	clientOpt := option.WithGRPCConn(conn)
	client, err := cloudtasks.NewClient(context.Background(), clientOpt)
	fatalIfError(err)

	return client
}

func getQueue(client *cloudtasks.Client, queuePath string) {
	log.Printf("Checking queue exists: %s", queuePath)
	getQueueRequest := &taskspb.GetQueueRequest{
		Name: queuePath,
	}

	_, err := client.GetQueue(context.Background(), getQueueRequest)
	fatalIfError(err)

	log.Printf("Confirmed queue exists: %s", queuePath)
}

func createTask(client *cloudtasks.Client, queuePath string, httpHandlerUrl string) string {
	log.Printf("Queuing task:\n -> Queue:      %s\n -> Target URL: %s", queuePath, httpHandlerUrl)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: queuePath,
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: httpHandlerUrl,
					Headers: map[string]string{
						"X-My-Header": "isThis",
					},
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: "emulator@service.test",
						},
					},
					Body: []byte("Here is a body for you"),
				},
			},
		},
	}

	createdTaskResp, err := client.CreateTask(context.Background(), &createTaskRequest)
	fatalIfError(err)

	log.Printf("Created task: %s\n", createdTaskResp.GetName())
	return createdTaskResp.GetName()
}

func listTasks(client *cloudtasks.Client, queuePath string) map[string]string {
	listTasksRequest := &taskspb.ListTasksRequest{
		Parent: queuePath,
	}

	taskIterator := client.ListTasks(context.Background(), listTasksRequest)

	result := make(map[string]string)

	for {
		task, err := taskIterator.Next()
		if err == iterator.Done {
			break
		}
		fatalIfError(err)
		result[task.GetName()] = task.String()
	}
	return result
}

func waitForRequestOrTimeout(channel <-chan *http.Request, timeout time.Duration) (*http.Request, error) {
	select {
	case result := <-channel:
		return result, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timed out after %v waiting for task delivery", timeout)
	}
}

func assertEqual(expect string, actual string) {
	if expect != actual {
		log.Fatalf("Failed asserting %s = %s", expect, actual)
	}
}

func fatalIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func readRequestBody(req *http.Request) string {
	body, err := io.ReadAll(req.Body)
	fatalIfError(err)
	return string(body)
}

func getUnverifiedIssuerFromJWT(tokenStr string) string {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenStr, &jwt.RegisteredClaims{})
	fatalIfError(err)
	claims := token.Claims.(*jwt.RegisteredClaims)
	return claims.Issuer
}

func parseOpenIDConnectToken(tokenStr string, keySet jwk.Set) (*jwt.Token, *OpenIDConnectClaims) {

	token, err := new(jwt.Parser).ParseWithClaims(
		tokenStr,
		&OpenIDConnectClaims{},
		func(token *jwt.Token) (any, error) {
			keyId := token.Header["kid"].(string)
			jwkKey, ok := keySet.LookupKeyID(keyId)
			if !ok {
				return nil, fmt.Errorf("no key found for kid %q", keyId)
			}

			var key rsa.PublicKey
			err := jwkKey.Raw(&key)

			return &key, err
		},
	)

	fatalIfError(err)
	return token, token.Claims.(*OpenIDConnectClaims)
}

func fetchJsonFromUrl(url string) map[string]any {
	client := http.Client{
		Timeout: time.Second * 10,
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	fatalIfError(err)

	res, err := client.Do(req)
	fatalIfError(err)

	body, err := io.ReadAll(res.Body)
	fatalIfError(err)

	var parsedBody map[string]any
	err = json.Unmarshal(body, &parsedBody)
	fatalIfError(err)

	return parsedBody
}

// stringSliceFlag collects repeated occurrences of a string flag into a slice
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	emulatorHost := flag.String("emulator-host", "cloud-tasks-emulator", "The hostname for the emulator")
	emulatorPort := flag.String("emulator-port", "8123", "The port for the emulator")
	httpHandlerHost := flag.String("http-handler-host", "ct-smoketests", "The hostname we can be reached on")
	httpHandlerPort := flag.String("http-handler-port", "8920", "The port our HTTP handler can be reached on")
	queuePath := flag.String("queue-path", "projects/test-project/locations/us-central1/queues/test", "Queue to use (must exist)")

	var expectQueues stringSliceFlag
	flag.Var(&expectQueues, "expect-queue", "Queue path expected to already exist (may be repeated)")

	flag.Parse()

	handlerUrl := fmt.Sprintf("http://%s:%s/handler-test?param=foo", *httpHandlerHost, *httpHandlerPort)
	taskDeliveries := runTaskHttpServer(fmt.Sprintf("0.0.0.0:%s", *httpHandlerPort))

	client := createTasksClient(fmt.Sprintf("%s:%s", *emulatorHost, *emulatorPort))

	for _, expectQueue := range expectQueues {
		getQueue(client, expectQueue)
	}

	// Purge first so repeated local runs against a reused emulator start clean.
	purgeQueue(client, *queuePath)

	createTask(client, *queuePath, handlerUrl)

	request, err := waitForRequestOrTimeout(taskDeliveries, 10*time.Second)
	fatalIfError(err)
	deliveredAt := time.Now()

	assertEqual("POST", request.Method)
	assertEqual("Here is a body for you", readRequestBody(request))
	assertEqual("isThis", request.Header.Get("X-My-Header"))
	assertEqual("/handler-test?param=foo", request.URL.String())
	log.Println("HTTP request matched expectations")

	log.Println("Verifying OIDC Authentication and discovery")
	authHeader := request.Header.Get("Authorization")
	tokenStr := strings.Replace(authHeader, "Bearer ", "", 1)

	// So far, so good. Now check token can be verified using http discovery
	// Note - this is *never* how you would usually do this, you should *always*
	// start from a whitelist of issuers and pre-load their certs to your app.
	issuer := getUnverifiedIssuerFromJWT(tokenStr)
	log.Printf("Got token issued by %v", issuer)
	discovery := fetchJsonFromUrl(issuer + "/.well-known/openid-configuration")

	jwks_uri := discovery["jwks_uri"].(string)
	keySet, err := jwk.Fetch(context.Background(), jwks_uri)
	fatalIfError(err)
	log.Printf("Retrieved issuer keys from %v", jwks_uri)

	// Basic validation, values are fully validated in oidc_internal_test
	token, claims := parseOpenIDConnectToken(tokenStr, keySet)
	if !token.Valid {
		log.Fatal("Auth token was not valid")
	}
	assertEqual("emulator@service.test", claims.Email)
	log.Printf("Validated auth token from %v for %v", claims.Audience, claims.Issuer)

	log.Println("Verifying dispatched tasks are removed from the list")
	queuedTasks := listTasks(client, *queuePath)
	if len(queuedTasks) > 0 {
		log.Fatalf("Unexpectedly got %v tasks remaining:\n%v", len(queuedTasks), queuedTasks)
	}

	log.Println("Waiting to verify no duplicate deliveries")
	// The other checks above already used up some of the 5 second window since delivery,
	// so only wait out whatever is left of it here rather than waiting the full duration again
	remainingWait := 5*time.Second - time.Since(deliveredAt)
	if remainingWait <= 0 {
		remainingWait = time.Millisecond
	}
	request, _ = waitForRequestOrTimeout(taskDeliveries, remainingWait)
	if request != nil {
		// The request is logged on receipt, so it's not necessary to log it again here
		log.Fatal("Got unexpected extra HTTP delivery")
	}

	log.Println("Test complete")
}
