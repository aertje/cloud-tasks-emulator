package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenIdConfigHttpHandler(t *testing.T) {
	config := DefaultConfig()
	config.IssuerURL = "http://foo.bar:8080"
	s := openIDServer{config: *config}

	resp := performRequest("GET", "/.well-known/openid-configuration", s.configHandler)

	assert.Equal(t, http.StatusOK, resp.Code)
	body := parseJSONResponse(t, resp)

	assert.Equal(t, "http://foo.bar:8080", body["issuer"], "Provides issuer")
	assert.Equal(t, "http://foo.bar:8080/jwks", body["jwks_uri"], "Provides jwks")
	assert.ElementsMatch(t, []string{"RS256"}, body["id_token_signing_alg_values_supported"])
}

func TestOpenIdJWKSHttpHandler(t *testing.T) {
	config := DefaultConfig()
	config.KeyID = "any-key-id"
	s := openIDServer{config: *config}

	resp := performRequest("GET", "/jwks", s.jwksHandler)

	assert.Equal(t, http.StatusOK, resp.Code)

	expires, err := time.Parse(http.TimeFormat, resp.Result().Header.Get("Expires"))
	require.NoError(t, err)
	assertRoughTimestamp(t, 24*time.Hour, expires.Unix(), "Expect future expires")

	// Verifies against the expected public key based on the private key const
	// As this seems the easiest way to assert the JSON
	assert.JSONEq(
		t,
		`
    {
      "keys": [
        {
          "e": "AQAB",
          "n": "vhHj4zZSEg7q1-BdSbzSivtmn4EWF_PZIF7gAH-4iqm7v22MN-2wvnmpNLQG_-LeJ5M39kDHWt2ei3HEsxPxbEeeHRzCm23AgXDGTkjuUUXi7GP1nQmWcZnyckD0jr8kZO789pauck61GvnQhtdl4mP3JCCXPI0dJvbkr76ni-hMnjpB7GyChEglIArshTNPwhHm-6M0c4R3uVvzQUpuE7CEcisNH1u8HM0aOnQKXYc42a3P4yllpD70jNt008StXyyIIwN4LFT9-5vM9FrBEpY4k9EAggU2R7FEodnky6e8grPV2b0z2eRLu4jgITPMzyyqYc6LccEPsn99XqkF1w",
          "kid": "any-key-id",
          "use": "sig",
          "alg": "RS256",
          "kty": "RSA"
        }
      ]
    }
    `,
		resp.Body.String(),
	)
}

func TestConfigureOpenIdIssuerRejectsInvalidUrl(t *testing.T) {
	var err error
	_, _, err = ConfigureIssuer("junk", *DefaultConfig())
	assert.Error(t, err, "-openid-issuer must be a base URL e.g. http://any-host:8237")

	_, _, err = ConfigureIssuer("https://foo:8900", *DefaultConfig())
	assert.Error(t, err, "-openid-issuer only supports http protocol")

	_, _, err = ConfigureIssuer("http://foo:8900/deep", *DefaultConfig())
	assert.Error(t, err, "-openid-issuer must not contain a path")
}

func TestConfigureOpenIdIssuerSetsConfigAndRunsServer(t *testing.T) {
	srv, cfg, err := ConfigureIssuer("http://my-external.route.to.me:8200", *DefaultConfig())
	require.NoError(t, err)
	assert.Equal(t, "http://my-external.route.to.me:8200", cfg.IssuerURL)
	assert.Equal(t, "0.0.0.0:8200", srv.Addr)
	srv.Shutdown(context.Background())
}

func TestConfigureOpenIdIssuerSupportsPort80(t *testing.T) {
	srv, cfg, err := ConfigureIssuer("http://my-external.route.to.me", *DefaultConfig())
	require.NoError(t, err)
	assert.Equal(t, "http://my-external.route.to.me", cfg.IssuerURL)
	assert.Equal(t, "0.0.0.0:80", srv.Addr)
	srv.Shutdown(context.Background())
}

func assertRoughTimestamp(t *testing.T, expectOffset time.Duration, timestamp int64, msg string) {
	actual := time.Unix(timestamp, 0)
	expect := time.Now().Add(expectOffset)
	assert.WithinDuration(t, expect, actual, 1*time.Second, msg)
	assert.LessOrEqual(t, expect.Unix(), actual.Unix(), msg+"(must be less than expected)")
}

func parseJSONResponse(t *testing.T, resp *httptest.ResponseRecorder) map[string]any {
	assert.Equal(t, "application/json", resp.Result().Header.Get("Content-Type"))

	var body map[string]any
	err := json.Unmarshal(resp.Body.Bytes(), &body)
	require.NoError(t, err)
	return body
}

func performRequest(method string, url string, handler func(w http.ResponseWriter, r *http.Request)) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		panic(err)
	}

	resp := httptest.NewRecorder()
	http.HandlerFunc(handler).ServeHTTP(resp, req)

	return resp
}
