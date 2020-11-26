package main

import (
	"context"
	"encoding/json"
	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreateOIDCTokenSetsCorrectData(t *testing.T) {
	tokenStr := createOIDCToken("foobar@service.com", "http://my.service/foo?bar=v")
	parser := new(jwt.Parser)
	token, _, err := parser.ParseUnverified(tokenStr, &OpenIDConnectClaims{})
	require.NoError(t, err)
	assert.Equal(t, "RS256", token.Header["alg"], "Uses RS256")
	assert.Equal(t, OpenIDConfig.KeyID, token.Header["kid"], "Specifies kid")

	claims := token.Claims.(*OpenIDConnectClaims)

	assert.Equal(t, "http://my.service/foo?bar=v", claims.Audience, "Specifies audience")
	assert.Equal(t, OpenIDConfig.IssuerURL, claims.Issuer, "Specifies issuer")
	assert.Equal(t, "foobar@service.com", claims.Email, "Specifies email")
	assert.True(t, claims.EmailVerified, "Specifies email")
	assertRoughTimestamp(t, 0*time.Second, claims.IssuedAt, "Issued now")
	assertRoughTimestamp(t, 0*time.Second, claims.NotBefore, "Not before now")
	assertRoughTimestamp(t, 5*time.Minute, claims.ExpiresAt, "Expires in 5 mins")
}

func TestCreateOIDCTokenSignatureIsValidAgainstKey(t *testing.T) {
	// Sanity check that the token is valid if we have the private key in go format
	tokenStr := createOIDCToken("foobar@service.com", "http://any.service/foo")
	_, err := new(jwt.Parser).ParseWithClaims(
		tokenStr,
		&OpenIDConnectClaims{},
		func(token *jwt.Token) (interface{}, error) {
			// Can safely skip kid checking as we check it in the data test above
			assert.IsType(t, jwt.SigningMethodRS256, token.Method)
			return OpenIDConfig.PrivateKey.Public(), nil
		},
	)
	require.NoError(t, err)
}

func TestOpenIdConfigHttpHandler(t *testing.T) {
	OpenIDConfig.IssuerURL = "http://foo.bar:8080"

	resp := performRequest("GET", "/.well-known/openid-configuration", openIDConfigHttpHandler)

	assert.Equal(t, http.StatusOK, resp.Code)
	body := parseJSONResponse(t, resp)

	assert.Equal(t, "http://foo.bar:8080", body["issuer"], "Provides issuer")
	assert.Equal(t, "http://foo.bar:8080/jwks", body["jwks_uri"], "Provides jwks")
	assert.ElementsMatch(t, []string{"RS256"}, body["id_token_signing_alg_values_supported"])
}

func TestOpenIdJWKSHttpHandler(t *testing.T) {
	OpenIDConfig.KeyID = "any-key-id"

	resp := performRequest("GET", "/jwks", openIDJWKSHttpHandler)

	assert.Equal(t, http.StatusOK, resp.Code)

	expires, err := time.Parse(http.TimeFormat, resp.HeaderMap.Get("Expires"))
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
          "alg": "RSA256",
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
	_, err = configureOpenIdIssuer("junk")
	assert.Error(t, err, "-openid-issuer must be a base URL e.g. http://any-host:8237")

	_, err = configureOpenIdIssuer("https://foo:8900")
	assert.Error(t, err, "-openid-issuer only supports http protocol")

	_, err = configureOpenIdIssuer("http://foo:8900/deep")
	assert.Error(t, err, "-openid-issuer must not contain a path")
}

func TestConfigureOpenIdIssuerSetsConfigAndRunsServer(t *testing.T) {
	srv, err := configureOpenIdIssuer("http://my-external.route.to.me:8200")
	require.NoError(t, err)
	assert.Equal(t, "http://my-external.route.to.me:8200", OpenIDConfig.IssuerURL)
	assert.Equal(t, "0.0.0.0:8200", srv.Addr)
	srv.Shutdown(context.Background())
}

func TestConfigureOpenIdIssuerSupportsPort80(t *testing.T) {
	srv, err := configureOpenIdIssuer("http://my-external.route.to.me")
	require.NoError(t, err)
	assert.Equal(t, "http://my-external.route.to.me", OpenIDConfig.IssuerURL)
	assert.Equal(t, "0.0.0.0:80", srv.Addr)
	srv.Shutdown(context.Background())
}

func assertRoughTimestamp(t *testing.T, expectOffset time.Duration, timestamp int64, msg string) {
	// Ensures that the timestamp is roughly correct, and that it is *less* than
	// the expected value. So e.g. a timestamp that should be 5 minutes in the
	// future might be slightly under due to the clock ticking since creation,
	// but it should not be over.
	actual := time.Unix(timestamp, 0)
	expect := time.Now().Add(expectOffset)
	assert.WithinDuration(t, expect, actual, 1*time.Second, msg)
	assert.LessOrEqual(t, expect.Unix(), actual.Unix(), msg+"(must be less than expected)")
}

func parseJSONResponse(t *testing.T, resp *httptest.ResponseRecorder) map[string]interface{} {
	assert.Equal(t, "application/json", resp.HeaderMap.Get("Content-Type"))

	var body map[string]interface{}
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
