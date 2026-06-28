package engine

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOIDCTokenSetsCorrectData(t *testing.T) {
	config := DefaultOIDCConfig()
	tokenStr := config.CreateToken("foobar@service.com", "http://my.service/foo?bar=v", "")
	parser := new(jwt.Parser)
	token, _, err := parser.ParseUnverified(tokenStr, &OpenIDConnectClaims{})
	require.NoError(t, err)
	assert.Equal(t, "RS256", token.Header["alg"], "Uses RS256")
	assert.Equal(t, config.KeyID, token.Header["kid"], "Specifies kid")

	claims := token.Claims.(*OpenIDConnectClaims)

	assert.Equal(t, "http://my.service/foo?bar=v", claims.Audience, "Specifies audience")
	assert.Equal(t, config.IssuerURL, claims.Issuer, "Specifies issuer")
	assert.Equal(t, "foobar@service.com", claims.Email, "Specifies email")
	assert.Equal(t, "foobar@service.com", claims.Subject, "Specifies subject")
	assert.True(t, claims.EmailVerified, "Specifies email")
	assertRoughTimestamp(t, 0*time.Second, claims.IssuedAt, "Issued now")
	assertRoughTimestamp(t, 0*time.Second, claims.NotBefore, "Not before now")
	assertRoughTimestamp(t, 5*time.Minute, claims.ExpiresAt, "Expires in 5 mins")
}

func TestCreateOIDCTokenWithCustomAudienceSetsCorrectData(t *testing.T) {
	config := DefaultOIDCConfig()
	tokenStr := config.CreateToken("foobar@service.com", "http://my.service/foo?bar=v", "http://my.api")
	parser := new(jwt.Parser)
	token, _, err := parser.ParseUnverified(tokenStr, &OpenIDConnectClaims{})
	require.NoError(t, err)
	assert.Equal(t, "RS256", token.Header["alg"], "Uses RS256")
	assert.Equal(t, config.KeyID, token.Header["kid"], "Specifies kid")

	claims := token.Claims.(*OpenIDConnectClaims)

	assert.Equal(t, "http://my.api", claims.Audience, "Specifies audience")
	assert.Equal(t, config.IssuerURL, claims.Issuer, "Specifies issuer")
	assert.Equal(t, "foobar@service.com", claims.Email, "Specifies email")
	assert.True(t, claims.EmailVerified, "Specifies email")
	assertRoughTimestamp(t, 0*time.Second, claims.IssuedAt, "Issued now")
	assertRoughTimestamp(t, 0*time.Second, claims.NotBefore, "Not before now")
	assertRoughTimestamp(t, 5*time.Minute, claims.ExpiresAt, "Expires in 5 mins")
}

func TestCreateOIDCTokenSignatureIsValidAgainstKey(t *testing.T) {
	// Sanity check that the token is valid if we have the private key in go format
	config := DefaultOIDCConfig()
	tokenStr := config.CreateToken("foobar@service.com", "http://any.service/foo", "")
	_, err := new(jwt.Parser).ParseWithClaims(
		tokenStr,
		&OpenIDConnectClaims{},
		func(token *jwt.Token) (interface{}, error) {
			// Can safely skip kid checking as we check it in the data test above
			assert.IsType(t, jwt.SigningMethodRS256, token.Method)
			return config.PrivateKey.Public(), nil
		},
	)
	require.NoError(t, err)
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
