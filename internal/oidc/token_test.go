package oidc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOIDCTokenSetsCorrectData(t *testing.T) {
	config := DefaultConfig()
	tokenStr, err := config.CreateToken("foobar@service.com", "http://my.service/foo?bar=v", "")
	require.NoError(t, err)
	parser := new(jwt.Parser)
	token, _, err := parser.ParseUnverified(tokenStr, &Claims{})
	require.NoError(t, err)
	assert.Equal(t, "RS256", token.Header["alg"], "Uses RS256")
	assert.Equal(t, config.KeyID, token.Header["kid"], "Specifies kid")

	claims := token.Claims.(*Claims)

	assert.Equal(t, jwt.ClaimStrings{"http://my.service/foo?bar=v"}, claims.Audience, "Specifies audience")
	assert.Equal(t, config.IssuerURL, claims.Issuer, "Specifies issuer")
	assert.Equal(t, "foobar@service.com", claims.Email, "Specifies email")
	assert.Equal(t, "foobar@service.com", claims.Subject, "Specifies subject")
	assert.True(t, claims.EmailVerified, "Specifies email")
	assertRoughNumericDate(t, 0*time.Second, claims.IssuedAt, "Issued now")
	assertRoughNumericDate(t, 0*time.Second, claims.NotBefore, "Not before now")
	assertRoughNumericDate(t, 5*time.Minute, claims.ExpiresAt, "Expires in 5 mins")
}

func TestCreateOIDCTokenWithCustomAudienceSetsCorrectData(t *testing.T) {
	config := DefaultConfig()
	tokenStr, err := config.CreateToken("foobar@service.com", "http://my.service/foo?bar=v", "http://my.api")
	require.NoError(t, err)
	parser := new(jwt.Parser)
	token, _, err := parser.ParseUnverified(tokenStr, &Claims{})
	require.NoError(t, err)
	assert.Equal(t, "RS256", token.Header["alg"], "Uses RS256")
	assert.Equal(t, config.KeyID, token.Header["kid"], "Specifies kid")

	claims := token.Claims.(*Claims)

	assert.Equal(t, jwt.ClaimStrings{"http://my.api"}, claims.Audience, "Specifies audience")
	assert.Equal(t, config.IssuerURL, claims.Issuer, "Specifies issuer")
	assert.Equal(t, "foobar@service.com", claims.Email, "Specifies email")
	assert.True(t, claims.EmailVerified, "Specifies email")
	assertRoughNumericDate(t, 0*time.Second, claims.IssuedAt, "Issued now")
	assertRoughNumericDate(t, 0*time.Second, claims.NotBefore, "Not before now")
	assertRoughNumericDate(t, 5*time.Minute, claims.ExpiresAt, "Expires in 5 mins")
}

func TestCreateOIDCTokenSignatureIsValidAgainstKey(t *testing.T) {
	// Sanity check that the token is valid if we have the private key in go format
	config := DefaultConfig()
	tokenStr, err := config.CreateToken("foobar@service.com", "http://any.service/foo", "")
	require.NoError(t, err)
	_, err = new(jwt.Parser).ParseWithClaims(
		tokenStr,
		&Claims{},
		func(token *jwt.Token) (any, error) {
			// Can safely skip kid checking as we check it in the data test above
			assert.IsType(t, jwt.SigningMethodRS256, token.Method)
			return config.PrivateKey.Public(), nil
		},
	)
	require.NoError(t, err)
}

func TestNewConfigSignsWithSuppliedKey(t *testing.T) {
	// A user-supplied key must be the one that actually signs tokens, and the
	// signature must verify against that key's public half (the same half the
	// JWKS endpoint derives and publishes).
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	config, err := NewConfig(keyPEM)
	require.NoError(t, err)
	assert.True(t, key.Equal(config.PrivateKey), "uses the supplied key, not the baked-in one")

	tokenStr, err := config.CreateToken("foobar@service.com", "http://any.service/foo", "")
	require.NoError(t, err)
	_, err = new(jwt.Parser).ParseWithClaims(
		tokenStr,
		&Claims{},
		func(token *jwt.Token) (any, error) {
			assert.IsType(t, jwt.SigningMethodRS256, token.Method)
			return config.PrivateKey.Public(), nil
		},
	)
	require.NoError(t, err, "token verifies against the supplied key")
}

func TestNewConfigRejectsInvalidPEM(t *testing.T) {
	_, err := NewConfig([]byte("not a pem key"))
	require.Error(t, err)
}

func assertRoughNumericDate(t *testing.T, expectOffset time.Duration, timestamp *jwt.NumericDate, msg string) {
	// Ensures that the timestamp is roughly correct, and that it is *less* than
	// the expected value. So e.g. a timestamp that should be 5 minutes in the
	// future might be slightly under due to the clock ticking since creation,
	// but it should not be over.
	actual := timestamp.Time
	expect := time.Now().Add(expectOffset)
	assert.WithinDuration(t, expect, actual, 1*time.Second, msg)
	assert.LessOrEqual(t, expect.Unix(), actual.Unix(), msg+"(must be less than expected)")
}
