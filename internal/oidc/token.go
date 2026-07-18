package oidc

import (
	"crypto/rsa"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// This private key is, of course, not actually private!
const openIdPrivateKeyStr = `
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+EePjNlISDurX
4F1JvNKK+2afgRYX89kgXuAAf7iKqbu/bYw37bC+eak0tAb/4t4nkzf2QMda3Z6L
ccSzE/FsR54dHMKbbcCBcMZOSO5RReLsY/WdCZZxmfJyQPSOvyRk7vz2lq5yTrUa
+dCG12XiY/ckIJc8jR0m9uSvvqeL6EyeOkHsbIKESCUgCuyFM0/CEeb7ozRzhHe5
W/NBSm4TsIRyKw0fW7wczRo6dApdhzjZrc/jKWWkPvSM23TTxK1fLIgjA3gsVP37
m8z0WsESljiT0QCCBTZHsUSh2eTLp7yCs9XZvTPZ5Eu7iOAhM8zPLKphzotxwQ+y
f31eqQXXAgMBAAECggEAGqcbk7L8UzfwSpFVw49M3txeCaPqWzWAjv9+3dMLJ7ah
cziDXxxfmnYo+hD8oklH6bjFMiznR6CoKNmtQYdcZVitnVt5Fp6PThdoV3X2pULt
jUR/HqRHimqSCt9867919QlmQ5XhpHnQ/5VkXmQ6D0MBVvmS+5S2L86TRumvSPjt
xkcsFryxMwyhHiv3Dx+Vqz0RcSWqBe3AJAEUCDsqXL8OMUOoyDcsD34iRQdV7O5m
sjRzU+od9a5b3dLrY9ufrlkcvrn5SbDZPMfwMXvrH5Y+XpGLHAxsMjqktVBitesV
njHiO57RQePbvtQ8sgxTLFe9sbPT51kI+R2urS7f8QKBgQD8oYxQ4NyjUB5SgQ02
/KA5FLcDlkI6wQK5C2hMEmW7Q2+DRQ70KjoSdLVowkRuAk3MX3RxfRVLTq+Cgkjn
dgW2msjqAqOjpZ6Smw01hjEbMMcVMrwRHjSWwG4vIGMaNQqVpzaAR9Pu48jCHyMX
LnsdGbcD8L1jLcSDuE1ComJFRQKBgQDAmsQMoCBH824Q8PhTj2jH0hra+jZg1Tje
42br87FtHovpfUjVYalCg4oiQWAqapeIbagjgA5eMqzf2JOFbu7VgebYr15v3Nc1
WJzwMmE7fAojopo1fOYQ1HTddbvf3LTJcnwnAggcGq4ENysFcbfRD+ldTm1RLoLO
Ny7yuwHqawKBgQCmZkYE88eAboI6d7RblpR2ZJWTcEJZbs47Ui81hByr9uQZg8Aw
xSuRAnyG7wahqzTRO8J4ChqfismB3gzlIFDtERDrSie835cOG8Dck3H+5ecLqGpF
oC6laURqGBwOpAc/wW7dmfIXdMPEUTwMxdnjtg9dMhGcpQW+eQOys0ClPQKBgCOP
b4r1NYCTTUsLco3a+HmMLTEo6UlPlMRyL9p4j9WZwjNF0mCzO1DwgFx6vYqXS4sA
0/5Z8k0qBgj+L55/MNFyvnBbUJBOsd1DkxY19wXIjQavStF9UezhjQImbp2SXj6j
SJDbKywlMOPOW78Rk+KhkXCMvloywCvavGxMYropAoGBAK7ECAs0AZLlUPkXuYmL
U1GzFKUl3xDgczMSof5nPJCHcUm0fl02883IhEFEBvzqo5fu8pIzKGKpVwrNud7E
/cLTJUkejD5e0h4V5ykcTUs9yDrxopQ54NW0lj7Se00e5MAUH0SRwbjbFdzQ3AYd
FSkhEKj2YXWlriv3hyPIC8Aq
-----END PRIVATE KEY-----
`

// Config holds the key material and issuer identity used to mint OIDC tokens
// during dispatch and to publish the matching discovery/JWKS endpoints. It is
// created per-engine (see DefaultConfig) and threaded through to dispatch.
type Config struct {
	IssuerURL  string
	KeyID      string
	PrivateKey *rsa.PrivateKey
}

type Claims struct {
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

// NewConfig builds a Config that signs OIDC tokens with the RSA private key in
// privateKeyPEM. The public key published at the JWKS endpoint is derived from
// it, so callers supply key material in one place. It returns an error rather
// than panicking so the binary can report a bad -openid-signing-key cleanly.
func NewConfig(privateKeyPEM []byte) (*Config, error) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing RSA private key: %w", err)
	}

	return &Config{
		IssuerURL:  "http://cloud-tasks-emulator",
		KeyID:      "cloudtasks-emulator-test",
		PrivateKey: privateKey,
	}, nil
}

// DefaultConfig builds a Config from the baked-in development key. The key is a
// compile-time constant known to parse, so a failure here is a programmer error.
func DefaultConfig() *Config {
	cfg, err := NewConfig([]byte(openIdPrivateKeyStr))
	if err != nil {
		panic(err)
	}

	return cfg
}

// CreateToken issues an RS256-signed OIDC token for the given service account.
// audience defaults to handlerUrl if not provided. now anchors the token's
// iat/nbf/exp claims and is supplied by the caller (dispatch threads the
// engine's injectable clock) so token timing is testable with a fake clock. It
// returns an error rather than terminating the process so callers (e.g.
// dispatch) can fail a single delivery attempt instead of taking the whole
// emulator down.
func (c Config) CreateToken(now time.Time, serviceAccountEmail string, handlerUrl string, audience string) (string, error) {
	if audience == "" {
		audience = handlerUrl
	}
	claims := Claims{
		Email:         serviceAccountEmail,
		EmailVerified: true,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   serviceAccountEmail,
			Audience:  jwt.ClaimStrings{audience},
			Issuer:    c.IssuerURL,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(5 * time.Minute)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = c.KeyID

	tokenString, err := token.SignedString(c.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("signing OIDC token: %w", err)
	}

	return tokenString, nil
}
