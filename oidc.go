package main

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const jwksUriPath = "/jwks"

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

var OpenIDConfig struct {
	IssuerURL  string
	KeyID      string
	PrivateKey *rsa.PrivateKey
}

type OpenIDConnectClaims struct {
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	jwt.StandardClaims
}

func init() {
	var err error
	OpenIDConfig.PrivateKey, err = jwt.ParseRSAPrivateKeyFromPEM([]byte(openIdPrivateKeyStr))
	if err != nil {
		panic(err)
	}

	OpenIDConfig.IssuerURL = "http://cloud-tasks-emulator"
	OpenIDConfig.KeyID = "cloudtasks-emulator-test"
}

func createOIDCToken(serviceAccountEmail string, handlerUrl string) string {
	now := time.Now()
	claims := OpenIDConnectClaims{
		Email:         serviceAccountEmail,
		EmailVerified: true,
		StandardClaims: jwt.StandardClaims{
			Audience:  handlerUrl,
			Issuer:    OpenIDConfig.IssuerURL,
			IssuedAt:  now.Unix(),
			NotBefore: now.Unix(),
			ExpiresAt: now.Add(5 * time.Minute).Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = OpenIDConfig.KeyID

	tokenString, err := token.SignedString(OpenIDConfig.PrivateKey)

	if err != nil {
		log.Fatalf("Failed to create OIDC token: %v", err)
	}

	return tokenString
}

func openIDConfigHttpHandler(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"issuer":                                OpenIDConfig.IssuerURL,
		"jwks_uri":                              OpenIDConfig.IssuerURL + jwksUriPath,
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"claims_supported":                      []string{"aud", "email", "email_verified", "exp", "iat", "iss", "nbf"},
	}

	respondJSON(w, config, 24*time.Hour)
}

func respondJSON(w http.ResponseWriter, body interface{}, expiresAfter time.Duration) {
	json, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	expires := time.Now().Add(expiresAfter).Format(http.TimeFormat)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public")
	w.Header().Set("Expires", expires)
	w.Write(json)
}

func openIDJWKSHttpHandler(w http.ResponseWriter, r *http.Request) {
	publicKey := OpenIDConfig.PrivateKey.Public().(*rsa.PublicKey)
	b64Url := base64.URLEncoding.WithPadding(base64.NoPadding)

	config := map[string]interface{}{
		"keys": []map[string]string{
			{
				// Ideally we would export the exponent from the key too but frankly
				// it's always AQAB in practice and I lost the will to live trying to
				// base64url encode a 2-bytes int in go!
				"e":   "AQAB",
				"n":   b64Url.EncodeToString(publicKey.N.Bytes()),
				"kid": OpenIDConfig.KeyID,
				"use": "sig",
				"alg": "RSA256",
				"kty": "RSA",
			},
		},
	}

	respondJSON(w, config, 24*time.Hour)
}

func serveOpenIDConfigurationEndpoint(listenAddr string, listenPort string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", openIDConfigHttpHandler)
	mux.HandleFunc(jwksUriPath, openIDJWKSHttpHandler)

	server := &http.Server{Addr: listenAddr + ":" + listenPort, Handler: mux}
	go server.ListenAndServe()

	return server
}

func configureOpenIdIssuer(issuerUrl string) (*http.Server, error) {
	url, err := url.ParseRequestURI(issuerUrl)
	if err != nil {
		return nil, fmt.Errorf("-openid-issuer must be a base URL e.g. http://any-host:8237")
	}

	if url.Scheme != "http" {
		return nil, fmt.Errorf("-openid-issuer only supports http protocol")
	}

	if url.Path != "" {
		return nil, fmt.Errorf("-openid-issuer must not contain a path")
	}

	OpenIDConfig.IssuerURL = issuerUrl

	hostParts := strings.Split(url.Host, ":")
	var port string
	if len(hostParts) > 1 {
		port = hostParts[1]
	} else {
		port = "80"
	}

	listenAddr := "0.0.0.0"
	fmt.Printf("Issuing OpenID tokens as %v - running endpoint on %v:%v\n", issuerUrl, listenAddr, port)
	return serveOpenIDConfigurationEndpoint(listenAddr, port), nil
}
