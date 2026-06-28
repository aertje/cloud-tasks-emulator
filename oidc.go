package main

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aertje/cloud-tasks-emulator/engine"
)

const jwksUriPath = "/jwks"

func openIDConfigHttpHandler(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"issuer":                                engine.OpenIDConfig.IssuerURL,
		"jwks_uri":                              engine.OpenIDConfig.IssuerURL + jwksUriPath,
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"claims_supported":                      []string{"aud", "email", "email_verified", "exp", "iat", "iss", "nbf"},
	}

	respondJSON(w, config, 24*time.Hour)
}

func respondJSON(w http.ResponseWriter, body interface{}, expiresAfter time.Duration) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	utc, _ := time.LoadLocation("UTC")
	expires := time.Now().In(utc).Add(expiresAfter).Format(http.TimeFormat)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public")
	w.Header().Set("Expires", expires)
	w.Write(jsonBody)
}

func openIDJWKSHttpHandler(w http.ResponseWriter, r *http.Request) {
	publicKey := engine.OpenIDConfig.PrivateKey.Public().(*rsa.PublicKey)
	b64Url := base64.URLEncoding.WithPadding(base64.NoPadding)

	config := map[string]interface{}{
		"keys": []map[string]string{
			{
				// Ideally we would export the exponent from the key too but frankly
				// it's always AQAB in practice and I lost the will to live trying to
				// base64url encode a 2-bytes int in go!
				"e":   "AQAB",
				"n":   b64Url.EncodeToString(publicKey.N.Bytes()),
				"kid": engine.OpenIDConfig.KeyID,
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

	engine.OpenIDConfig.IssuerURL = issuerUrl

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
