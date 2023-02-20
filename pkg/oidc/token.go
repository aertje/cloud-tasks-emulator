package oidc

import (
	"time"

	"github.com/golang-jwt/jwt"
)

func CreateOIDCToken(serviceAccountEmail string, handlerUrl string, audience string) (string, error) {
	if audience == "" {
		audience = handlerUrl
	}
	now := time.Now()
	claims := OpenIDConnectClaims{
		Email:         serviceAccountEmail,
		EmailVerified: true,
		StandardClaims: jwt.StandardClaims{
			Subject:   serviceAccountEmail,
			Audience:  audience,
			Issuer:    OpenIDConfig.IssuerURL,
			IssuedAt:  now.Unix(),
			NotBefore: now.Unix(),
			ExpiresAt: now.Add(5 * time.Minute).Unix(),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = OpenIDConfig.KeyID

	return token.SignedString(OpenIDConfig.PrivateKey)
}
