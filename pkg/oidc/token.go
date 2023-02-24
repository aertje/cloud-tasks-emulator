package oidc

import (
	"time"

	"github.com/golang-jwt/jwt"
)

type OIDCTokenCreator struct{}

func NewOIDCTokenCreator() OIDCTokenCreator {
	return OIDCTokenCreator{}
}

func (c OIDCTokenCreator) CreateOIDCToken(email, subject, audience string) (string, error) {
	now := time.Now()
	claims := OpenIDConnectClaims{
		Email:         email,
		EmailVerified: true,
		StandardClaims: jwt.StandardClaims{
			Subject:   subject,
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
