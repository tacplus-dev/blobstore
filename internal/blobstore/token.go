package blobstore

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// bearerTokenBytes is the raw entropy behind BOTH tokens this service mints — a
// `/b/` link token and a `PUT /upload/` token. 256 bits, the floor the decision
// doc sets (R3, R7).
const bearerTokenBytes = 32

// mintBearerToken returns a raw token and the hash that keys the row granting it.
//
// ONE FUNCTION FOR BOTH TOKENS, deliberately: an upload token and a link token
// are the same construction — randomness in a URL path, sha256 in a record key —
// and the two wrappers below differ only in which record the hash keys. Nothing
// here signs anything; there is no key to sign with (R3).
func mintBearerToken() (token string, tokenHash string, err error) {
	b := make([]byte, bearerTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generating bearer token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(b)
	return token, sha256Hex([]byte(token)), nil
}

// MintUploadToken returns a raw upload token and its hash. The token is returned
// to the caller once and stored NOWHERE; the hash is the UploadSession key, and
// the row it keys carries the upload's terms (R9 step 1).
func MintUploadToken() (token string, tokenHash string, err error) { return mintBearerToken() }

// UploadTokenHash is how a `PUT /upload/<token>` turns the token in the path into
// the UploadSession key. It hashes the token STRING rather than the decoded
// bytes, so mint and verify agree without either having to re-decode.
func UploadTokenHash(token string) string { return sha256Hex([]byte(token)) }

// MintLinkToken returns a raw bearer token and its hash. The token is returned
// to the caller once and stored NOWHERE (R7); the hash is the BlobLink key.
func MintLinkToken() (token string, tokenHash string, err error) { return mintBearerToken() }

// LinkTokenHash is how a resolve turns the token in the path into a lookup key.
// It hashes the token STRING rather than the decoded bytes, so mint and resolve
// agree without either having to re-decode.
func LinkTokenHash(token string) string { return sha256Hex([]byte(token)) }
