package blobstore

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// uploadTokenVersion prefixes every upload token. It is not decoration: it is
// what lets the envelope's field set change later without a pod having to guess
// whether a token it cannot parse is malformed or merely newer.
const uploadTokenVersion = "v1"

// linkTokenBytes is the raw entropy behind a `/b/` token — 256 bits, the floor
// the decision doc sets (R7).
const linkTokenBytes = 32

// uploadIDBytes is the entropy behind an upload id. It is a record key, not a
// credential: the token that carries it is the credential. 128 bits is plenty to
// make a collision impossible while keeping the id short enough to read in a log.
const uploadIDBytes = 16

// ErrBadToken is returned for every rejected upload token — bad signature,
// malformed envelope, wrong version. The callers turn all of them into one
// answer (404), so an attacker learns nothing about WHY a token failed.
var ErrBadToken = errors.New("blobstore: invalid upload token")

// UploadClaims is what an upload token carries, and it is the whole reason the
// PUT path needs no stratum read: the terms of the upload travel with the
// credential, signed.
//
// Binding the content hash in is what makes a replayed token harmless. A token
// authorizes ONE set of bytes into ONE place; using it twice writes the same
// content-addressed file twice, which is a dedup. It cannot be turned into
// permission to upload something else.
type UploadClaims struct {
	UploadID    string `json:"u"`
	Group       string `json:"g"`
	Namespace   string `json:"n"`
	ContentHash string `json:"h"`
	SizeBytes   int64  `json:"s"`
	ContentType string `json:"c"`
	ExpiresMs   int64  `json:"e"`
	// OwnerPrincipal is the service that asked for the upload — carried so the
	// published Blob names the right owner without the PUT having to read the
	// UploadSession row.
	OwnerPrincipal string `json:"o"`
}

// TokenSigner mints and verifies upload tokens, and mints link tokens.
//
// ONE KEY, TWO UNRELATED JOBS, and only one of them uses it: the upload token is
// an HMAC envelope; a link token is raw randomness whose hash is a record key
// (see Config.token_hmac_key). Both live here because both are "the credential
// in a URL path", and keeping them in one type is what stops a future change
// signing link tokens by accident.
type TokenSigner struct {
	key []byte
}

// NewTokenSigner takes the raw key bytes, already base64-decoded from config.
func NewTokenSigner(key []byte) (*TokenSigner, error) {
	if len(key) < 32 {
		return nil, fmt.Errorf("blobstore: token_hmac_key is %d bytes, need at least 32", len(key))
	}
	return &TokenSigner{key: key}, nil
}

// MintUploadToken returns `v1.<payload>.<sig>`, base64url throughout.
func (s *TokenSigner) MintUploadToken(c UploadClaims) (string, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encoding upload claims: %w", err)
	}
	body := uploadTokenVersion + "." + base64.RawURLEncoding.EncodeToString(payload)
	return body + "." + base64.RawURLEncoding.EncodeToString(s.sign([]byte(body))), nil
}

// VerifyUploadToken checks the signature and returns the claims. It does NOT
// check expiry: the caller does, because what "expired" means differs between
// the public PUT (the token must be live when the request arrives) and the peer
// hop (which is only reached from a request that already passed that check).
func (s *TokenSigner) VerifyUploadToken(token string) (UploadClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != uploadTokenVersion {
		return UploadClaims{}, ErrBadToken
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return UploadClaims{}, ErrBadToken
	}
	body := parts[0] + "." + parts[1]
	// Constant time: a byte-at-a-time comparison here would leak the expected
	// signature one byte per request to a caller willing to measure.
	if !hmac.Equal(sig, s.sign([]byte(body))) {
		return UploadClaims{}, ErrBadToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return UploadClaims{}, ErrBadToken
	}
	var c UploadClaims
	if err := json.Unmarshal(payload, &c); err != nil {
		return UploadClaims{}, ErrBadToken
	}
	// The envelope is authentic; it still has to describe something this service
	// can act on. A signed token naming "../.." is the case that matters.
	if err := validateBlobKey(c.Group, c.Namespace, c.ContentHash); err != nil {
		return UploadClaims{}, ErrBadToken
	}
	if c.UploadID == "" || c.SizeBytes <= 0 || c.ContentType == "" {
		return UploadClaims{}, ErrBadToken
	}
	return c, nil
}

func (s *TokenSigner) sign(body []byte) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write(body)
	return mac.Sum(nil)
}

// MintLinkToken returns a raw bearer token and its hash. The token is returned
// to the caller once and stored NOWHERE (R7); the hash is the BlobLink key.
func MintLinkToken() (token string, tokenHash string, err error) {
	b := make([]byte, linkTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generating link token: %w", err)
	}
	token = base64.RawURLEncoding.EncodeToString(b)
	return token, sha256Hex([]byte(token)), nil
}

// LinkTokenHash is how a resolve turns the token in the path into a lookup key.
// It hashes the token STRING rather than the decoded bytes, so mint and resolve
// agree without either having to re-decode.
func LinkTokenHash(token string) string { return sha256Hex([]byte(token)) }

// NewUploadID returns a fresh upload id.
func NewUploadID() (string, error) {
	b := make([]byte, uploadIDBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating upload id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
