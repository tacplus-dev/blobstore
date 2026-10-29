package blobstore

import (
	"encoding/base64"
	"net/http"
	"testing"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
)

// The upload token is a BEARER TOKEN BY RECORD (R3, R9): verification is hashing
// what arrived in the path and looking up the UploadSession row it keys. These
// four are the whole contract — the row is there and live, the row is not there,
// the row has expired, the row has already been used.
//
// They are the upload path's counterpart to
// TestEveryRefusalOnTheDownloadPathIsTheSame404, which does the same for `/b/`.

func TestUploadTokenResolvesItsSessionAndSucceeds(t *testing.T) {
	p := newPair(t)
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("PUT with a live token: got %d, want 201", got)
	}
	// The row the token resolved to is what named the bytes' resting place, on
	// BOTH pods (R2).
	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if !svc.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
			t.Errorf("pod %s does not hold the blob", name)
		}
	}
}

// A token that hashes to no row is refused, and refused the same way a bad link
// token is: one 404, no explanation. This is the case that would have needed a
// signature check before — now there is simply nothing to look up.
func TestUploadTokenWithNoSessionIsRefused(t *testing.T) {
	p := newPair(t)
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	// A DIFFERENT random token of the right shape: correct length, correct
	// alphabet, and it keys nothing.
	wrong, _, err := MintUploadToken()
	if err != nil {
		t.Fatalf("MintUploadToken: %v", err)
	}
	garbage := base64.RawURLEncoding.EncodeToString([]byte("not a token at all!!"))

	for name, bad := range map[string]string{"another random token": wrong, "garbage": garbage} {
		if got := put(t, p.aBytes, bad, pdfBody); got != http.StatusNotFound {
			t.Errorf("%s: got %d, want 404", name, got)
		}
	}
	// And nothing was written anywhere on the way.
	if p.a.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
		t.Fatal("a refused upload still landed bytes")
	}
	// The real token still works, so the refusals above were about the token and
	// not about the pod.
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("the live token was refused too: got %d, want 201", got)
	}
}

// Expiry is checked when the PUT ARRIVES, from the row (R9 step 1). 404, because
// an expired session is indistinguishable from an unknown one to its holder.
func TestUploadTokenForAnExpiredSessionIsRefused(t *testing.T) {
	p := newPair(t)
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	p.records.sessions[sess.GetUploadId()].ExpiresMs = p.nowMs - 1

	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusNotFound {
		t.Fatalf("PUT with an expired session: got %d, want 404", got)
	}
	if p.a.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
		t.Fatal("an expired upload still landed bytes")
	}
}

// Single use (R9 step 3), and it survives the pod: the second PUT is refused by
// the ROW's state, not by the in-memory ledger, which is what makes the guarantee
// durable rather than best-effort.
func TestUploadTokenIsRefusedAfterItHasBeenConsumed(t *testing.T) {
	p := newPair(t)
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("first PUT: got %d, want 201", got)
	}
	if state := p.records.sessions[sess.GetUploadId()].GetState(); state != blobstore_pb.UploadState_UPLOAD_STATE_COMMITTED {
		t.Fatalf("session settled as %v, want COMMITTED", state)
	}
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusConflict {
		t.Fatalf("replayed PUT: got %d, want 409", got)
	}

	// Now with the in-memory ledger emptied, as a pod restart would leave it: the
	// settled row is what refuses the replay.
	p.a.consumed.Delete(sess.GetUploadId())
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusConflict {
		t.Fatalf("replayed PUT after the in-memory ledger was cleared: got %d, want 409", got)
	}
}

// Single use across pods: the gateway round-robins `PUT /upload/`, so a replay
// can land on the pod that did not take the first PUT. That pod has never seen
// the upload in its in-memory ledger, so it is the settled row alone that refuses.
func TestUploadTokenIsRefusedWhenReplayedOnThePeer(t *testing.T) {
	p := newPair(t)
	token, _ := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("first PUT on pod a: got %d, want 201", got)
	}
	if got := put(t, p.bBytes, token, pdfBody); got != http.StatusConflict {
		t.Fatalf("replayed PUT on pod b: got %d, want 409", got)
	}
}
