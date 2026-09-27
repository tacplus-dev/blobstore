package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Every test in this file is one line of the decision doc's Tests list, in its
// own words. The harness stands up the real pair; see harness_test.go.

const testNS = "/tenants/acme"

var pdfBody = []byte("%PDF-1.7\nhello blob store\n")

func pdfHash() string {
	sum := sha256.Sum256(pdfBody)
	return hex.EncodeToString(sum[:])
}

// "write refused with one pod down, reads served."
func TestUploadRefusedWhenPeerIsDownAndReadsStillServed(t *testing.T) {
	p := newPair(t)

	// First, a successful upload while both pods are up — the read has to have
	// something to serve.
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("upload with both pods up: got %d, want 201", got)
	}

	// Now pod B stops answering. Its port is still open — this is a pod that is
	// restarting, not a DNS entry that has gone away — so the failure the code has
	// to notice is the health probe, not a dial error.
	p.bPeerRoute.set(nil)

	token2, _ := p.mintUpload(t, p.a, testNS, "application/pdf", []byte("%PDF-1.7\nsecond\n"))
	if got := put(t, p.aBytes, token2, []byte("%PDF-1.7\nsecond\n")); got != http.StatusServiceUnavailable {
		t.Fatalf("upload with the peer down: got %d, want 503", got)
	}

	// And the read still works, from the pod that is up.
	linkToken := p.mintLink(t, claims)
	resp := p.get(t, p.aBytes, linkToken)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read while the peer is down: got %d, want 200", resp.StatusCode)
	}
}

// "hash mismatch cleans both pods."
//
// The bytes sent hash to something other than the declared hash, so the upload
// must fail AND neither pod may be left holding a temp file or a promoted file.
func TestHashMismatchLeavesNothingOnEitherPod(t *testing.T) {
	p := newPair(t)
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	// Same length, different content: the size check passes and the hash check is
	// what has to catch it.
	wrong := append([]byte(nil), pdfBody...)
	wrong[len(wrong)-2] = 'X'
	if got := put(t, p.aBytes, token, wrong); got != http.StatusBadRequest {
		t.Fatalf("hash mismatch: got %d, want 400", got)
	}

	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if svc.store.Has(claims.Group, claims.Namespace, claims.ContentHash) {
			t.Errorf("pod %s promoted a blob whose hash did not match", name)
		}
		assertNoTemps(t, name, svc)
	}
	if _, ok := p.records.blobs[blobIDOf(claims)]; ok {
		t.Error("a Blob record was published for a failed upload")
	}
}

// "magic-byte mismatch refused" — the renamed-executable case the doc names.
func TestMagicByteMismatchIsRefused(t *testing.T) {
	p := newPair(t)
	// An ELF header declaring itself a PDF. The declared type is on the group's
	// allow-list and the hash and size are correct, so magic bytes are the only
	// thing that can refuse it.
	elf := []byte{0x7F, 'E', 'L', 'F', 1, 1, 1, 0, 0, 0}
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", elf)
	if got := put(t, p.aBytes, token, elf); got != http.StatusBadRequest {
		t.Fatalf("magic-byte mismatch: got %d, want 400", got)
	}
	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if svc.store.Has(claims.Group, claims.Namespace, claims.ContentHash) {
			t.Errorf("pod %s promoted a blob that failed the magic-byte check", name)
		}
		assertNoTemps(t, name, svc)
	}
}

// "upload past max_blob_bytes" — refused on the wire, at the limit, even though
// the DECLARED size was inside it.
func TestUploadPastMaxBlobBytesIsRefusedOnTheWire(t *testing.T) {
	p := newPair(t)
	// The token declares 10 bytes; the group's ceiling is 1024; the body is 2048.
	// Nothing but the wire check can catch this.
	small := []byte("%PDF-1.7\n\n")
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", small)
	big := append([]byte("%PDF-1.7\n"), make([]byte, 2048)...)

	if got := put(t, p.aBytes, token, big); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized upload: got %d, want 413", got)
	}
	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if svc.store.Has(claims.Group, claims.Namespace, claims.ContentHash) {
			t.Errorf("pod %s promoted an oversized blob", name)
		}
		assertNoTemps(t, name, svc)
	}
}

// "group unknown refused" — a token is only as good as the group it names.
func TestUploadForAnUnknownGroupIsRefused(t *testing.T) {
	p := newPair(t)
	token, _ := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	// The group disappears from config while the token is live.
	cfg := testConfig()
	cfg.VolumeGroups = nil
	if err := p.a.Configure(cfg); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusServiceUnavailable {
		t.Fatalf("upload naming an unconfigured group: got %d, want 503", got)
	}
}

// "dedup within tenant only" — the same bytes under two namespaces are two files.
func TestDedupIsPerTenant(t *testing.T) {
	p := newPair(t)
	const otherNS = "/tenants/other"

	tokenA, claimsA := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, tokenA, pdfBody); got != http.StatusCreated {
		t.Fatalf("first tenant's upload: got %d, want 201", got)
	}
	tokenB, claimsB := p.mintUpload(t, p.a, otherNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, tokenB, pdfBody); got != http.StatusCreated {
		t.Fatalf("second tenant's upload: got %d, want 201", got)
	}

	if claimsA.ContentHash != claimsB.ContentHash {
		t.Fatal("the two uploads must be the same bytes for this test to mean anything")
	}
	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		pathA := svc.store.Path(testGroup, testNS, claimsA.ContentHash)
		pathB := svc.store.Path(testGroup, otherNS, claimsB.ContentHash)
		if pathA == pathB {
			t.Fatalf("pod %s: two tenants share one path %s", name, pathA)
		}
		for _, path := range []string{pathA, pathB} {
			if _, err := os.Stat(path); err != nil {
				t.Errorf("pod %s: %s is missing: %v", name, path, err)
			}
		}
	}
	if len(p.records.blobs) != 2 {
		t.Errorf("want two Blob rows, one per tenant, got %d", len(p.records.blobs))
	}
}

// "token single-use."
func TestUploadTokenIsSingleUse(t *testing.T) {
	p := newPair(t)
	token, _ := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("first use: got %d, want 201", got)
	}
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusConflict {
		t.Fatalf("second use of the same token: got %d, want 409", got)
	}
}

// "anti-entropy rebuilds an empty PVC before serving."
func TestAntiEntropyFillsAnEmptyPVCFromThePeer(t *testing.T) {
	p := newPair(t)
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("upload: got %d, want 201", got)
	}

	// Pod A's disk is replaced: a fresh, empty PVC at a new path, same peer.
	replaced := p.newPod(t, "pod-a", hostOf(t, p.aPeer.URL), hostOf(t, p.bPeer.URL))
	if replaced.store.Has(claims.Group, claims.Namespace, claims.ContentHash) {
		t.Fatal("a fresh PVC must start empty")
	}

	replaced.SyncOnStart(context.Background())

	if !replaced.store.Has(claims.Group, claims.Namespace, claims.ContentHash) {
		t.Fatal("anti-entropy did not copy the blob the peer holds")
	}
	// And the copy is the real bytes, not an empty file named after a hash.
	got, err := os.ReadFile(replaced.store.Path(claims.Group, claims.Namespace, claims.ContentHash))
	if err != nil {
		t.Fatalf("reading the copied blob: %v", err)
	}
	if string(got) != string(pdfBody) {
		t.Fatalf("copied bytes differ: got %q", got)
	}
}

// Anti-entropy must not trust the name it fetched under: a peer that serves the
// wrong bytes for a hash is refused rather than believed.
func TestAntiEntropyRefusesBytesThatDoNotHashToTheirName(t *testing.T) {
	p := newPair(t)
	key := BlobKey{Group: testGroup, Namespace: testNS, Hash: pdfHash()}
	err := p.a.store.WriteFrom(key, bytesOf("not the pdf at all"))
	if err == nil {
		t.Fatal("WriteFrom accepted bytes that do not hash to the key")
	}
	if p.a.store.Has(key.Group, key.Namespace, key.Hash) {
		t.Fatal("a mis-hashed anti-entropy copy was promoted")
	}
	assertNoTemps(t, "a", p.a)
}

// A promoted file is never overwritten: a second upload of the same content is a
// dedup, and the file that is there stays there.
func TestPromoteIsDedupAndNeverOverwrites(t *testing.T) {
	p := newPair(t)
	token, claims := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("first upload: got %d, want 201", got)
	}
	path := p.a.store.Path(claims.Group, claims.Namespace, claims.ContentHash)
	first, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	token2, _ := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token2, pdfBody); got != http.StatusCreated {
		t.Fatalf("second upload of the same bytes: got %d, want 201", got)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !os.SameFile(first, second) {
		t.Error("the promoted file was replaced rather than deduped")
	}
	assertNoTemps(t, "a", p.a)
	assertNoTemps(t, "b", p.b)
}

// assertNoTemps fails if a pod is still holding a partial upload once it has had
// a moment to clean up.
//
// IT POLLS, and the wait is not papering over a race — it is the protocol. The
// receiving pod aborts the mirror stream and answers the client immediately; the
// PEER discovers the abort when its own body read fails, and its cleanup runs on
// its own goroutine after the client's response has already been sent. So "both
// pods have no temp" is true shortly after the failure rather than at the instant
// of it, and a test that asserted it synchronously would be asserting something
// the design does not promise.
func assertNoTemps(t *testing.T, pod string, svc *Service) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var names []string
	for {
		entries, err := os.ReadDir(filepath.Join(svc.store.Root(), tmpDirName))
		if err != nil {
			t.Fatalf("pod %s: reading tmp: %v", pod, err)
		}
		if len(entries) == 0 {
			return
		}
		names = names[:0]
		for _, e := range entries {
			names = append(names, e.Name())
		}
		if time.Now().After(deadline) {
			t.Errorf("pod %s still holds %d temp file(s) after 3s: %v", pod, len(names), names)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func blobIDOf(c UploadClaims) string {
	return BlobKey{Group: c.Group, Namespace: c.Namespace, Hash: c.ContentHash}.String()
}
