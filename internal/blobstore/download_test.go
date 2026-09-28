package blobstore

import (
	"encoding/base64"
	"io"
	"net/http"
	"testing"
)

// The `/b/` path's rules, one test each. Every line below is a [review] item the
// decision doc carries into v3 (R3, R7, R8).

func TestDownloadServesTheBytesWithTheRequiredHeaders(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "quarterly report.pdf")

	resp := p.get(t, p.aBytes, token)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if string(body) != string(pdfBody) {
		t.Fatalf("body differs: got %q", body)
	}

	// The filename comes from the STORED link and is always an attachment (R8).
	wantCD := `attachment; filename="quarterly report.pdf"; filename*=UTF-8''quarterly%20report.pdf`
	if got := resp.Header.Get("Content-Disposition"); got != wantCD {
		t.Errorf("Content-Disposition:\n got %q\nwant %q", got, wantCD)
	}
	for header, want := range map[string]string{
		"Content-Type":           "application/pdf",
		"Cache-Control":          "no-store, private",
		"Referrer-Policy":        "no-referrer",
		"X-Content-Type-Options": "nosniff",
		"Accept-Ranges":          "bytes",
	} {
		if got := resp.Header.Get(header); got != want {
			t.Errorf("%s: got %q, want %q", header, got, want)
		}
	}
}

// "unknown and revoked indistinguishable 404s" — and expired, and deleted, and a
// blob this pod does not hold. One answer for all of them.
func TestEveryRefusalOnTheDownloadPathIsTheSame404(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)

	unknown := p.mintLinkNamed(t, sess, "x.pdf")
	delete(p.records.links, LinkTokenHash(unknown)) // never existed

	revoked := p.mintLinkNamed(t, sess, "x.pdf")
	p.records.links[LinkTokenHash(revoked)].RevokedMs = p.nowMs - 1

	expired := p.mintLinkNamed(t, sess, "x.pdf")
	p.records.links[LinkTokenHash(expired)].ExpiresMs = p.nowMs - 1

	deletedBlob := p.mintLinkNamed(t, sess, "x.pdf")
	p.records.blobs[blobIDOf(sess)].Deleted = true

	garbage := base64.RawURLEncoding.EncodeToString([]byte("not a token at all!!"))

	for name, token := range map[string]string{
		"unknown":      unknown,
		"revoked":      revoked,
		"expired":      expired,
		"deleted blob": deletedBlob,
		"garbage":      garbage,
	} {
		resp := p.get(t, p.aBytes, token)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: got %d, want 404", name, resp.StatusCode)
		}
		// The body must not distinguish them either.
		if len(body) > 64 {
			t.Errorf("%s: 404 body is %d bytes; a refusal must not explain itself", name, len(body))
		}
	}
}

// "HEAD authorizes and answers headers, no metric."
func TestHeadAnswersHeadersAndTakesNoMetric(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")

	resp := p.do(t, http.MethodHead, p.aBytes, token, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD: got %d, want 200", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("HEAD returned a %d-byte body", len(body))
	}
	if resp.Header.Get("Content-Disposition") == "" {
		t.Error("HEAD must answer the same headers as GET")
	}
	if r, _, _ := p.a.stats.PendingFor(LinkTokenHash(token)); r != 0 {
		t.Errorf("HEAD counted %d resolutions; it must count none", r)
	}

	// And a GET on the same link does count.
	p.get(t, p.aBytes, token).Body.Close()
	if r, b, _ := p.a.stats.PendingFor(LinkTokenHash(token)); r != 1 || b != int64(len(pdfBody)) {
		t.Errorf("GET counted resolutions=%d bytes=%d, want 1 and %d", r, b, len(pdfBody))
	}
}

// "other methods 405."
func TestOtherMethodsOnTheDownloadPathAre405(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		resp := p.do(t, method, p.aBytes, token, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s: got %d, want 405", method, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s: Allow: got %q, want %q", method, got, "GET, HEAD")
		}
	}
}

// "Range requests supported (PDF viewers use them)" — and bytes_served counts the
// RANGE, not the file.
func TestRangeRequestsAreServedAndCountedByWhatWentOut(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")

	resp := p.do(t, http.MethodGet, p.aBytes, token, http.Header{"Range": {"bytes=0-7"}})
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range request: got %d, want 206", resp.StatusCode)
	}
	if string(body) != string(pdfBody[:8]) {
		t.Fatalf("range body: got %q, want %q", body, pdfBody[:8])
	}
	if _, b, _ := p.a.stats.PendingFor(LinkTokenHash(token)); b != 8 {
		t.Errorf("bytes_served counted %d, want the 8 bytes of the range", b)
	}
}

// A revoked link stops resolving IMMEDIATELY, which is v3's improvement over an
// unrecallable signed URL (R7).
func TestRevocationTakesEffectOnTheNextRequest(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")

	first := p.get(t, p.aBytes, token)
	first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("before revocation: got %d, want 200", first.StatusCode)
	}

	p.records.links[LinkTokenHash(token)].RevokedMs = p.nowMs

	second := p.get(t, p.aBytes, token)
	second.Body.Close()
	if second.StatusCode != http.StatusNotFound {
		t.Fatalf("after revocation: got %d, want 404", second.StatusCode)
	}
}

// Stratum unreachable is 503, not 404 (R11): the caller is being told about US,
// and a 404 would mean "your link is gone" to a client that should retry.
func TestStratumUnreachableIs503NotNotFound(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")

	p.records.failReads = true
	resp := p.get(t, p.aBytes, token)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", resp.StatusCode)
	}
}

// Nothing but the two byte paths is served on the public listener.
func TestThePublicListenerServesNothingElse(t *testing.T) {
	p := newPair(t)
	for _, path := range []string{"/", "/healthz", PeerPathHashes, "/peer/blob/documents/:tenants:acme/x"} {
		resp, err := http.Get(p.aBytes.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: got %d, want 404 — the peer paths must not be on the public listener", path, resp.StatusCode)
		}
	}
}
