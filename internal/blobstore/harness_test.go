package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
)

// The harness stands up the REAL pair: two Services, each with its own temp
// directory for a PVC, each with a live peer listener the other one forwards to.
// Nothing about the mirror, the promotion order, the hashing or the HTTP paths is
// stubbed — only stratum is, because the record plane needs a cluster.
//
// It exists because every behaviour the decision doc asks for a test of is a
// behaviour OF THE PAIR: "write refused with one pod down, reads served", "hash
// mismatch cleans both pods", "anti-entropy rebuilds an empty PVC". A single-pod
// test could not observe any of them.

const testGroup = "documents"

// pair is two pods that mirror to each other.
type pair struct {
	a, b       *Service
	aBytes     *httptest.Server
	bBytes     *httptest.Server
	aPeer      *httptest.Server
	bPeer      *httptest.Server
	records    *fakeRecords
	nowMs      int64
	aPeerRoute *routableHandler
	bPeerRoute *routableHandler
}

// routableHandler lets a peer listener be bound before the Service that serves it
// exists, which is what breaks the chicken-and-egg of two pods that each need the
// other's address. Setting delegate to nil is also how a test takes a pod DOWN
// without closing the port — the peer then answers 503 to the health probe, which
// is what a rolling restart looks like from the other pod.
type routableHandler struct {
	mu       sync.Mutex
	delegate http.Handler
}

func (r *routableHandler) set(h http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delegate = h
}

func (r *routableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	h := r.delegate
	r.mu.Unlock()
	if h == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	h.ServeHTTP(w, req)
}

func newPair(t *testing.T) *pair {
	t.Helper()
	p := &pair{
		records:    newFakeRecords(),
		nowMs:      1_700_000_000_000,
		aPeerRoute: &routableHandler{},
		bPeerRoute: &routableHandler{},
	}
	p.aPeer = httptest.NewServer(p.aPeerRoute)
	p.bPeer = httptest.NewServer(p.bPeerRoute)
	t.Cleanup(p.aPeer.Close)
	t.Cleanup(p.bPeer.Close)

	p.a = p.newPod(t, "pod-a", hostOf(t, p.aPeer.URL), hostOf(t, p.bPeer.URL))
	p.b = p.newPod(t, "pod-b", hostOf(t, p.bPeer.URL), hostOf(t, p.aPeer.URL))
	p.aPeerRoute.set(p.a.PeerHandler())
	p.bPeerRoute.set(p.b.PeerHandler())

	p.aBytes = httptest.NewServer(p.a.BytesHandler())
	p.bBytes = httptest.NewServer(p.b.BytesHandler())
	t.Cleanup(p.aBytes.Close)
	t.Cleanup(p.bBytes.Close)
	return p
}

// newPod builds one Service over a fresh temp directory.
//
// `self` is a synthetic hostname rather than the machine's, so NewMirror's
// self-identification is exercised the way it runs in the cluster (match the first
// DNS label of a peer entry) instead of being bypassed.
func (p *pair) newPod(t *testing.T, self, selfAddr, peerAddr string) *Service {
	t.Helper()
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	// The peers list carries BOTH addresses including this pod's own, which is
	// what the operator renders.
	mirror, err := NewMirror([]string{self + "." + selfAddr, peerAddr}, self, httpClientForPeer())
	if err != nil {
		t.Fatalf("NewMirror: %v", err)
	}
	svc := NewService(store, mirror, p.records, func() time.Time {
		return time.UnixMilli(p.nowMs)
	})
	if err := svc.Configure(testConfig()); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return svc
}

// hostOf reduces an httptest URL to host:port.
func hostOf(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parsing %s: %v", raw, err)
	}
	return u.Host
}

func testConfig() *blobstore_pb.Config {
	return &blobstore_pb.Config{
		VolumeGroups: []*blobstore_pb.VolumeGroup{{
			Name:             testGroup,
			MaxBlobBytes:     1024,
			ContentTypes:     []string{"application/pdf", "text/plain"},
			TenantQuotaBytes: 4096,
		}},
		UploadTokenTtlSeconds:       900,
		StatsFlushIntervalSeconds:   60,
		SessionSweepIntervalSeconds: 60,
	}
}

// mintUpload produces a live upload token for `body`, the way CreateUpload would,
// and records the session it resolves to. The RPC path is exercised separately;
// this keeps the byte-path tests about bytes.
//
// The token and the row are one thing now: the row's key IS the token's sha256,
// so there is nothing to keep in step by hand.
func (p *pair) mintUpload(t *testing.T, _ *Service, namespace, contentType string, body []byte) (string, *blobstore_pb.UploadSession) {
	t.Helper()
	sum := sha256.Sum256(body)
	token, uploadID, err := MintUploadToken()
	if err != nil {
		t.Fatalf("MintUploadToken: %v", err)
	}
	sess := &blobstore_pb.UploadSession{
		UploadId:       uploadID,
		Group:          testGroup,
		ContentHash:    hex.EncodeToString(sum[:]),
		SizeBytes:      int64(len(body)),
		ContentType:    contentType,
		ReservedBytes:  int64(len(body)),
		State:          blobstore_pb.UploadState_UPLOAD_STATE_PENDING,
		CreatedMs:      p.nowMs,
		ExpiresMs:      p.nowMs + 60_000,
		OwnerPrincipal: "docmanager",
		Namespace:      namespace,
	}
	p.records.putSession(sess)
	return token, sess
}

// put streams a body to one pod's public upload path and returns the status code.
func put(t *testing.T, srv *httptest.Server, token string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, srv.URL+PathUpload+token, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building PUT: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// ---------------------------------------------------------------------------
// fakeRecords — stratum, in memory
// ---------------------------------------------------------------------------

// fakeRecords is the only stub in the harness. It behaves the way the real record
// plane behaves in the two ways the code depends on: a publish REPLACES the row
// with that key (a record's write path is a republish on its bus), and a missing
// row reads as (nil, nil) rather than as an error.
type fakeRecords struct {
	mu       sync.Mutex
	blobs    map[string]*blobstore_pb.Blob
	links    map[string]*blobstore_pb.BlobLink
	sessions map[string]*blobstore_pb.UploadSession
	stats    map[string]*blobstore_pb.BlobLinkStats

	// failReads makes every read return an error, for the fail-closed tests (R11).
	failReads bool
	// failPublish makes every publish return an error.
	failPublish bool
}

func newFakeRecords() *fakeRecords {
	return &fakeRecords{
		blobs:    map[string]*blobstore_pb.Blob{},
		links:    map[string]*blobstore_pb.BlobLink{},
		sessions: map[string]*blobstore_pb.UploadSession{},
		stats:    map[string]*blobstore_pb.BlobLinkStats{},
	}
}

var errFakeStratum = fmt.Errorf("fake stratum is unreachable")

func (f *fakeRecords) putSession(s *blobstore_pb.UploadSession) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.GetUploadId()] = s
}

func (f *fakeRecords) FetchBlob(_ context.Context, blobID string) (*blobstore_pb.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	return f.blobs[blobID], nil
}

func (f *fakeRecords) ListBlobsInGroup(_ context.Context, group, namespace string) ([]*blobstore_pb.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	var out []*blobstore_pb.Blob
	for _, b := range f.blobs {
		if b.GetGroup() == group && b.GetNamespace() == namespace {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetBlobId() < out[j].GetBlobId() })
	return out, nil
}

func (f *fakeRecords) FetchLink(_ context.Context, tokenHash string) (*blobstore_pb.BlobLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	return f.links[tokenHash], nil
}

func (f *fakeRecords) ListLinksForBlob(_ context.Context, blobID, namespace string) ([]*blobstore_pb.BlobLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	var out []*blobstore_pb.BlobLink
	for _, l := range f.links {
		if l.GetBlobId() == blobID && l.GetNamespace() == namespace {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetTokenHash() < out[j].GetTokenHash() })
	return out, nil
}

func (f *fakeRecords) FetchSession(_ context.Context, uploadID string) (*blobstore_pb.UploadSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	return f.sessions[uploadID], nil
}

func (f *fakeRecords) ListExpiredSessions(_ context.Context, nowMs int64) ([]*blobstore_pb.UploadSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	var out []*blobstore_pb.UploadSession
	for _, s := range f.sessions {
		if s.GetExpiresMs() > 0 && s.GetExpiresMs() <= nowMs {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetUploadId() < out[j].GetUploadId() })
	return out, nil
}

func (f *fakeRecords) FetchLinkStats(_ context.Context, tokenHash string) (*blobstore_pb.BlobLinkStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failReads {
		return nil, errFakeStratum
	}
	return f.stats[tokenHash], nil
}

func (f *fakeRecords) PublishBlob(_ context.Context, rec *blobstore_pb.Blob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish {
		return errFakeStratum
	}
	f.blobs[rec.GetBlobId()] = rec
	return nil
}

func (f *fakeRecords) PublishSession(_ context.Context, rec *blobstore_pb.UploadSession) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish {
		return errFakeStratum
	}
	f.sessions[rec.GetUploadId()] = rec
	return nil
}

func (f *fakeRecords) PublishLink(_ context.Context, rec *blobstore_pb.BlobLink) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish {
		return errFakeStratum
	}
	f.links[rec.GetTokenHash()] = rec
	return nil
}

func (f *fakeRecords) PublishLinkStats(_ context.Context, rec *blobstore_pb.BlobLinkStats) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failPublish {
		return errFakeStratum
	}
	f.stats[rec.GetTokenHash()] = rec
	return nil
}

// mintLink records a live BEARER link for a blob and returns the raw token, the
// way CreateLink would. The RPC path is tested separately; this keeps the
// download tests about the download.
func (p *pair) mintLink(t *testing.T, sess *blobstore_pb.UploadSession) string {
	t.Helper()
	return p.mintLinkNamed(t, sess, "report.pdf")
}

func (p *pair) mintLinkNamed(t *testing.T, sess *blobstore_pb.UploadSession, filename string) string {
	t.Helper()
	token, tokenHash, err := MintLinkToken()
	if err != nil {
		t.Fatalf("MintLinkToken: %v", err)
	}
	p.records.links[tokenHash] = &blobstore_pb.BlobLink{
		TokenHash:      tokenHash,
		BlobId:         BlobKey{Group: sess.GetGroup(), Namespace: sess.GetNamespace(), Hash: sess.GetContentHash()}.String(),
		Filename:       filename,
		Policy:         blobstore_pb.LinkPolicy_LINK_POLICY_BEARER,
		CreatedMs:      p.nowMs,
		OwnerPrincipal: sess.GetOwnerPrincipal(),
		Namespace:      sess.GetNamespace(),
	}
	return token
}

// get fetches a link from one pod's public byte path.
func (p *pair) get(t *testing.T, srv *httptest.Server, token string) *http.Response {
	t.Helper()
	return p.do(t, http.MethodGet, srv, token, nil)
}

func (p *pair) do(t *testing.T, method string, srv *httptest.Server, token string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+PathBlob+token, nil)
	if err != nil {
		t.Fatalf("building %s: %v", method, err)
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return resp
}

// bytesOf is a reader over a string, for the anti-entropy sink test.
func bytesOf(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

// uploadOne runs one successful upload through the public path and returns its
// session, for the tests that need a blob to exist before they start.
func (p *pair) uploadOne(t *testing.T) *blobstore_pb.UploadSession {
	t.Helper()
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("setup upload: got %d, want 201", got)
	}
	return sess
}
