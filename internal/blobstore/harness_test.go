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
	"google.golang.org/protobuf/proto"
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
	a, b    *Service
	aBytes  *httptest.Server
	bBytes  *httptest.Server
	aPeer   *httptest.Server
	bPeer   *httptest.Server
	records *fakeRecords
	nowMs   int64
	// tenants is what the sweep walks: the tenants kernel would report a
	// Stratum for.
	tenants    []string
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
		tenants:    []string{testTenant},
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
	svc := NewService(store, mirror, p.records, func() []string { return p.tenants }, func() time.Time {
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

// put streams a body to one pod's public upload path, under testTenant, and
// returns the status code.
func put(t *testing.T, srv *httptest.Server, token string, body []byte) int {
	t.Helper()
	return putPath(t, srv, UploadPath(testTenant, token), body)
}

// putPath streams a body to a path of one pod's public listener.
func putPath(t *testing.T, srv *httptest.Server, path string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, srv.URL+path, bytes.NewReader(body))
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

// fakeRecords is the only stub in the harness. It answers the way recordStore
// does over stratum's write surface: an insert of a stored key is
// ErrRecordExists, a replace of a row that changed since it was read is
// ErrRecordChanged, a Put replaces whatever is stored, and a missing row reads
// as (nil, nil) rather than as an error. records_test.go tests recordStore
// itself against a fake of stratum's gRPC surface.
type fakeRecords struct {
	mu       sync.Mutex
	blobs    map[string]*blobstore_pb.Blob
	links    map[string]*blobstore_pb.BlobLink
	sessions map[string]*blobstore_pb.UploadSession
	stats    map[string]*blobstore_pb.BlobLinkStats

	// failReads makes every read return an error, for the fail-closed tests (R11).
	failReads bool
	// failWrites makes every write refused as stratum refuses them while the
	// redo service is unreachable.
	failWrites bool
	// beforeWrite, when set, runs once before the next write takes the lock: it
	// is how a test makes another writer get there between a read and a write.
	beforeWrite func()
}

// interleave runs and clears beforeWrite.
func (f *fakeRecords) interleave() {
	f.mu.Lock()
	hook := f.beforeWrite
	f.beforeWrite = nil
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func newFakeRecords() *fakeRecords {
	return &fakeRecords{
		blobs:    map[string]*blobstore_pb.Blob{},
		links:    map[string]*blobstore_pb.BlobLink{},
		sessions: map[string]*blobstore_pb.UploadSession{},
		stats:    map[string]*blobstore_pb.BlobLinkStats{},
	}
}

var (
	errFakeStratum       = fmt.Errorf("fake stratum is unreachable")
	errFakeStratumWrites = fmt.Errorf("fake stratum: redo is unreachable: %w", ErrRecordStoreUnavailable)
)

func (f *fakeRecords) putSession(s *blobstore_pb.UploadSession) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.GetUploadId()] = s
}

// The fake is one store per tenant, as Stratum is: rows are kept in one map per
// type (their keys do not collide across tenants), but a call reaches only the
// rows of the tenant it names. Another tenant's row reads as absent, a write of a
// row outside the named tenant is refused as the store's route check refuses it
// (PERMISSION_DENIED), and a call that names no tenant is refused before it is
// sent, as the generated client refuses it.
func fakeRoute(tenant string) error {
	if !ValidTenantID(tenant) {
		return fmt.Errorf("fake stratum: call names no tenant (%q): %w", tenant, ErrRecordOutsideTenant)
	}
	return nil
}

func fakeRead[T interface{ GetNamespace() string }](f *fakeRecords, tenant string, row T) (T, error) {
	var none T
	if err := fakeRoute(tenant); err != nil {
		return none, err
	}
	if f.failReads {
		return none, errFakeStratum
	}
	if !inTenant(tenant, row.GetNamespace()) {
		return none, nil
	}
	return row, nil
}

func fakeList[T interface{ GetNamespace() string }](f *fakeRecords, tenant string, rows map[string]T, keep func(T) bool) ([]T, error) {
	if err := fakeRoute(tenant); err != nil {
		return nil, err
	}
	if f.failReads {
		return nil, errFakeStratum
	}
	var out []T
	for _, row := range rows {
		if inTenant(tenant, row.GetNamespace()) && keep(row) {
			out = append(out, row)
		}
	}
	return out, nil
}

func fakeWritable(tenant string, rows ...interface{ GetNamespace() string }) error {
	if err := fakeRoute(tenant); err != nil {
		return err
	}
	for _, row := range rows {
		if !inTenant(tenant, row.GetNamespace()) {
			return fmt.Errorf("fake stratum of %s: row of %q: %w", tenant, row.GetNamespace(), ErrRecordOutsideTenant)
		}
	}
	return nil
}

func (f *fakeRecords) FetchBlob(_ context.Context, tenant, blobID string) (*blobstore_pb.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeRead(f, tenant, f.blobs[blobID])
}

func (f *fakeRecords) ListBlobsInGroup(_ context.Context, tenant, group, namespace string) ([]*blobstore_pb.Blob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out, err := fakeList(f, tenant, f.blobs, func(b *blobstore_pb.Blob) bool {
		return b.GetGroup() == group && b.GetNamespace() == namespace
	})
	sort.Slice(out, func(i, j int) bool { return out[i].GetBlobId() < out[j].GetBlobId() })
	return out, err
}

func (f *fakeRecords) FetchLink(_ context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeRead(f, tenant, f.links[tokenHash])
}

func (f *fakeRecords) ListLinksForBlob(_ context.Context, tenant, blobID, namespace string) ([]*blobstore_pb.BlobLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out, err := fakeList(f, tenant, f.links, func(l *blobstore_pb.BlobLink) bool {
		return l.GetBlobId() == blobID && l.GetNamespace() == namespace
	})
	sort.Slice(out, func(i, j int) bool { return out[i].GetTokenHash() < out[j].GetTokenHash() })
	return out, err
}

func (f *fakeRecords) FetchSession(_ context.Context, tenant, uploadID string) (*blobstore_pb.UploadSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeRead(f, tenant, f.sessions[uploadID])
}

func (f *fakeRecords) ListExpiredSessions(_ context.Context, tenant string, nowMs int64) ([]*blobstore_pb.UploadSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out, err := fakeList(f, tenant, f.sessions, func(s *blobstore_pb.UploadSession) bool {
		return s.GetExpiresMs() > 0 && s.GetExpiresMs() <= nowMs
	})
	sort.Slice(out, func(i, j int) bool { return out[i].GetUploadId() < out[j].GetUploadId() })
	return out, err
}

func (f *fakeRecords) FetchLinkStats(_ context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLinkStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fakeRead(f, tenant, f.stats[tokenHash])
}

func (f *fakeRecords) PutBlob(_ context.Context, tenant string, rec *blobstore_pb.Blob) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, rec); err != nil {
		return err
	}
	if f.failWrites {
		return errFakeStratumWrites
	}
	f.blobs[rec.GetBlobId()] = rec
	return nil
}

func (f *fakeRecords) ReplaceBlob(_ context.Context, tenant string, prev, next *blobstore_pb.Blob) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, prev, next); err != nil {
		return err
	}
	return fakeReplace(f.failWrites, f.blobs, next.GetBlobId(), prev, next)
}

func (f *fakeRecords) InsertSession(_ context.Context, tenant string, rec *blobstore_pb.UploadSession) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, rec); err != nil {
		return err
	}
	return fakeInsert(f.failWrites, f.sessions, rec.GetUploadId(), rec)
}

func (f *fakeRecords) ReplaceSession(_ context.Context, tenant string, prev, next *blobstore_pb.UploadSession) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, prev, next); err != nil {
		return err
	}
	return fakeReplace(f.failWrites, f.sessions, next.GetUploadId(), prev, next)
}

func (f *fakeRecords) InsertLink(_ context.Context, tenant string, rec *blobstore_pb.BlobLink) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, rec); err != nil {
		return err
	}
	return fakeInsert(f.failWrites, f.links, rec.GetTokenHash(), rec)
}

func (f *fakeRecords) ReplaceLink(_ context.Context, tenant string, prev, next *blobstore_pb.BlobLink) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, prev, next); err != nil {
		return err
	}
	return fakeReplace(f.failWrites, f.links, next.GetTokenHash(), prev, next)
}

func (f *fakeRecords) InsertLinkStats(_ context.Context, tenant string, rec *blobstore_pb.BlobLinkStats) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, rec); err != nil {
		return err
	}
	return fakeInsert(f.failWrites, f.stats, rec.GetTokenHash(), rec)
}

func (f *fakeRecords) ReplaceLinkStats(_ context.Context, tenant string, prev, next *blobstore_pb.BlobLinkStats) error {
	f.interleave()
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := fakeWritable(tenant, prev, next); err != nil {
		return err
	}
	return fakeReplace(f.failWrites, f.stats, next.GetTokenHash(), prev, next)
}

// fakeInsert and fakeReplace answer as stratum's InsertIfAbsent and
// ReplaceIfUnchanged (against the expected row) do.
func fakeInsert[T proto.Message](fail bool, rows map[string]T, key string, rec T) error {
	if fail {
		return errFakeStratumWrites
	}
	if _, ok := rows[key]; ok {
		return ErrRecordExists
	}
	rows[key] = rec
	return nil
}

func fakeReplace[T proto.Message](fail bool, rows map[string]T, key string, prev, next T) error {
	if fail {
		return errFakeStratumWrites
	}
	stored, ok := rows[key]
	if !ok {
		return ErrRecordNotFound
	}
	if !proto.Equal(stored, prev) {
		return ErrRecordChanged
	}
	rows[key] = next
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
	return p.doPath(t, method, srv, BlobPath(testTenant, token), header)
}

// doPath requests a path of one pod's public listener.
func (p *pair) doPath(t *testing.T, method string, srv *httptest.Server, path string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
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
