package blobstore

import (
	"context"
	"net/http"
	"strings"
	"testing"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// The service's answers to stratum's write surface (#6): a write the store
// refuses is refused to the caller, and a row another writer changed between
// this call's read and its write is never overwritten.

// While the redo service is unreachable stratum refuses every write. Every path
// that writes a record refuses with it (R11), and the upload leaves no Blob row.
func TestEveryWriteIsRefusedWhileTheStoreRefusesWrites(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	owner := callerFor("docmanager")
	sess := p.uploadOne(t)
	link := p.mintLinkNamed(t, sess, "x.pdf")
	token, pending := p.mintUpload(t, p.a, testNS, "text/plain", []byte("hello"))
	p.records.failWrites = true

	if _, err := h.CreateUpload(ctx, owner, &blobstore_pb.CreateUploadRequest{
		Group: testGroup, ContentHash: strings.Repeat("b", 64), SizeBytes: 10,
		ContentType: "application/pdf", Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("CreateUpload: got %v, want Unavailable", err)
	}
	if _, err := h.CreateLink(ctx, owner, &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("CreateLink: got %v, want Unavailable", err)
	}
	if _, err := h.CreateBlobUrl(ctx, owner, &blobstore_pb.CreateBlobUrlRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", TtlSeconds: 60, Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("CreateBlobUrl: got %v, want Unavailable", err)
	}
	if _, err := h.RevokeLink(ctx, owner, &blobstore_pb.RevokeLinkRequest{
		TokenHash: LinkTokenHash(link), Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("RevokeLink: got %v, want Unavailable", err)
	}
	if _, err := h.CancelUpload(ctx, owner, &blobstore_pb.CancelUploadRequest{
		UploadId: pending.GetUploadId(), Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("CancelUpload: got %v, want Unavailable", err)
	}
	if _, err := h.DeleteBlob(ctx, owner, &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("DeleteBlob: got %v, want Unavailable", err)
	}

	if got := put(t, p.aBytes, token, []byte("hello")); got != http.StatusServiceUnavailable {
		t.Errorf("PUT: got %d, want 503", got)
	}
	key := BlobKey{Group: pending.GetGroup(), Namespace: pending.GetNamespace(), Hash: pending.GetContentHash()}
	if p.records.blobs[key.String()] != nil {
		t.Error("a Blob row exists for an upload whose write was refused")
	}
}

// Two flushes of one link's stats at once (one per pod): the one that read the
// row before the other wrote it is refused, keeps its delta, and adds it on the
// next tick. Neither pod's count is lost.
func TestStatsFlushThatLosesARaceKeepsItsDelta(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")
	tokenHash := LinkTokenHash(token)
	ctx := context.Background()

	p.get(t, p.aBytes, token).Body.Close()
	p.get(t, p.bBytes, token).Body.Close()

	// Pod b flushes between pod a's read of the (absent) row and its write.
	p.records.beforeWrite = func() { p.b.stats.Flush(ctx) }
	p.a.stats.Flush(ctx)
	if got := p.records.stats[tokenHash].GetResolutions(); got != 1 {
		t.Fatalf("after the race: resolutions=%d, want 1 (pod b's)", got)
	}
	if r, _, _ := p.a.stats.PendingFor(tokenHash); r != 1 {
		t.Fatalf("pod a's refused delta: pending resolutions=%d, want 1", r)
	}

	p.a.stats.Flush(ctx)
	if got := p.records.stats[tokenHash].GetResolutions(); got != 2 {
		t.Errorf("after pod a's next flush: resolutions=%d, want 2", got)
	}
}

// A cancel racing the end of the upload: the session settles once. The cancel
// read the row PENDING, the upload committed it before the cancel's write, and
// the cancel answers cancelled=false instead of overwriting COMMITTED.
func TestCancelUploadDoesNotOverwriteASessionSettledUnderneathIt(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	_, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	p.records.beforeWrite = func() {
		committed := proto.Clone(sess).(*blobstore_pb.UploadSession)
		committed.State = blobstore_pb.UploadState_UPLOAD_STATE_COMMITTED
		p.records.putSession(committed)
	}
	resp, err := h.CancelUpload(ctx, callerFor("docmanager"), &blobstore_pb.CancelUploadRequest{
		UploadId: sess.GetUploadId(), Namespace: testNS,
	})
	if err != nil || resp.GetCancelled() {
		t.Fatalf("CancelUpload: got %v, %v; want cancelled=false", resp, err)
	}
	if got := p.records.sessions[sess.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_COMMITTED {
		t.Errorf("session state = %v, want COMMITTED", got)
	}
}

// A revoke whose link changed after it was read is ABORTED: the caller retries
// against the row as it now is.
func TestRevokeLinkOfALinkChangedUnderneathItIsAborted(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")
	tokenHash := LinkTokenHash(token)

	p.records.beforeWrite = func() {
		p.records.mu.Lock()
		defer p.records.mu.Unlock()
		changed := proto.Clone(p.records.links[tokenHash]).(*blobstore_pb.BlobLink)
		changed.ExpiresMs = p.nowMs + 1
		p.records.links[tokenHash] = changed
	}
	_, err := h.RevokeLink(context.Background(), callerFor("docmanager"), &blobstore_pb.RevokeLinkRequest{
		TokenHash: tokenHash, Namespace: testNS,
	})
	if codeOf(err) != codes.Aborted {
		t.Fatalf("RevokeLink: got %v, want Aborted", err)
	}
	if p.records.links[tokenHash].GetRevokedMs() != 0 {
		t.Error("the refused revoke was written")
	}
}
