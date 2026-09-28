package blobstore

import (
	"context"
	"strings"
	"testing"

	"gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/chassis/collection"
	"gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/chassis/servicemanager"
	wrapperToken "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/chassis/token"
	blobstore_pb "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/types/services/blobstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callerFor builds the verified-caller value the generated wrapper hands a
// handler. The tenant half of authorization has already run by then — every RPC
// declares target_namespace_field — so what these tests exercise is the SERVICE
// half: is this caller the row's owner (R6).
func callerFor(service string) *servicemanager.Caller {
	tok := wrapperToken.NewImmutableToken(nil).SetSubType("service").SetSubject(service)
	return servicemanager.NewCaller(tok, "test-request")
}

func codeOf(err error) codes.Code { return status.Code(err) }

func TestCreateUploadReservesQuotaAndRefusesPastIt(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")

	req := func(size int64) *blobstore_pb.CreateUploadRequest {
		return &blobstore_pb.CreateUploadRequest{
			Group:       testGroup,
			ContentHash: strings.Repeat("a", 64),
			SizeBytes:   size,
			ContentType: "application/pdf",
			Namespace:   testNS,
		}
	}

	// The group's tenant quota is 4096 and max_blob_bytes is 1024, so four
	// reservations fit exactly and the fifth must not.
	for i := 0; i < 4; i++ {
		if _, err := h.CreateUpload(ctx, caller, req(1024)); err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	_, err := h.CreateUpload(ctx, caller, req(1024))
	if codeOf(err) != codes.ResourceExhausted {
		t.Fatalf("the reservation past the quota: got %v, want ResourceExhausted", err)
	}

	// Cancelling one frees exactly one.
	var pending string
	for id, s := range p.records.sessions {
		if s.GetState() == blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
			pending = id
			break
		}
	}
	if pending == "" {
		t.Fatal("no pending session to cancel")
	}
	resp, err := h.CancelUpload(ctx, caller, &blobstore_pb.CancelUploadRequest{UploadId: pending, Namespace: testNS})
	if err != nil || !resp.GetCancelled() {
		t.Fatalf("CancelUpload: resp=%v err=%v", resp, err)
	}
	if _, err := h.CreateUpload(ctx, caller, req(1024)); err != nil {
		t.Fatalf("after cancelling one reservation: %v", err)
	}

	// CancelUpload is idempotent.
	again, err := h.CancelUpload(ctx, caller, &blobstore_pb.CancelUploadRequest{UploadId: pending, Namespace: testNS})
	if err != nil {
		t.Fatalf("second CancelUpload: %v", err)
	}
	if again.GetCancelled() {
		t.Error("a second cancel reported cancelled=true; it must be a no-op")
	}
}

func TestCreateUploadRefusesAGroupRuleBreach(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")

	cases := map[string]*blobstore_pb.CreateUploadRequest{
		"unknown group": {Group: "photos", ContentHash: strings.Repeat("a", 64), SizeBytes: 10, ContentType: "application/pdf", Namespace: testNS},
		"type not allowed": {Group: testGroup, ContentHash: strings.Repeat("a", 64), SizeBytes: 10,
			ContentType: "application/x-msdownload", Namespace: testNS},
		"past max_blob_bytes": {Group: testGroup, ContentHash: strings.Repeat("a", 64), SizeBytes: 99999,
			ContentType: "application/pdf", Namespace: testNS},
	}
	for name, req := range cases {
		if _, err := h.CreateUpload(ctx, caller, req); codeOf(err) != codes.FailedPrecondition {
			t.Errorf("%s: got %v, want FailedPrecondition", name, err)
		}
	}

	// A hash or namespace that is not a legal path segment is INVALID_ARGUMENT, not
	// FailedPrecondition: the request itself is malformed.
	bad := &blobstore_pb.CreateUploadRequest{Group: testGroup, ContentHash: "../../x", SizeBytes: 10,
		ContentType: "application/pdf", Namespace: testNS}
	if _, err := h.CreateUpload(ctx, caller, bad); codeOf(err) != codes.InvalidArgument {
		t.Errorf("a traversal content hash: got %v, want InvalidArgument", err)
	}
}

// already_present is what lets a consumer skip the transfer, which is the payoff
// of declaring the hash up front.
func TestCreateUploadReportsAlreadyPresent(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t)

	resp, err := h.CreateUpload(context.Background(), callerFor("docmanager"), &blobstore_pb.CreateUploadRequest{
		Group: sess.GetGroup(), ContentHash: sess.GetContentHash(), SizeBytes: sess.GetSizeBytes(),
		ContentType: sess.GetContentType(), Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	if !resp.GetAlreadyPresent() {
		t.Error("already_present is false for bytes this tenant already holds")
	}
	if resp.GetBlobId() != blobIDOf(sess) {
		t.Errorf("blob_id = %q, want %q", resp.GetBlobId(), blobIDOf(sess))
	}
	if resp.GetUploadPath() != PathUpload+resp.GetUploadToken() {
		t.Error("upload_path must be the /upload/ prefix plus the token")
	}
}

// Another service's blob is NOT_FOUND, not PERMISSION_DENIED: "exists but is not
// yours" is the content oracle per-tenant addressing was chosen to avoid (R5, R6).
func TestAnotherServicesBlobIsNotFound(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t) // owned by docmanager
	ctx := context.Background()
	intruder := callerFor("someoneelse")

	if _, err := h.CreateLink(ctx, intruder, &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: sess.GetNamespace(),
	}); codeOf(err) != codes.NotFound {
		t.Errorf("CreateLink by a non-owner: got %v, want NotFound", err)
	}
	if _, err := h.DeleteBlob(ctx, intruder, &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: sess.GetNamespace(),
	}); codeOf(err) != codes.NotFound {
		t.Errorf("DeleteBlob by a non-owner: got %v, want NotFound", err)
	}
	// And the owner naming another tenant's namespace is refused the same way.
	if _, err := h.CreateLink(ctx, callerFor("docmanager"), &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: "/tenants/other",
	}); codeOf(err) != codes.NotFound {
		t.Errorf("CreateLink naming another tenant: got %v, want NotFound", err)
	}
}

// CreateLink returns the raw token once; the row holds only its hash (R7).
func TestCreateLinkReturnsTheTokenOnceAndStoresOnlyItsHash(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t)

	resp, err := h.CreateLink(context.Background(), callerFor("docmanager"), &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "report.pdf", Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	if resp.GetToken() == "" || resp.GetTokenHash() != LinkTokenHash(resp.GetToken()) {
		t.Fatal("the response must carry the raw token and its matching hash")
	}
	if resp.GetLinkPath() != PathBlob+resp.GetToken() {
		t.Error("link_path must be the /b/ prefix plus the token")
	}
	row := p.records.links[resp.GetTokenHash()]
	if row == nil {
		t.Fatal("no BlobLink row was published")
	}
	if strings.Contains(row.String(), resp.GetToken()) {
		t.Fatal("the stored row contains the raw token")
	}
	if row.GetPolicy() != blobstore_pb.LinkPolicy_LINK_POLICY_BEARER {
		t.Errorf("policy = %v, want BEARER", row.GetPolicy())
	}
	if row.GetExpiresMs() != 0 {
		t.Error("a durable shared link must not carry an expiry")
	}

	// A filename that would break the header is refused at mint, once.
	if _, err := h.CreateLink(context.Background(), callerFor("docmanager"), &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf\r\nSet-Cookie: a=b", Namespace: sess.GetNamespace(),
	}); codeOf(err) != codes.InvalidArgument {
		t.Errorf("a header-injecting filename: got %v, want InvalidArgument", err)
	}
}

// CreateBlobUrl is the same row with an expiry — the doc's GetBlobUrl.
func TestCreateBlobUrlIsShortLived(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t)

	resp, err := h.CreateBlobUrl(context.Background(), callerFor("docmanager"), &blobstore_pb.CreateBlobUrlRequest{
		BlobId: blobIDOf(sess), Filename: "report.pdf", TtlSeconds: 60, Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("CreateBlobUrl: %v", err)
	}
	if want := p.nowMs + 60_000; resp.GetExpiresMs() != want {
		t.Errorf("expires_ms = %d, want %d", resp.GetExpiresMs(), want)
	}
	if got := p.records.links[resp.GetTokenHash()].GetExpiresMs(); got != resp.GetExpiresMs() {
		t.Errorf("the stored row's expiry (%d) differs from the response's (%d)", got, resp.GetExpiresMs())
	}
}

// RevokeLink is immediate and idempotent, and the row is kept for audit (R7).
func TestRevokeLinkKeepsTheRowAndIsIdempotent(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	sess := p.uploadOne(t)
	link, err := h.CreateLink(ctx, callerFor("docmanager"), &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}

	first, err := h.RevokeLink(ctx, callerFor("docmanager"), &blobstore_pb.RevokeLinkRequest{
		TokenHash: link.GetTokenHash(), Namespace: sess.GetNamespace(),
	})
	if err != nil || !first.GetRevoked() {
		t.Fatalf("RevokeLink: resp=%v err=%v", first, err)
	}
	row := p.records.links[link.GetTokenHash()]
	if row == nil {
		t.Fatal("revocation deleted the row; it must be kept for audit")
	}
	if row.GetRevokedMs() != p.nowMs {
		t.Errorf("revoked_ms = %d, want %d", row.GetRevokedMs(), p.nowMs)
	}

	second, err := h.RevokeLink(ctx, callerFor("docmanager"), &blobstore_pb.RevokeLinkRequest{
		TokenHash: link.GetTokenHash(), Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("second RevokeLink: %v", err)
	}
	if second.GetRevoked() {
		t.Error("a second revoke reported revoked=true; it must be a no-op")
	}
}

// ListLinks returns no tokens, pages over the declared order, and shows only the
// caller's own rows.
func TestListLinksPagesAndCarriesNoTokens(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	sess := p.uploadOne(t)
	owner := callerFor("docmanager")

	for i := 0; i < 5; i++ {
		if _, err := h.CreateLink(ctx, owner, &blobstore_pb.CreateLinkRequest{
			BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: sess.GetNamespace(),
		}); err != nil {
			t.Fatalf("CreateLink %d: %v", i, err)
		}
	}

	req := &blobstore_pb.ListLinksRequest{BlobId: blobIDOf(sess), Namespace: sess.GetNamespace()}
	var seen []string
	var after *collection.Cursor
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("paging did not terminate")
		}
		page, err := h.ListLinks(ctx, owner, req, collection.PageParams{Limit: 2, After: after})
		if err != nil {
			t.Fatalf("ListLinks: %v", err)
		}
		if page.Total != 5 {
			t.Errorf("Total = %d, want 5", page.Total)
		}
		for _, l := range page.Items {
			if len(l.GetTokenHash()) != 64 {
				t.Errorf("a listed row's token_hash is not a hash: %q", l.GetTokenHash())
			}
			seen = append(seen, l.GetTokenHash())
		}
		if page.NextAfter == nil {
			break
		}
		if len(page.Items) != 2 {
			t.Errorf("a non-final page has %d items, want the limit of 2", len(page.Items))
		}
		after = page.NextAfter
	}
	if len(seen) != 5 {
		t.Fatalf("paged over %d rows, want 5", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] >= seen[i] {
			t.Fatalf("rows are not in the declared token_hash order: %q then %q", seen[i-1], seen[i])
		}
	}

	// Another service sees none of them.
	page, err := h.ListLinks(ctx, callerFor("someoneelse"), req, collection.PageParams{Limit: 10})
	if codeOf(err) != codes.NotFound {
		t.Errorf("ListLinks by a non-owner: got %v (%d items), want NotFound", err, len(page.Items))
	}
}

// DeleteBlob unlinks on BOTH pods and flags the row; it is idempotent.
func TestDeleteBlobRemovesTheBytesFromBothPods(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	sess := p.uploadOne(t)
	owner := callerFor("docmanager")

	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if !svc.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
			t.Fatalf("pod %s does not hold the blob before the delete", name)
		}
	}

	resp, err := h.DeleteBlob(ctx, owner, &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: sess.GetNamespace(),
	})
	if err != nil || !resp.GetDeleted() {
		t.Fatalf("DeleteBlob: resp=%v err=%v", resp, err)
	}
	for name, svc := range map[string]*Service{"a": p.a, "b": p.b} {
		if svc.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
			t.Errorf("pod %s still holds the bytes after the delete", name)
		}
	}
	if !p.records.blobs[blobIDOf(sess)].GetDeleted() {
		t.Error("the Blob row was not flagged deleted")
	}

	again, err := h.DeleteBlob(ctx, owner, &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("second DeleteBlob: %v", err)
	}
	if again.GetDeleted() {
		t.Error("a second delete reported deleted=true; it must be a no-op")
	}
}

// A delete the peer cannot confirm fails the whole delete: bytes gone on one pod
// and not the other is worse than a delete the owner retries.
func TestDeleteBlobFailsWhenThePeerCannotBeReached(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	sess := p.uploadOne(t)
	p.bPeerRoute.set(nil)

	_, err := h.DeleteBlob(context.Background(), callerFor("docmanager"), &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: sess.GetNamespace(),
	})
	if codeOf(err) != codes.Unavailable {
		t.Fatalf("DeleteBlob with the peer down: got %v, want Unavailable", err)
	}
	if !p.a.store.Has(sess.GetGroup(), sess.GetNamespace(), sess.GetContentHash()) {
		t.Error("the local bytes were removed even though the peer's could not be")
	}
	if p.records.blobs[blobIDOf(sess)].GetDeleted() {
		t.Error("the row was flagged deleted even though the bytes remain")
	}
}

// Stratum unreachable fails every RPC closed (R11).
func TestRpcsFailClosedWhenStratumIsUnreachable(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	owner := callerFor("docmanager")
	sess := p.uploadOne(t)
	p.records.failReads = true

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
	if _, err := h.DeleteBlob(ctx, owner, &blobstore_pb.DeleteBlobRequest{
		BlobId: blobIDOf(sess), Namespace: testNS,
	}); codeOf(err) != codes.Unavailable {
		t.Errorf("DeleteBlob: got %v, want Unavailable", err)
	}
}

// GetLinkStats adds this pod's unflushed delta to the stored row, so a caller that
// reads right after a download is not told zero (R10).
func TestGetLinkStatsIncludesTheUnflushedDelta(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")
	tokenHash := LinkTokenHash(token)

	p.get(t, p.aBytes, token).Body.Close()

	before, err := h.GetLinkStats(ctx, callerFor("docmanager"), &blobstore_pb.GetLinkStatsRequest{
		TokenHash: tokenHash, Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("GetLinkStats: %v", err)
	}
	if before.GetResolutions() != 1 || before.GetBytesServed() != int64(len(pdfBody)) {
		t.Fatalf("unflushed: resolutions=%d bytes=%d, want 1 and %d",
			before.GetResolutions(), before.GetBytesServed(), len(pdfBody))
	}

	// After a flush the same numbers come from the stored row, not from memory —
	// and a second download adds to them rather than replacing them.
	p.a.stats.Flush(ctx)
	p.get(t, p.aBytes, token).Body.Close()
	after, err := h.GetLinkStats(ctx, callerFor("docmanager"), &blobstore_pb.GetLinkStatsRequest{
		TokenHash: tokenHash, Namespace: sess.GetNamespace(),
	})
	if err != nil {
		t.Fatalf("GetLinkStats: %v", err)
	}
	if after.GetResolutions() != 2 {
		t.Errorf("after a flush and a second download: resolutions=%d, want 2", after.GetResolutions())
	}
}

// A flush READS BEFORE IT ADDS, so the other pod's contribution is never lost.
func TestStatsFlushDoesNotLoseThePeersCount(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLinkNamed(t, sess, "x.pdf")
	tokenHash := LinkTokenHash(token)
	ctx := context.Background()

	// One download on each pod, then each pod flushes.
	p.get(t, p.aBytes, token).Body.Close()
	p.get(t, p.bBytes, token).Body.Close()
	p.a.stats.Flush(ctx)
	p.b.stats.Flush(ctx)

	row := p.records.stats[tokenHash]
	if row == nil {
		t.Fatal("no BlobLinkStats row was published")
	}
	if row.GetResolutions() != 2 {
		t.Errorf("resolutions = %d, want 2 — one pod's flush discarded the other's", row.GetResolutions())
	}
	if want := int64(2 * len(pdfBody)); row.GetBytesServed() != want {
		t.Errorf("bytes_served = %d, want %d", row.GetBytesServed(), want)
	}
}

// The expiry sweep releases the reservation and settles the row (R9 step 4).
func TestSweepSettlesExpiredSessionsAndFreesTheirQuota(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")
	req := &blobstore_pb.CreateUploadRequest{
		Group: testGroup, ContentHash: strings.Repeat("a", 64), SizeBytes: 1024,
		ContentType: "application/pdf", Namespace: testNS,
	}

	// Fill the quota with reservations that are never used.
	for i := 0; i < 4; i++ {
		if _, err := h.CreateUpload(ctx, caller, req); err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	if _, err := h.CreateUpload(ctx, caller, req); codeOf(err) != codes.ResourceExhausted {
		t.Fatalf("the reservation past the quota: got %v, want ResourceExhausted", err)
	}

	// Time passes past the 15-minute token TTL and the sweep runs.
	p.nowMs += 16 * 60 * 1000
	p.a.sweepSessions(ctx)

	for id, s := range p.records.sessions {
		if s.GetState() != blobstore_pb.UploadState_UPLOAD_STATE_ABORTED {
			t.Errorf("session %s is %v after the sweep, want ABORTED", id, s.GetState())
		}
		if s.GetReservedBytes() != 0 {
			t.Errorf("session %s still reserves %d bytes", id, s.GetReservedBytes())
		}
		if !s.GetDeleted() {
			t.Errorf("session %s was not flagged deleted; the ledger would not be purged", id)
		}
	}
	if _, err := h.CreateUpload(ctx, caller, req); err != nil {
		t.Fatalf("after the sweep freed the quota: %v", err)
	}
}
