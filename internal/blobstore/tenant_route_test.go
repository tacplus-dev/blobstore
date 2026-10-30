package blobstore

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"reflect"
	"strings"
	"testing"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"github.com/tacplus-dev/blobstore/tacplus/types/sys/healthreport"
	servicehealth_pb "github.com/tacplus-dev/blobstore/tacplus/types/sys/servicehealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// Tenant routing (#8). Stratum is tenant_owned: every record call names the
// tenant whose store it reaches, a bearer path names it before the token
// (/b/<tid>/<token>, /upload/<tid>/<token>), and a call routed to the wrong
// tenant finds nothing and writes nothing.

const (
	otherTenant = "other001"
	otherNS     = "/tenants/" + otherTenant
)

// ---------------------------------------------------------------------------
// recordStore, over the generated client
// ---------------------------------------------------------------------------

// The 2.5.2 plane cannot route by tenant, so the one store answers every call:
// another tenant's row must still read as absent and never be delivered.
func TestRecordsReadsRoutedToAnotherTenantFindNothing(t *testing.T) {
	ours, theirs := recordCasesFor(testTenant), recordCasesFor(otherTenant)
	for i := range ours {
		t.Run(ours[i].name, func(t *testing.T) {
			records, _ := newStratumRecords(t)
			ctx := context.Background()
			if err := ours[i].insert(ctx, records, ours[i].row); err != nil {
				t.Fatalf("write in its own tenant: %v", err)
			}
			got, err := theirs[i].read(ctx, records)
			if err != nil || got != nil {
				t.Fatalf("read routed to %s: got %v, %v; want nil, nil", otherTenant, got, err)
			}
		})
	}
}

func TestRecordsWatchRoutedToAnotherTenantSkipsItsRows(t *testing.T) {
	ours, theirs := recordCasesFor(testTenant), recordCasesFor(otherTenant)
	for i := range ours {
		t.Run(ours[i].name, func(t *testing.T) {
			records, fake := newStratumRecords(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := ours[i].insert(ctx, records, ours[i].row); err != nil {
				t.Fatalf("write in its own tenant: %v", err)
			}
			feed, err := theirs[i].watch(ctx, records)
			if err != nil {
				t.Fatalf("watch: %v", err)
			}
			next, stop := iter.Pull2(feed)
			defer stop()
			typ := "blobstore." + ours[i].name
			fake.waitWatching(t, typ)

			// testTenant's row is in the snapshot and testTenant's next write is a
			// change: the other tenant's feed must pass over both and deliver its
			// own row, written last.
			if err := ours[i].replace(ctx, records, ours[i].row, ours[i].changed); err != nil {
				t.Fatalf("replace in its own tenant: %v", err)
			}
			theirRow := proto.Clone(ours[i].row)
			setNamespace(theirRow, otherNS)
			if _, err := fake.write(typ, keyOf(theirRow)+"-other", fakePut, theirRow, nil); err != nil {
				t.Fatalf("writing %s's row: %v", otherTenant, err)
			}
			c := nextChange(t, next)
			if c.Row == nil || !proto.Equal(*c.Row, theirRow) {
				t.Fatalf("%s's watch delivered %+v; want only its own row", otherTenant, c)
			}
		})
	}
}

func TestRecordsWritesRoutedToAnotherTenantAreRefusedBeforeTheyAreSent(t *testing.T) {
	for _, tc := range recordCasesFor(otherTenant) {
		t.Run(tc.name, func(t *testing.T) {
			records, fake := newStratumRecords(t)
			ctx := context.Background()
			if err := tc.insert(ctx, records, tc.row); !errors.Is(err, ErrRecordOutsideTenant) {
				t.Fatalf("write of a %s row routed to %s: got %v, want ErrRecordOutsideTenant", testNS, otherTenant, err)
			}
			if err := tc.replace(ctx, records, tc.row, tc.changed); !errors.Is(err, ErrRecordOutsideTenant) {
				t.Fatalf("replace of a %s row routed to %s: got %v, want ErrRecordOutsideTenant", testNS, otherTenant, err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.rows) != 0 {
				t.Fatalf("the store holds %d rows; a refused write must not reach it", len(fake.rows))
			}
		})
	}
}

func TestRecordsCallsNamingNoTenantAreRefused(t *testing.T) {
	for _, tenant := range []string{"", "ACME0001", "acme", "acme0001/x"} {
		for _, tc := range recordCasesFor(tenant) {
			t.Run(tc.name+"/"+tenant, func(t *testing.T) {
				records, fake := newStratumRecords(t)
				ctx := context.Background()
				if _, err := tc.read(ctx, records); !errors.Is(err, ErrRecordOutsideTenant) {
					t.Errorf("read: got %v, want ErrRecordOutsideTenant", err)
				}
				if err := tc.insert(ctx, records, tc.row); !errors.Is(err, ErrRecordOutsideTenant) {
					t.Errorf("write: got %v, want ErrRecordOutsideTenant", err)
				}
				if _, err := tc.watch(ctx, records); !errors.Is(err, ErrRecordOutsideTenant) {
					t.Errorf("watch: got %v, want ErrRecordOutsideTenant", err)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if len(fake.rows) != 0 {
					t.Errorf("the store holds %d rows", len(fake.rows))
				}
			})
		}
	}
}

func setNamespace(m proto.Message, ns string) {
	switch r := m.(type) {
	case *blobstore_pb.Blob:
		r.Namespace = ns
	case *blobstore_pb.UploadSession:
		r.Namespace = ns
	case *blobstore_pb.BlobLink:
		r.Namespace = ns
	case *blobstore_pb.BlobLinkStats:
		r.Namespace = ns
	}
}

// ---------------------------------------------------------------------------
// The bearer paths
// ---------------------------------------------------------------------------

func TestDownloadResolvesTheTokenOnlyInItsOwnTenant(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLink(t, sess)

	for name, path := range map[string]string{
		"another tenant":    BlobPath(otherTenant, token),
		"no tenant":         PathBlob + token,
		"not a tid":         BlobPath("ACME0001", token),
		"tenant, no token":  PathBlob + testTenant + "/",
		"a further segment": BlobPath(testTenant, token) + "/x",
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := p.doPath(t, method, p.aBytes, path, nil)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s: got %d, want 404", method, name, resp.StatusCode)
			}
		}
	}

	resp := p.doPath(t, http.MethodGet, p.aBytes, BlobPath(testTenant, token), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the token in its own tenant: got %d, want 200", resp.StatusCode)
	}
	if _, bytesServed, _ := p.a.stats.PendingFor(LinkTokenHash(token)); bytesServed != int64(len(pdfBody)) {
		t.Errorf("a refused path must count nothing: bytes_served %d, want one download's %d", bytesServed, len(pdfBody))
	}
}

func TestUploadUnderAnotherTenantIs404AndLeavesTheTokenUnused(t *testing.T) {
	p := newPair(t)
	token, sess := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)

	if got := putPath(t, p.aBytes, UploadPath(otherTenant, token), pdfBody); got != http.StatusNotFound {
		t.Fatalf("PUT under another tenant: got %d, want 404", got)
	}
	if got := putPath(t, p.aBytes, PathUpload+token, pdfBody); got != http.StatusNotFound {
		t.Fatalf("PUT with no tenant: got %d, want 404", got)
	}
	// The peer's sink resolves the token in the tenant of its path too.
	if got := putPath(t, p.bPeer, PeerPathUpload+otherTenant+"/"+token, pdfBody); got != http.StatusNotFound {
		t.Fatalf("peer PUT under another tenant: got %d, want 404", got)
	}
	if got := p.records.sessions[sess.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		t.Fatalf("the session is %v after refused PUTs, want PENDING", got)
	}

	if got := put(t, p.aBytes, token, pdfBody); got != http.StatusCreated {
		t.Fatalf("PUT in its own tenant: got %d, want 201", got)
	}
	if p.records.blobs[blobIDOf(sess)] == nil {
		t.Fatal("no Blob row in the tenant after the upload")
	}
}

// The paths the RPCs hand out are the paths that resolve.
func TestTheHandedOutPathsResolve(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")

	sum := sha256Hex(pdfBody)
	up, err := h.CreateUpload(ctx, caller, &blobstore_pb.CreateUploadRequest{
		Group: testGroup, ContentHash: sum, SizeBytes: int64(len(pdfBody)),
		ContentType: "application/pdf", Namespace: testNS,
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	if got := putPath(t, p.bBytes, up.GetUploadPath(), pdfBody); got != http.StatusCreated {
		t.Fatalf("PUT %s: got %d, want 201", PathUpload+"<tid>/<token>", got)
	}

	link, err := h.CreateLink(ctx, caller, &blobstore_pb.CreateLinkRequest{
		BlobId: up.GetBlobId(), Filename: "x.pdf", Namespace: testNS,
	})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	url, err := h.CreateBlobUrl(ctx, caller, &blobstore_pb.CreateBlobUrlRequest{
		BlobId: up.GetBlobId(), Filename: "x.pdf", TtlSeconds: 60, Namespace: testNS,
	})
	if err != nil {
		t.Fatalf("CreateBlobUrl: %v", err)
	}
	for name, path := range map[string]string{"CreateLink": link.GetLinkPath(), "CreateBlobUrl": url.GetLinkPath()} {
		resp := p.doPath(t, http.MethodGet, p.aBytes, path, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s's link_path: got %d, want 200", name, resp.StatusCode)
		}
	}
}

// ---------------------------------------------------------------------------
// The RPCs
// ---------------------------------------------------------------------------

// A namespace that is no tenant's has no store: the RPC is refused rather than
// sent to whichever store answers.
func TestRPCsRefuseANamespaceThatIsNoTenants(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")
	sess := p.uploadOne(t)

	for _, ns := range []string{"/", "/system", "/tenants/acme", "/tenants/ACME0001"} {
		_, err := h.CreateUpload(ctx, caller, &blobstore_pb.CreateUploadRequest{
			Group: testGroup, ContentHash: strings.Repeat("a", 64), SizeBytes: 1,
			ContentType: "application/pdf", Namespace: ns,
		})
		if codeOf(err) != codes.InvalidArgument {
			t.Errorf("CreateUpload in %q: got %v, want InvalidArgument", ns, err)
		}
		_, err = h.CreateLink(ctx, caller, &blobstore_pb.CreateLinkRequest{
			BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: ns,
		})
		if codeOf(err) != codes.InvalidArgument {
			t.Errorf("CreateLink in %q: got %v, want InvalidArgument", ns, err)
		}
		_, err = h.DeleteBlob(ctx, caller, &blobstore_pb.DeleteBlobRequest{BlobId: blobIDOf(sess), Namespace: ns})
		if codeOf(err) != codes.InvalidArgument {
			t.Errorf("DeleteBlob in %q: got %v, want InvalidArgument", ns, err)
		}
	}
	if p.records.blobs[blobIDOf(sess)].GetDeleted() {
		t.Fatal("a refused DeleteBlob deleted the blob")
	}
}

// A request naming another tenant's namespace reaches that tenant's store,
// which does not hold the row: NOT_FOUND, never the row.
func TestRPCsRoutedToAnotherTenantFindNothing(t *testing.T) {
	p := newPair(t)
	h := NewHandler(p.a)
	ctx := context.Background()
	caller := callerFor("docmanager")
	sess := p.uploadOne(t)

	_, err := h.CreateLink(ctx, caller, &blobstore_pb.CreateLinkRequest{
		BlobId: blobIDOf(sess), Filename: "x.pdf", Namespace: otherNS,
	})
	if codeOf(err) != codes.NotFound {
		t.Errorf("CreateLink for %s's blob in %s: got %v, want NotFound", testTenant, otherTenant, err)
	}
	_, err = h.DeleteBlob(ctx, caller, &blobstore_pb.DeleteBlobRequest{BlobId: blobIDOf(sess), Namespace: otherNS})
	if codeOf(err) != codes.NotFound {
		t.Errorf("DeleteBlob of %s's blob in %s: got %v, want NotFound", testTenant, otherTenant, err)
	}
	if p.records.blobs[blobIDOf(sess)].GetDeleted() {
		t.Fatal("a DeleteBlob routed to another tenant deleted the blob")
	}
}

// ---------------------------------------------------------------------------
// The background loops: per tenant
// ---------------------------------------------------------------------------

func TestSweepSettlesEachTenantsSessionsInItsOwnStore(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	_, ours := p.mintUpload(t, p.a, testNS, "application/pdf", pdfBody)
	_, theirs := p.mintUpload(t, p.a, otherNS, "application/pdf", pdfBody)
	p.nowMs += 16 * 60 * 1000

	// A tenant kernel reports no Stratum for has no store the sweep can reach.
	p.tenants = []string{testTenant}
	p.a.sweepSessions(ctx)
	if got := p.records.sessions[ours.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_ABORTED {
		t.Fatalf("%s's session is %v, want ABORTED", testTenant, got)
	}
	if got := p.records.sessions[theirs.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		t.Fatalf("%s's session is %v before its store is listed, want PENDING", otherTenant, got)
	}

	p.tenants = []string{testTenant, otherTenant}
	p.a.sweepSessions(ctx)
	if got := p.records.sessions[theirs.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_ABORTED {
		t.Fatalf("%s's session is %v, want ABORTED", otherTenant, got)
	}
}

func TestStatsFlushWritesEachTenantsRowToItsOwnStore(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	tokens := map[string]string{}
	for tenant, ns := range map[string]string{testTenant: testNS, otherTenant: otherNS} {
		token, sess := p.mintUpload(t, p.a, ns, "application/pdf", pdfBody)
		if got := putPath(t, p.aBytes, UploadPath(tenant, token), pdfBody); got != http.StatusCreated {
			t.Fatalf("%s's upload: got %d, want 201", tenant, got)
		}
		link := p.mintLink(t, sess)
		resp := p.doPath(t, http.MethodGet, p.aBytes, BlobPath(tenant, link), nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s's download: got %d, want 200", tenant, resp.StatusCode)
		}
		tokens[tenant] = LinkTokenHash(link)
	}

	p.a.stats.Flush(ctx)
	for tenant, tokenHash := range tokens {
		row, err := p.records.FetchLinkStats(ctx, tenant, tokenHash)
		if err != nil || row.GetResolutions() != 1 {
			t.Errorf("%s's stats row: got %v, %v; want one resolution", tenant, row, err)
		}
		if r, _, _ := p.a.stats.PendingFor(tokenHash); r != 0 {
			t.Errorf("%s's delta was not flushed: %d pending", tenant, r)
		}
	}
}

// ---------------------------------------------------------------------------
// The tenant list, from kernel
// ---------------------------------------------------------------------------

func TestStratumTenantsAreTheTenantsKernelReportsAStratumFor(t *testing.T) {
	inst := func(svc, spiffeID string) *healthreport.ServiceInstanceHealth {
		return &healthreport.ServiceInstanceHealth{ServiceId: svc, SpiffeId: spiffeID}
	}
	health := &servicehealth_pb.ServiceHealth{Instances: []*healthreport.ServiceInstanceHealth{
		inst("stratum", "spiffe://example.com/tenants/bbbb0002/services/stratum"),
		inst("stratum", "spiffe://example.com/tenants/aaaa0001/services/stratum"),
		inst("stratum", "spiffe://example.com/tenants/aaaa0001/services/stratum"), // a second member
		inst("stratum", "spiffe://example.com/system/aws/az1/services/stratum"),   // no tenant's
		inst("stratum", "spiffe://example.com/tenants/NOTATID/services/stratum"),
		inst("crew", "spiffe://example.com/tenants/cccc0003/services/crew"), // no Stratum there
	}}
	got := StratumTenants(func() *servicehealth_pb.ServiceHealth { return health })()
	if want := []string{"aaaa0001", "bbbb0002"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got := StratumTenants(func() *servicehealth_pb.ServiceHealth { return nil })(); len(got) != 0 {
		t.Fatalf("no health yet: got %v, want none", got)
	}
}
