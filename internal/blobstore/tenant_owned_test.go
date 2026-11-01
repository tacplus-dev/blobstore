package blobstore

import (
	"context"
	"errors"
	"iter"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tacplus-dev/blobstore/tacplus/chassis/collection"
	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// blobstore is tenant-owned (#9): one instance per tenant, in /tenants/<tid>,
// with its own two pods, its own disks and its own Stratum. These tests stand
// up two tenants' instances side by side, each a real pair over its own temp
// directories and its own fake Stratum, and check that nothing of one tenant's
// reaches the other's, and that /system reaches neither.

const (
	otherTenant = "other001"
	otherNS     = "/tenants/" + otherTenant
)

// twoTenants is testTenant's instance and otherTenant's.
func twoTenants(t *testing.T) (ours, theirs *pair) {
	t.Helper()
	return newTenantPair(t, testTenant), newTenantPair(t, otherTenant)
}

// createAndPut runs CreateUpload on an instance as its tenant's docmanager, then
// PUTs body to the path it handed out, at that instance's pod a.
func (p *pair) createAndPut(t *testing.T, body []byte) *blobstore_pb.CreateUploadResponse {
	t.Helper()
	up, err := NewHandler(p.a).CreateUpload(context.Background(), callerIn("docmanager", p.ns()), &blobstore_pb.CreateUploadRequest{
		Group: testGroup, ContentHash: sha256Hex(body), SizeBytes: int64(len(body)),
		ContentType: "application/pdf", Namespace: p.ns(), Tenant: p.tenant,
	})
	if err != nil {
		t.Fatalf("%s: CreateUpload: %v", p.tenant, err)
	}
	if got := putPath(t, p.aBytes, up.GetUploadPath(), body); got != http.StatusCreated {
		t.Fatalf("%s: PUT %s: got %d, want 201", p.tenant, PathUpload+"<tid>/<token>", got)
	}
	return up
}

// onDisk reports whether a blob of body is promoted on both of an instance's
// pods, and on neither: the two answers a pair can give.
func (p *pair) onDisk(t *testing.T, body []byte) (both, neither bool) {
	t.Helper()
	a := p.a.store.Has(testGroup, sha256Hex(body))
	b := p.b.store.Has(testGroup, sha256Hex(body))
	return a && b, !a && !b
}

// ---------------------------------------------------------------------------
// Each tenant's uploads land on its own instance's disks
// ---------------------------------------------------------------------------

func TestEachTenantsUploadsLandOnItsOwnInstancesDisks(t *testing.T) {
	ours, theirs := twoTenants(t)
	shared := pdfBody
	onlyOurs := []byte("%PDF-1.7\nonly the first tenant has this\n")

	// The same bytes in both tenants are two files, one on each instance's
	// disks, and two rows, one in each Stratum (R5).
	upOurs := ours.createAndPut(t, shared)
	upTheirs := theirs.createAndPut(t, shared)
	for _, p := range []*pair{ours, theirs} {
		if both, _ := p.onDisk(t, shared); !both {
			t.Fatalf("%s: the shared bytes are not on both of its pods", p.tenant)
		}
	}
	if upOurs.GetBlobId() != upTheirs.GetBlobId() {
		t.Fatalf("blob ids %q and %q: the id is <group>/<sha256> on every instance", upOurs.GetBlobId(), upTheirs.GetBlobId())
	}
	if ours.records.blobs[upOurs.GetBlobId()].GetNamespace() != testNS ||
		theirs.records.blobs[upTheirs.GetBlobId()].GetNamespace() != otherNS {
		t.Fatal("each tenant's Blob row must be in its own Stratum, in its own namespace")
	}

	// Bytes only one tenant uploaded are on its disks and nowhere else.
	up := ours.createAndPut(t, onlyOurs)
	if both, _ := ours.onDisk(t, onlyOurs); !both {
		t.Fatalf("%s: its own upload is not on both of its pods", testTenant)
	}
	if _, neither := theirs.onDisk(t, onlyOurs); !neither {
		t.Fatalf("%s's upload reached %s's disks", testTenant, otherTenant)
	}
	if theirs.records.blobs[up.GetBlobId()] != nil {
		t.Fatalf("%s's upload wrote a row into %s's Stratum", testTenant, otherTenant)
	}

	// The layout is <data_dir>/<group>/<sha256>: no namespace level.
	want := filepath.Join(ours.a.store.Root(), testGroup, sha256Hex(onlyOurs))
	if st, err := os.Stat(want); err != nil || !st.Mode().IsRegular() {
		t.Fatalf("the promoted file is not at %s: %v", want, err)
	}
}

// ---------------------------------------------------------------------------
// A token used against another tenant's instance or path is a 404
// ---------------------------------------------------------------------------

func TestALinkTokenAtAnotherTenantsInstanceOrPathIs404(t *testing.T) {
	ours, theirs := twoTenants(t)
	up := ours.createAndPut(t, pdfBody)
	link, err := NewHandler(ours.a).CreateLink(context.Background(), callerIn("docmanager", testNS), &blobstore_pb.CreateLinkRequest{
		BlobId: up.GetBlobId(), Filename: "x.pdf", Namespace: testNS, Tenant: testTenant,
	})
	if err != nil {
		t.Fatalf("CreateLink: %v", err)
	}

	for name, at := range map[string]struct {
		srv  *pair
		path string
	}{
		"its path, at the other tenant's instance":           {theirs, BlobPath(testTenant, link.GetToken())},
		"the other tenant's path, at that tenant's instance": {theirs, BlobPath(otherTenant, link.GetToken())},
		"the other tenant's path, at its own instance":       {ours, BlobPath(otherTenant, link.GetToken())},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			resp := at.srv.doPath(t, method, at.srv.aBytes, at.path, nil)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s: got %d, want 404", method, name, resp.StatusCode)
			}
		}
	}

	resp := ours.doPath(t, http.MethodGet, ours.bBytes, link.GetLinkPath(), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the link at its own instance: got %d, want 200", resp.StatusCode)
	}
}

func TestAnUploadTokenAtAnotherTenantsInstanceOrPathIs404(t *testing.T) {
	ours, theirs := twoTenants(t)
	up, err := NewHandler(ours.a).CreateUpload(context.Background(), callerIn("docmanager", testNS), &blobstore_pb.CreateUploadRequest{
		Group: testGroup, ContentHash: pdfHash(), SizeBytes: int64(len(pdfBody)),
		ContentType: "application/pdf", Namespace: testNS, Tenant: testTenant,
	})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	token := up.GetUploadToken()

	for name, at := range map[string]struct {
		srv  string
		path string
	}{
		"its path, at the other tenant's instance":           {theirs.aBytes.URL, UploadPath(testTenant, token)},
		"the other tenant's path, at that tenant's instance": {theirs.aBytes.URL, UploadPath(otherTenant, token)},
		"the other tenant's path, at its own instance":       {ours.aBytes.URL, UploadPath(otherTenant, token)},
		"the other tenant's peer path, at its own peer":      {ours.bPeer.URL, PeerPathUpload + otherTenant + "/" + token},
		"its peer path, at the other tenant's peer":          {theirs.bPeer.URL, PeerPathUpload + testTenant + "/" + token},
	} {
		if got := putURL(t, at.srv+at.path, pdfBody); got != http.StatusNotFound {
			t.Errorf("PUT %s: got %d, want 404", name, got)
		}
	}
	if _, neither := theirs.onDisk(t, pdfBody); !neither {
		t.Fatalf("a refused PUT left bytes on %s's disks", otherTenant)
	}
	if got := ours.records.sessions[up.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		t.Fatalf("the session is %v after refused PUTs, want PENDING: the token is still unused", got)
	}

	if got := putPath(t, ours.aBytes, up.GetUploadPath(), pdfBody); got != http.StatusCreated {
		t.Fatalf("PUT at its own instance: got %d, want 201", got)
	}
}

func TestMalformedBearerPathsAre404(t *testing.T) {
	p := newPair(t)
	sess := p.uploadOne(t)
	token := p.mintLink(t, sess)

	for name, path := range map[string]string{
		"no tenant":         PathBlob + token,
		"not a tid":         BlobPath("ACME0001", token),
		"tenant, no token":  PathBlob + testTenant + "/",
		"a further segment": BlobPath(testTenant, token) + "/x",
	} {
		resp := p.doPath(t, http.MethodGet, p.aBytes, path, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: got %d, want 404", name, resp.StatusCode)
		}
	}
	if got := putPath(t, p.aBytes, PathUpload+token, pdfBody); got != http.StatusNotFound {
		t.Errorf("PUT with no tenant: got %d, want 404", got)
	}
	if _, bytesServed, _ := p.a.stats.PendingFor(LinkTokenHash(token)); bytesServed != 0 {
		t.Errorf("a refused path must count nothing: bytes_served %d", bytesServed)
	}
}

// ---------------------------------------------------------------------------
// A /system caller can't reach a tenant's blobs
// ---------------------------------------------------------------------------

// rpcCalls is every RPC of the surface, against one blob and link of p's tenant,
// as caller, naming namespace and tenant.
func rpcCalls(p *pair, blobID, tokenHash, uploadID string) map[string]func(caller callerT, ns, tenant string) error {
	h := NewHandler(p.a)
	ctx := context.Background()
	return map[string]func(callerT, string, string) error{
		"CreateUpload": func(c callerT, ns, tenant string) error {
			_, err := h.CreateUpload(ctx, c, &blobstore_pb.CreateUploadRequest{Group: testGroup, ContentHash: strings.Repeat("a", 64),
				SizeBytes: 1, ContentType: "application/pdf", Namespace: ns, Tenant: tenant})
			return err
		},
		"CancelUpload": func(c callerT, ns, tenant string) error {
			_, err := h.CancelUpload(ctx, c, &blobstore_pb.CancelUploadRequest{UploadId: uploadID, Namespace: ns, Tenant: tenant})
			return err
		},
		"CreateLink": func(c callerT, ns, tenant string) error {
			_, err := h.CreateLink(ctx, c, &blobstore_pb.CreateLinkRequest{BlobId: blobID, Filename: "x.pdf", Namespace: ns, Tenant: tenant})
			return err
		},
		"CreateBlobUrl": func(c callerT, ns, tenant string) error {
			_, err := h.CreateBlobUrl(ctx, c, &blobstore_pb.CreateBlobUrlRequest{BlobId: blobID, Filename: "x.pdf", TtlSeconds: 60,
				Namespace: ns, Tenant: tenant})
			return err
		},
		"RevokeLink": func(c callerT, ns, tenant string) error {
			_, err := h.RevokeLink(ctx, c, &blobstore_pb.RevokeLinkRequest{TokenHash: tokenHash, Namespace: ns, Tenant: tenant})
			return err
		},
		"ListLinks": func(c callerT, ns, tenant string) error {
			_, err := h.ListLinks(ctx, c, &blobstore_pb.ListLinksRequest{BlobId: blobID, Namespace: ns, Tenant: tenant}, collection.PageParams{Limit: 10})
			return err
		},
		"DeleteBlob": func(c callerT, ns, tenant string) error {
			_, err := h.DeleteBlob(ctx, c, &blobstore_pb.DeleteBlobRequest{BlobId: blobID, Namespace: ns, Tenant: tenant})
			return err
		},
		"GetLinkStats": func(c callerT, ns, tenant string) error {
			_, err := h.GetLinkStats(ctx, c, &blobstore_pb.GetLinkStatsRequest{TokenHash: tokenHash, Namespace: ns, Tenant: tenant})
			return err
		},
	}
}

// tenantFixture is one blob, one live link to it and one pending upload in p's
// tenant, for the RPC refusals to aim at.
func tenantFixture(t *testing.T, p *pair) (blobID, tokenHash, uploadID string) {
	t.Helper()
	sess := p.uploadOne(t)
	token := p.mintLink(t, sess)
	_, pending := p.mintUpload(t, p.a, p.ns(), "application/pdf", []byte("%PDF-1.7\npending\n"))
	return blobIDOf(sess), LinkTokenHash(token), pending.GetUploadId()
}

// assertUntouched fails if a refused call changed any row of p's Stratum or
// served any byte.
func assertUntouched(t *testing.T, p *pair, before map[string]proto.Message) {
	t.Helper()
	after := p.records.snapshot()
	if len(after) != len(before) {
		t.Fatalf("a refused call changed the store: %d rows before, %d after", len(before), len(after))
	}
	for k, row := range before {
		if !proto.Equal(row, after[k]) {
			t.Fatalf("a refused call changed %s: %v -> %v", k, row, after[k])
		}
	}
}

func TestASystemCallerCannotReachATenantsBlobs(t *testing.T) {
	p := newPair(t)
	blobID, tokenHash, uploadID := tenantFixture(t, p)
	before := p.records.snapshot()

	// Whatever a caller outside the tenant names, it is refused before a row is
	// read: /system (a platform service), the root, and another tenant's
	// service. The generated door lets /system in; this instance does not.
	for _, callerNS := range []string{"/system", "/system/aws/az1", "/", otherNS} {
		caller := callerIn("docmanager", callerNS)
		for name, call := range rpcCalls(p, blobID, tokenHash, uploadID) {
			for _, ns := range []string{testNS, callerNS} {
				if err := call(caller, ns, testTenant); codeOf(err) != codes.PermissionDenied {
					t.Errorf("%s from %s naming %s: got %v, want PermissionDenied", name, callerNS, ns, err)
				}
			}
		}
	}
	assertUntouched(t, p, before)
}

func TestRPCsRoutedToAnotherTenantAreRefused(t *testing.T) {
	p := newPair(t)
	blobID, tokenHash, uploadID := tenantFixture(t, p)
	before := p.records.snapshot()
	caller := callerIn("docmanager", testNS)

	for name, call := range rpcCalls(p, blobID, tokenHash, uploadID) {
		// A request whose route field names another tenant is that tenant's: the
		// generated server refuses it at the door, and so does the handler.
		if err := call(caller, testNS, otherTenant); codeOf(err) != codes.PermissionDenied {
			t.Errorf("%s routed to %s: got %v, want PermissionDenied", name, otherTenant, err)
		}
		// A namespace that is not this tenant's own has no rows here.
		for _, ns := range []string{"/", "/system", otherNS, testNS + "/sub", "/tenants/ACME0001"} {
			if err := call(caller, ns, testTenant); codeOf(err) != codes.InvalidArgument {
				t.Errorf("%s naming %s: got %v, want InvalidArgument", name, ns, err)
			}
		}
	}
	assertUntouched(t, p, before)
}

// ---------------------------------------------------------------------------
// Quota is enforced per tenant
// ---------------------------------------------------------------------------

func TestQuotaIsEnforcedPerTenant(t *testing.T) {
	ours, theirs := twoTenants(t)
	ctx := context.Background()
	reserve := func(p *pair, size int64) error {
		_, err := NewHandler(p.a).CreateUpload(ctx, callerIn("docmanager", p.ns()), &blobstore_pb.CreateUploadRequest{
			Group: testGroup, ContentHash: strings.Repeat("b", 64), SizeBytes: size,
			ContentType: "application/pdf", Namespace: p.ns(), Tenant: p.tenant,
		})
		return err
	}

	// testConfig's group quota is 4096. One promoted blob and three full
	// reservations leave less than 1024 in our tenant, and nothing of it is
	// spent in theirs.
	ours.createAndPut(t, pdfBody)
	for i := 0; i < 3; i++ {
		if err := reserve(ours, 1024); err != nil {
			t.Fatalf("%s: reservation %d: %v", testTenant, i, err)
		}
	}
	room := 4096 - 3*1024 - int64(len(pdfBody))
	if err := reserve(ours, room+1); codeOf(err) != codes.ResourceExhausted {
		t.Fatalf("%s: a reservation past its quota: got %v, want ResourceExhausted", testTenant, err)
	}
	if err := reserve(ours, room); err != nil {
		t.Fatalf("%s: a reservation of exactly what is left: %v", testTenant, err)
	}

	for i := 0; i < 4; i++ {
		if err := reserve(theirs, 1024); err != nil {
			t.Fatalf("%s: reservation %d: %v; the other tenant's usage must not count", otherTenant, i, err)
		}
	}
	if err := reserve(theirs, 1); codeOf(err) != codes.ResourceExhausted {
		t.Fatalf("%s: a reservation past its own quota: got %v, want ResourceExhausted", otherTenant, err)
	}
}

// ---------------------------------------------------------------------------
// The background loops stay in the instance's tenant
// ---------------------------------------------------------------------------

func TestSweepAndStatsFlushStayInTheInstancesTenant(t *testing.T) {
	ours, theirs := twoTenants(t)
	ctx := context.Background()
	_, pending := ours.mintUpload(t, ours.a, testNS, "application/pdf", pdfBody)
	sess := ours.uploadOne(t)
	link := ours.mintLink(t, sess)
	resp := ours.get(t, ours.aBytes, link)
	resp.Body.Close()
	ours.nowMs += 16 * 60 * 1000

	ours.a.sweepSessions(ctx)
	ours.a.stats.Flush(ctx)
	if got := ours.records.sessions[pending.GetUploadId()].GetState(); got != blobstore_pb.UploadState_UPLOAD_STATE_ABORTED {
		t.Fatalf("the expired session is %v, want ABORTED", got)
	}
	if row := ours.records.stats[LinkTokenHash(link)]; row.GetResolutions() != 1 {
		t.Fatalf("the stats row is %v, want one resolution", row)
	}
	if n := len(theirs.records.snapshot()); n != 0 {
		t.Fatalf("%s's loops wrote %d rows into %s's Stratum", testTenant, n, otherTenant)
	}
	if got := ours.records.routedOutside(testTenant); len(got) != 0 {
		t.Fatalf("%s's instance made record calls routed to %v", testTenant, got)
	}
}

// ---------------------------------------------------------------------------
// recordStore, over the generated client: every call carries target_tenant
// ---------------------------------------------------------------------------

func TestRecordsRouteEveryCallToItsTenant(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			records, fake := newStratumRecords(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := tc.insert(ctx, records, tc.row); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := tc.read(ctx, records); err != nil {
				t.Fatalf("read: %v", err)
			}
			if err := tc.replace(ctx, records, tc.row, tc.changed); err != nil {
				t.Fatalf("replace: %v", err)
			}
			feed, err := tc.watch(ctx, records)
			if err != nil {
				t.Fatalf("watch: %v", err)
			}
			next, stop := iter.Pull2(feed)
			defer stop()
			nextChange(t, next)

			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.routed) != 4 {
				t.Fatalf("the store saw %d calls, want 4: %v", len(fake.routed), fake.routed)
			}
			for _, tid := range fake.routed {
				if tid != testTenant {
					t.Fatalf("a call was routed to %q, want %s: %v", tid, testTenant, fake.routed)
				}
			}
		})
	}
}

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
			if len(fake.rows) != 0 || len(fake.routed) != 0 {
				t.Fatalf("a refused write reached the store: %d rows, calls %v", len(fake.rows), fake.routed)
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
				if len(fake.rows) != 0 || len(fake.routed) != 0 {
					t.Errorf("the store saw %d rows, calls %v", len(fake.rows), fake.routed)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// The instance's tenant, from its own identity
// ---------------------------------------------------------------------------

// An instance serves the tenant of exactly /tenants/<tid>; anywhere else (in
// /system above all) it has no tenant and must not start.
func TestInstanceTenantIsOnlyATenantsOwnNamespace(t *testing.T) {
	if tid, err := InstanceTenant(testNS); err != nil || tid != testTenant {
		t.Fatalf("InstanceTenant(%q) = %q, %v; want %q", testNS, tid, err, testTenant)
	}
	for _, ns := range []string{"", "/", "/system", "/system/aws/az1", "/tenants", testNS + "/sub", "/tenants/ACME0001", "/tenants/acme"} {
		if tid, err := InstanceTenant(ns); err == nil {
			t.Errorf("InstanceTenant(%q) = %q; want a refusal", ns, tid)
		}
	}
}
