package blobstore

import (
	"context"
	"iter"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tacplus-dev/blobstore/tacplus/chassis/collection"
	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"google.golang.org/protobuf/proto"
)

// These tests hold each RPC handler to the trace propagation rule
// (tacplus-dev/looper#51): every record-plane call a handler makes because of
// its request carries the request's trace_id, each page and the query's close
// included. They drive the handlers over the real recordStore and the generated
// client against fakeStratum, which records the trace_id of every request it
// receives.

const testTrace = "t-4bf92f3577b34da6"

// newTracedHandler is one pod of testTenant's instance over fakeStratum, with a
// peer that answers every DELETE as done.
func newTracedHandler(t *testing.T) (*Handler, *fakeStratum) {
	t.Helper()
	records, fake := newStratumRecords(t)
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(peer.Close)
	mirror, err := NewMirror([]string{"pod-a.127.0.0.1:1", hostOf(t, peer.URL)}, "pod-a", httpClientForPeer())
	if err != nil {
		t.Fatalf("NewMirror: %v", err)
	}
	svc := NewService(store, mirror, records, testTenant, nil)
	if err := svc.Configure(testConfig()); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return NewHandler(svc), fake
}

// tracedRows are the rows the handlers act on: two blobs of the group, two
// links to the first, a pending session and a stats row, all docmanager's.
type tracedRows struct {
	blob    *blobstore_pb.Blob
	link    *blobstore_pb.BlobLink
	session *blobstore_pb.UploadSession
}

func seedTraced(fake *fakeStratum) tracedRows {
	var rows tracedRows
	for i, h := range []string{strings.Repeat("ab", 32), strings.Repeat("ef", 32)} {
		b := &blobstore_pb.Blob{
			BlobId: BlobKey{Group: testGroup, Hash: h}.String(), Group: testGroup, ContentHash: h, SizeBytes: 9,
			ContentType: "application/pdf", CreatedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
		}
		fake.seed(testTenant, "blobstore.Blob", b.GetBlobId(), b)
		if i == 0 {
			rows.blob = b
		}
	}
	for i, th := range []string{strings.Repeat("cd", 32), strings.Repeat("ce", 32)} {
		l := &blobstore_pb.BlobLink{
			TokenHash: th, BlobId: rows.blob.GetBlobId(), Filename: "report.pdf", Policy: blobstore_pb.LinkPolicy_LINK_POLICY_BEARER,
			CreatedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
		}
		fake.seed(testTenant, "blobstore.BlobLink", th, l)
		if i == 0 {
			rows.link = l
		}
	}
	fake.seed(testTenant, "blobstore.BlobLinkStats", rows.link.GetTokenHash(), &blobstore_pb.BlobLinkStats{
		TokenHash: rows.link.GetTokenHash(), Resolutions: 1, BytesServed: 9, LastResolvedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
	})
	rows.session = &blobstore_pb.UploadSession{
		UploadId: strings.Repeat("12", 32), Group: testGroup, ContentHash: strings.Repeat("ab", 32), SizeBytes: 9,
		ContentType: "application/pdf", ReservedBytes: 9, State: blobstore_pb.UploadState_UPLOAD_STATE_PENDING,
		CreatedMs: 1, ExpiresMs: 2, OwnerPrincipal: "docmanager", Namespace: testNS,
	}
	fake.seed(testTenant, "blobstore.UploadSession", rows.session.GetUploadId(), rows.session)
	return rows
}

func TestHandlersCarryTheirTraceToEveryRecordCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(context.Context, *Handler, tracedRows) error
		// want is the record-plane calls the RPC makes, in order.
		want []string
	}{
		{
			name: "CreateUpload",
			call: func(ctx context.Context, h *Handler, _ tracedRows) error {
				_, err := h.CreateUpload(ctx, callerFor("docmanager"), &blobstore_pb.CreateUploadRequest{
					Group: testGroup, ContentHash: strings.Repeat("a", 64), SizeBytes: 9, ContentType: "application/pdf",
					Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			// The quota read is a query of the group's two blobs: two pages, then
			// the close.
			want: []string{"CreateBlobQuery", "ListBlob", "ListBlob", "DeleteBlobQuery", "InsertIfAbsentUploadSession", "GetBlob"},
		},
		{
			name: "CancelUpload",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.CancelUpload(ctx, callerFor("docmanager"), &blobstore_pb.CancelUploadRequest{
					UploadId: rows.session.GetUploadId(), Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetUploadSession", "ReplaceIfUnchangedUploadSession"},
		},
		{
			name: "CreateLink",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.CreateLink(ctx, callerFor("docmanager"), &blobstore_pb.CreateLinkRequest{
					BlobId: rows.blob.GetBlobId(), Filename: "report.pdf", Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetBlob", "InsertIfAbsentBlobLink"},
		},
		{
			name: "CreateBlobUrl",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.CreateBlobUrl(ctx, callerFor("docmanager"), &blobstore_pb.CreateBlobUrlRequest{
					BlobId: rows.blob.GetBlobId(), Filename: "report.pdf", TtlSeconds: 60, Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetBlob", "InsertIfAbsentBlobLink"},
		},
		{
			name: "RevokeLink",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.RevokeLink(ctx, callerFor("docmanager"), &blobstore_pb.RevokeLinkRequest{
					TokenHash: rows.link.GetTokenHash(), Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetBlobLink", "ReplaceIfUnchangedBlobLink"},
		},
		{
			name: "ListLinks",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.ListLinks(ctx, callerFor("docmanager"), &blobstore_pb.ListLinksRequest{
					BlobId: rows.blob.GetBlobId(), Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				}, collection.PageParams{Limit: 10})
				return err
			},
			// The blob's two links: two pages, then the close.
			want: []string{"GetBlob", "CreateBlobLinkQuery", "ListBlobLink", "ListBlobLink", "DeleteBlobLinkQuery"},
		},
		{
			name: "DeleteBlob",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.DeleteBlob(ctx, callerFor("docmanager"), &blobstore_pb.DeleteBlobRequest{
					BlobId: rows.blob.GetBlobId(), Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetBlob", "ReplaceIfUnchangedBlob"},
		},
		{
			name: "GetLinkStats",
			call: func(ctx context.Context, h *Handler, rows tracedRows) error {
				_, err := h.GetLinkStats(ctx, callerFor("docmanager"), &blobstore_pb.GetLinkStatsRequest{
					TokenHash: rows.link.GetTokenHash(), Namespace: testNS, Tenant: testTenant, TraceId: testTrace,
				})
				return err
			},
			want: []string{"GetBlobLink", "GetBlobLinkStats"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, fake := newTracedHandler(t)
			rows := seedTraced(fake)
			if err := tc.call(context.Background(), h, rows); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			calls := fake.takeCalls()
			var methods []string
			for _, c := range calls {
				methods = append(methods, c.method)
				if c.trace != testTrace {
					t.Errorf("%s reached the store with trace_id %q; want the request's %q", c.method, c.trace, testTrace)
				}
			}
			if !reflect.DeepEqual(methods, tc.want) {
				t.Fatalf("record-plane calls: got %v, want %v", methods, tc.want)
			}
		})
	}
}

// The requests the handlers above do not send carry the trace too: the
// sweep's list, each page and its close, and the change feeds.
func TestRecordsCarryTheContextsTraceOnEveryRequest(t *testing.T) {
	records, fake := newStratumRecords(t)
	rows := seedTraced(fake)
	second := proto.Clone(rows.session).(*blobstore_pb.UploadSession)
	second.UploadId = strings.Repeat("34", 32)
	fake.seed(testTenant, "blobstore.UploadSession", second.GetUploadId(), second)

	ctx, cancel := context.WithCancel(withTrace(context.Background(), testTrace))
	defer cancel()
	if _, err := records.ListExpiredSessions(ctx, testTenant, 10); err != nil {
		t.Fatalf("ListExpiredSessions: %v", err)
	}
	feed, err := records.WatchBlobs(ctx, testTenant)
	if err != nil {
		t.Fatalf("WatchBlobs: %v", err)
	}
	next, stop := iter.Pull2(feed)
	defer stop()
	nextChange(t, func() (RecordChange[proto.Message], error, bool) {
		c, err, ok := next()
		return RecordChange[proto.Message]{Snapshot: c.Snapshot}, err, ok
	})

	var methods []string
	for _, c := range fake.takeCalls() {
		methods = append(methods, c.method)
		if c.trace != testTrace {
			t.Errorf("%s reached the store with trace_id %q; want %q", c.method, c.trace, testTrace)
		}
	}
	want := []string{"CreateUploadSessionQuery", "ListUploadSession", "ListUploadSession", "DeleteUploadSessionQuery", "WatchBlobChanges"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("record-plane calls: got %v, want %v", methods, want)
	}
}

// The sweep is caused by no request, so its calls carry no trace.
func TestSweepCarriesNoTrace(t *testing.T) {
	h, fake := newTracedHandler(t)
	seedTraced(fake)
	h.svc.sweepSessions(context.Background())
	calls := fake.takeCalls()
	if len(calls) == 0 {
		t.Fatal("the sweep made no record-plane call")
	}
	for _, c := range calls {
		if c.trace != "" {
			t.Errorf("%s carried trace_id %q; the sweep serves no request", c.method, c.trace)
		}
	}
}
