package blobstore

import (
	"context"
	"errors"
	"iter"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	query_gc "github.com/tacplus-dev/blobstore/tacplus/s2sm/grpc/clients/vservices/query"
	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	query_pb "github.com/tacplus-dev/blobstore/tacplus/types/vservices/query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// These tests drive recordStore — the real Records — through the GENERATED
// RecordMap client, over gRPC, against fakeStratum: an in-memory stand-in for
// stratum's write surface that answers the way the 2.5 contract says the store
// does (tacplus docs/RECORD_EVENT_MODEL.md, "Write surface, durability and
// Watch"). The service-level tests use fakeRecords instead; these are what show
// the rows reach the store through its write surface and nothing else.

// fakeStratum is stratum's write surface, Get and Watch<T>Changes over a map.
type fakeStratum struct {
	query_pb.UnimplementedRecordMapServer

	mu       sync.Mutex
	rows     map[string]proto.Message // "<record type>/<key>"
	revision int64
	watchers map[string][]chan fakeChange // by record type

	// redoDown is the redo service unreachable: stratum refuses every write
	// with UNAVAILABLE and still serves reads from memory.
	redoDown bool
}

type fakeChange struct {
	kind     query_pb.RecordChangeType
	row      proto.Message
	revision int64
}

type fakeWrite int

const (
	fakePut fakeWrite = iota
	fakeInsertIfAbsent
	fakeReplaceIfUnchanged
)

func newFakeStratum() *fakeStratum {
	return &fakeStratum{rows: map[string]proto.Message{}, watchers: map[string][]chan fakeChange{}}
}

func (f *fakeStratum) get(typ, key string) (proto.Message, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[typ+"/"+key]
	if !ok {
		return nil, 0, status.Errorf(codes.NotFound, "%s %s not found", typ, key)
	}
	return proto.Clone(row), f.revision, nil
}

func (f *fakeStratum) write(typ, key string, op fakeWrite, row, expected proto.Message) (*query_pb.RecordWriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.redoDown {
		return nil, status.Error(codes.Unavailable, "redo is unreachable: writes are refused")
	}
	id := typ + "/" + key
	stored, present := f.rows[id]
	kind := query_pb.RecordChangeType_RECORD_CHANGE_TYPE_MODIFY
	switch op {
	case fakeInsertIfAbsent:
		if present {
			return nil, status.Errorf(codes.AlreadyExists, "%s already exists", id)
		}
		kind = query_pb.RecordChangeType_RECORD_CHANGE_TYPE_ADD
	case fakeReplaceIfUnchanged:
		if !present {
			return nil, status.Errorf(codes.NotFound, "%s not found", id)
		}
		if !proto.Equal(stored, expected) {
			return nil, status.Errorf(codes.Aborted, "%s changed underneath", id)
		}
	case fakePut:
		if !present {
			kind = query_pb.RecordChangeType_RECORD_CHANGE_TYPE_ADD
		}
	}
	f.revision++
	f.rows[id] = proto.Clone(row)
	for _, w := range f.watchers[typ] {
		w <- fakeChange{kind: kind, row: proto.Clone(row), revision: f.revision}
	}
	return &query_pb.RecordWriteResult{Revision: f.revision, Durability: query_pb.RecordDurability_RECORD_DURABILITY_LOCAL}, nil
}

// watch sends the snapshot of one record type, snapshot_done, then every write
// until the caller goes away: the generated WATCH collection shape.
func watchFake[E any](f *fakeStratum, typ string, stream grpc.ServerStreamingServer[E],
	snapshot func(proto.Message) *E, done func(int64) *E, delta func(fakeChange) *E,
) error {
	ch := make(chan fakeChange, 16)
	f.mu.Lock()
	var rows []proto.Message
	for id, row := range f.rows {
		if strings.HasPrefix(id, typ+"/") {
			rows = append(rows, proto.Clone(row))
		}
	}
	revision := f.revision
	f.watchers[typ] = append(f.watchers[typ], ch)
	f.mu.Unlock()

	for _, row := range rows {
		if err := stream.Send(snapshot(row)); err != nil {
			return err
		}
	}
	if err := stream.Send(done(revision)); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case c := <-ch:
			if err := stream.Send(delta(c)); err != nil {
				return err
			}
		}
	}
}

func (f *fakeStratum) GetBlob(_ context.Context, req *query_pb.GetBlobRequest) (*query_pb.GetBlobResponse, error) {
	row, rev, err := f.get("blobstore.Blob", req.GetBlobId())
	if err != nil {
		return nil, err
	}
	return &query_pb.GetBlobResponse{Item: row.(*blobstore_pb.Blob), Revision: rev}, nil
}

func (f *fakeStratum) PutBlob(_ context.Context, req *query_pb.PutBlobRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.Blob", req.GetItem().GetBlobId(), fakePut, req.GetItem(), nil)
}

func (f *fakeStratum) InsertIfAbsentBlob(_ context.Context, req *query_pb.InsertIfAbsentBlobRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.Blob", req.GetItem().GetBlobId(), fakeInsertIfAbsent, req.GetItem(), nil)
}

func (f *fakeStratum) ReplaceIfUnchangedBlob(_ context.Context, req *query_pb.ReplaceIfUnchangedBlobRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.Blob", req.GetItem().GetBlobId(), fakeReplaceIfUnchanged, req.GetItem(), req.GetExpectedItem())
}

func (f *fakeStratum) WatchBlobChanges(_ *query_pb.WatchBlobChangesRequest, stream grpc.ServerStreamingServer[query_pb.BlobWatchEvent]) error {
	return watchFake(f, "blobstore.Blob", stream,
		func(m proto.Message) *query_pb.BlobWatchEvent {
			return &query_pb.BlobWatchEvent{Event: &query_pb.BlobWatchEvent_SnapshotItem{SnapshotItem: m.(*blobstore_pb.Blob)}}
		},
		func(rev int64) *query_pb.BlobWatchEvent {
			return &query_pb.BlobWatchEvent{Event: &query_pb.BlobWatchEvent_SnapshotDone{SnapshotDone: rev}}
		},
		func(c fakeChange) *query_pb.BlobWatchEvent {
			return &query_pb.BlobWatchEvent{Event: &query_pb.BlobWatchEvent_Delta{Delta: &query_pb.BlobWatchChange{
				Type: c.kind, Item: c.row.(*blobstore_pb.Blob), Revision: c.revision,
			}}}
		})
}

func (f *fakeStratum) GetUploadSession(_ context.Context, req *query_pb.GetUploadSessionRequest) (*query_pb.GetUploadSessionResponse, error) {
	row, rev, err := f.get("blobstore.UploadSession", req.GetUploadId())
	if err != nil {
		return nil, err
	}
	return &query_pb.GetUploadSessionResponse{Item: row.(*blobstore_pb.UploadSession), Revision: rev}, nil
}

func (f *fakeStratum) PutUploadSession(_ context.Context, req *query_pb.PutUploadSessionRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.UploadSession", req.GetItem().GetUploadId(), fakePut, req.GetItem(), nil)
}

func (f *fakeStratum) InsertIfAbsentUploadSession(_ context.Context, req *query_pb.InsertIfAbsentUploadSessionRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.UploadSession", req.GetItem().GetUploadId(), fakeInsertIfAbsent, req.GetItem(), nil)
}

func (f *fakeStratum) ReplaceIfUnchangedUploadSession(_ context.Context, req *query_pb.ReplaceIfUnchangedUploadSessionRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.UploadSession", req.GetItem().GetUploadId(), fakeReplaceIfUnchanged, req.GetItem(), req.GetExpectedItem())
}

func (f *fakeStratum) WatchUploadSessionChanges(_ *query_pb.WatchUploadSessionChangesRequest, stream grpc.ServerStreamingServer[query_pb.UploadSessionWatchEvent]) error {
	return watchFake(f, "blobstore.UploadSession", stream,
		func(m proto.Message) *query_pb.UploadSessionWatchEvent {
			return &query_pb.UploadSessionWatchEvent{Event: &query_pb.UploadSessionWatchEvent_SnapshotItem{SnapshotItem: m.(*blobstore_pb.UploadSession)}}
		},
		func(rev int64) *query_pb.UploadSessionWatchEvent {
			return &query_pb.UploadSessionWatchEvent{Event: &query_pb.UploadSessionWatchEvent_SnapshotDone{SnapshotDone: rev}}
		},
		func(c fakeChange) *query_pb.UploadSessionWatchEvent {
			return &query_pb.UploadSessionWatchEvent{Event: &query_pb.UploadSessionWatchEvent_Delta{Delta: &query_pb.UploadSessionWatchChange{
				Type: c.kind, Item: c.row.(*blobstore_pb.UploadSession), Revision: c.revision,
			}}}
		})
}

func (f *fakeStratum) GetBlobLink(_ context.Context, req *query_pb.GetBlobLinkRequest) (*query_pb.GetBlobLinkResponse, error) {
	row, rev, err := f.get("blobstore.BlobLink", req.GetTokenHash())
	if err != nil {
		return nil, err
	}
	return &query_pb.GetBlobLinkResponse{Item: row.(*blobstore_pb.BlobLink), Revision: rev}, nil
}

func (f *fakeStratum) PutBlobLink(_ context.Context, req *query_pb.PutBlobLinkRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLink", req.GetItem().GetTokenHash(), fakePut, req.GetItem(), nil)
}

func (f *fakeStratum) InsertIfAbsentBlobLink(_ context.Context, req *query_pb.InsertIfAbsentBlobLinkRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLink", req.GetItem().GetTokenHash(), fakeInsertIfAbsent, req.GetItem(), nil)
}

func (f *fakeStratum) ReplaceIfUnchangedBlobLink(_ context.Context, req *query_pb.ReplaceIfUnchangedBlobLinkRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLink", req.GetItem().GetTokenHash(), fakeReplaceIfUnchanged, req.GetItem(), req.GetExpectedItem())
}

func (f *fakeStratum) WatchBlobLinkChanges(_ *query_pb.WatchBlobLinkChangesRequest, stream grpc.ServerStreamingServer[query_pb.BlobLinkWatchEvent]) error {
	return watchFake(f, "blobstore.BlobLink", stream,
		func(m proto.Message) *query_pb.BlobLinkWatchEvent {
			return &query_pb.BlobLinkWatchEvent{Event: &query_pb.BlobLinkWatchEvent_SnapshotItem{SnapshotItem: m.(*blobstore_pb.BlobLink)}}
		},
		func(rev int64) *query_pb.BlobLinkWatchEvent {
			return &query_pb.BlobLinkWatchEvent{Event: &query_pb.BlobLinkWatchEvent_SnapshotDone{SnapshotDone: rev}}
		},
		func(c fakeChange) *query_pb.BlobLinkWatchEvent {
			return &query_pb.BlobLinkWatchEvent{Event: &query_pb.BlobLinkWatchEvent_Delta{Delta: &query_pb.BlobLinkWatchChange{
				Type: c.kind, Item: c.row.(*blobstore_pb.BlobLink), Revision: c.revision,
			}}}
		})
}

func (f *fakeStratum) GetBlobLinkStats(_ context.Context, req *query_pb.GetBlobLinkStatsRequest) (*query_pb.GetBlobLinkStatsResponse, error) {
	row, rev, err := f.get("blobstore.BlobLinkStats", req.GetTokenHash())
	if err != nil {
		return nil, err
	}
	return &query_pb.GetBlobLinkStatsResponse{Item: row.(*blobstore_pb.BlobLinkStats), Revision: rev}, nil
}

func (f *fakeStratum) PutBlobLinkStats(_ context.Context, req *query_pb.PutBlobLinkStatsRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLinkStats", req.GetItem().GetTokenHash(), fakePut, req.GetItem(), nil)
}

func (f *fakeStratum) InsertIfAbsentBlobLinkStats(_ context.Context, req *query_pb.InsertIfAbsentBlobLinkStatsRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLinkStats", req.GetItem().GetTokenHash(), fakeInsertIfAbsent, req.GetItem(), nil)
}

func (f *fakeStratum) ReplaceIfUnchangedBlobLinkStats(_ context.Context, req *query_pb.ReplaceIfUnchangedBlobLinkStatsRequest) (*query_pb.RecordWriteResult, error) {
	return f.write("blobstore.BlobLinkStats", req.GetItem().GetTokenHash(), fakeReplaceIfUnchanged, req.GetItem(), req.GetExpectedItem())
}

func (f *fakeStratum) WatchBlobLinkStatsChanges(_ *query_pb.WatchBlobLinkStatsChangesRequest, stream grpc.ServerStreamingServer[query_pb.BlobLinkStatsWatchEvent]) error {
	return watchFake(f, "blobstore.BlobLinkStats", stream,
		func(m proto.Message) *query_pb.BlobLinkStatsWatchEvent {
			return &query_pb.BlobLinkStatsWatchEvent{Event: &query_pb.BlobLinkStatsWatchEvent_SnapshotItem{SnapshotItem: m.(*blobstore_pb.BlobLinkStats)}}
		},
		func(rev int64) *query_pb.BlobLinkStatsWatchEvent {
			return &query_pb.BlobLinkStatsWatchEvent{Event: &query_pb.BlobLinkStatsWatchEvent_SnapshotDone{SnapshotDone: rev}}
		},
		func(c fakeChange) *query_pb.BlobLinkStatsWatchEvent {
			return &query_pb.BlobLinkStatsWatchEvent{Event: &query_pb.BlobLinkStatsWatchEvent_Delta{Delta: &query_pb.BlobLinkStatsWatchChange{
				Type: c.kind, Item: c.row.(*blobstore_pb.BlobLinkStats), Revision: c.revision,
			}}}
		})
}

// waitWatching waits until a watch of typ is registered on the server, so a write
// made after it is a change and not part of the snapshot.
func (f *fakeStratum) waitWatching(t *testing.T, typ string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.watchers[typ])
		f.mu.Unlock()
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no watch of %s reached the store in 5s", typ)
		}
		time.Sleep(time.Millisecond)
	}
}

// newStratumRecords serves fakeStratum over an in-memory listener and returns the
// real recordStore on the generated client that dials it.
func newStratumRecords(t *testing.T) (*recordStore, *fakeStratum) {
	t.Helper()
	fake := newFakeStratum()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	query_pb.RegisterRecordMapServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///stratum",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialling the fake stratum: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := query_gc.NewRecordMapClient(conn, func() (string, error) { return "test-token", nil })
	return NewRecords(client).(*recordStore), fake
}

// recordCase is one record type's slice of the surface: insert one row, read it
// back by key, and the change that a watch of the type delivers.
type recordCase struct {
	name    string
	row     proto.Message
	changed proto.Message // row with one field changed, for the replace
	insert  func(context.Context, *recordStore, proto.Message) error
	replace func(ctx context.Context, r *recordStore, prev, next proto.Message) error
	read    func(context.Context, *recordStore) (proto.Message, error)
	watch   func(context.Context, *recordStore) (iter.Seq2[RecordChange[proto.Message], error], error)
}

// asChanges erases one record type's change feed to proto.Message, so the cases
// below share one assertion.
func asChanges[T any, P interface {
	*T
	proto.Message
}](seq iter.Seq2[RecordChange[T], error], err error) (iter.Seq2[RecordChange[proto.Message], error], error) {
	if err != nil {
		return nil, err
	}
	return func(yield func(RecordChange[proto.Message], error) bool) {
		for c, err := range seq {
			out := RecordChange[proto.Message]{Snapshot: c.Snapshot, Deleted: c.Deleted, Revision: c.Revision}
			if c.Row != nil {
				var row proto.Message = P(c.Row)
				out.Row = &row
			}
			if !yield(out, err) {
				return
			}
		}
	}, nil
}

func recordCases() []recordCase {
	hash := strings.Repeat("ab", 32)
	tokenHash := strings.Repeat("cd", 32)
	blob := &blobstore_pb.Blob{
		BlobId: BlobKey{Group: testGroup, Namespace: testNS, Hash: hash}.String(), Group: testGroup, ContentHash: hash,
		SizeBytes: 9, ContentType: "application/pdf", CreatedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
	}
	deletedBlob := proto.Clone(blob).(*blobstore_pb.Blob)
	deletedBlob.Deleted = true
	session := &blobstore_pb.UploadSession{
		UploadId: tokenHash, Group: testGroup, ContentHash: hash, SizeBytes: 9, ContentType: "application/pdf",
		ReservedBytes: 9, State: blobstore_pb.UploadState_UPLOAD_STATE_PENDING, CreatedMs: 1, ExpiresMs: 2,
		OwnerPrincipal: "docmanager", Namespace: testNS,
	}
	settled := proto.Clone(session).(*blobstore_pb.UploadSession)
	settled.State = blobstore_pb.UploadState_UPLOAD_STATE_COMMITTED
	link := &blobstore_pb.BlobLink{
		TokenHash: tokenHash, BlobId: blob.GetBlobId(), Filename: "report.pdf", Policy: blobstore_pb.LinkPolicy_LINK_POLICY_BEARER,
		CreatedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
	}
	revoked := proto.Clone(link).(*blobstore_pb.BlobLink)
	revoked.RevokedMs = 5
	stats := &blobstore_pb.BlobLinkStats{
		TokenHash: tokenHash, Resolutions: 1, BytesServed: 9, LastResolvedMs: 1, OwnerPrincipal: "docmanager", Namespace: testNS,
	}
	more := proto.Clone(stats).(*blobstore_pb.BlobLinkStats)
	more.Resolutions = 2

	return []recordCase{
		{
			name: "Blob", row: blob, changed: deletedBlob,
			// A Blob row is Put, not inserted: see recordStore.PutBlob.
			insert: func(ctx context.Context, r *recordStore, m proto.Message) error {
				return r.PutBlob(ctx, m.(*blobstore_pb.Blob))
			},
			replace: func(ctx context.Context, r *recordStore, prev, next proto.Message) error {
				return r.ReplaceBlob(ctx, prev.(*blobstore_pb.Blob), next.(*blobstore_pb.Blob))
			},
			read: func(ctx context.Context, r *recordStore) (proto.Message, error) {
				return nilable(r.FetchBlob(ctx, blob.GetBlobId()))
			},
			watch: func(ctx context.Context, r *recordStore) (iter.Seq2[RecordChange[proto.Message], error], error) {
				return asChanges(r.WatchBlobs(ctx))
			},
		},
		{
			name: "UploadSession", row: session, changed: settled,
			insert: func(ctx context.Context, r *recordStore, m proto.Message) error {
				return r.InsertSession(ctx, m.(*blobstore_pb.UploadSession))
			},
			replace: func(ctx context.Context, r *recordStore, prev, next proto.Message) error {
				return r.ReplaceSession(ctx, prev.(*blobstore_pb.UploadSession), next.(*blobstore_pb.UploadSession))
			},
			read: func(ctx context.Context, r *recordStore) (proto.Message, error) {
				return nilable(r.FetchSession(ctx, tokenHash))
			},
			watch: func(ctx context.Context, r *recordStore) (iter.Seq2[RecordChange[proto.Message], error], error) {
				return asChanges(r.WatchSessions(ctx))
			},
		},
		{
			name: "BlobLink", row: link, changed: revoked,
			insert: func(ctx context.Context, r *recordStore, m proto.Message) error {
				return r.InsertLink(ctx, m.(*blobstore_pb.BlobLink))
			},
			replace: func(ctx context.Context, r *recordStore, prev, next proto.Message) error {
				return r.ReplaceLink(ctx, prev.(*blobstore_pb.BlobLink), next.(*blobstore_pb.BlobLink))
			},
			read: func(ctx context.Context, r *recordStore) (proto.Message, error) {
				return nilable(r.FetchLink(ctx, tokenHash))
			},
			watch: func(ctx context.Context, r *recordStore) (iter.Seq2[RecordChange[proto.Message], error], error) {
				return asChanges(r.WatchLinks(ctx))
			},
		},
		{
			name: "BlobLinkStats", row: stats, changed: more,
			insert: func(ctx context.Context, r *recordStore, m proto.Message) error {
				return r.InsertLinkStats(ctx, m.(*blobstore_pb.BlobLinkStats))
			},
			replace: func(ctx context.Context, r *recordStore, prev, next proto.Message) error {
				return r.ReplaceLinkStats(ctx, prev.(*blobstore_pb.BlobLinkStats), next.(*blobstore_pb.BlobLinkStats))
			},
			read: func(ctx context.Context, r *recordStore) (proto.Message, error) {
				return nilable(r.FetchLinkStats(ctx, tokenHash))
			},
			watch: func(ctx context.Context, r *recordStore) (iter.Seq2[RecordChange[proto.Message], error], error) {
				return asChanges(r.WatchLinkStats(ctx))
			},
		},
	}
}

// nilable keeps a typed nil row a nil proto.Message, so "absent" reads as nil.
func nilable[T any, P interface {
	*T
	proto.Message
}](row P, err error) (proto.Message, error) {
	if row == nil {
		return nil, err
	}
	return row, err
}

// nextChange reads the feed's next change, failing the test after a few seconds
// rather than hanging it.
func nextChange(t *testing.T, next func() (RecordChange[proto.Message], error, bool)) RecordChange[proto.Message] {
	t.Helper()
	type result struct {
		c   RecordChange[proto.Message]
		err error
		ok  bool
	}
	got := make(chan result, 1)
	go func() {
		c, err, ok := next()
		got <- result{c, err, ok}
	}()
	select {
	case r := <-got:
		if r.err != nil || !r.ok {
			t.Fatalf("watch ended: ok=%v err=%v", r.ok, r.err)
		}
		return r.c
	case <-time.After(5 * time.Second):
		t.Fatal("watch delivered nothing in 5s")
	}
	return RecordChange[proto.Message]{}
}

func TestRecordsWriteReadAndWatchEachRecordType(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			records, fake := newStratumRecords(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Absent reads as (nil, nil): Get's NOT_FOUND is "no row", not a failure.
			if got, err := tc.read(ctx, records); err != nil || got != nil {
				t.Fatalf("read before the write: got %v, %v; want nil, nil", got, err)
			}

			// The watch is opened before the write, so the write must arrive as a
			// change after an empty snapshot.
			feed, err := tc.watch(ctx, records)
			if err != nil {
				t.Fatalf("opening the watch: %v", err)
			}
			next, stop := iter.Pull2(feed)
			defer stop()
			fake.waitWatching(t, "blobstore."+tc.name)

			if err := tc.insert(ctx, records, tc.row); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, _, err := fake.get("blobstore."+tc.name, keyOf(tc.row)); err != nil {
				t.Fatalf("the write did not reach the store's write surface: %v", err)
			}
			got, err := tc.read(ctx, records)
			if err != nil || !proto.Equal(got, tc.row) {
				t.Fatalf("read after the write: got %v, %v; want %v", got, err, tc.row)
			}

			c := nextChange(t, next)
			if c.Snapshot || c.Deleted || c.Row == nil || !proto.Equal(*c.Row, tc.row) || c.Revision == 0 {
				t.Fatalf("watch delivered %+v; want the written row as a change with its revision", c)
			}

			// A replace against the row as read lands; one against a stale read is
			// refused as changed underneath, and nothing is overwritten.
			if err := tc.replace(ctx, records, tc.row, tc.changed); err != nil {
				t.Fatalf("replace against the stored row: %v", err)
			}
			if c := nextChange(t, next); c.Row == nil || !proto.Equal(*c.Row, tc.changed) {
				t.Fatalf("watch delivered %+v after the replace; want %v", c, tc.changed)
			}
			if err := tc.replace(ctx, records, tc.row, tc.row); !errors.Is(err, ErrRecordChanged) {
				t.Fatalf("replace against a stale read: got %v, want ErrRecordChanged", err)
			}
			if got, _ := tc.read(ctx, records); !proto.Equal(got, tc.changed) {
				t.Fatalf("a refused replace changed the row: got %v, want %v", got, tc.changed)
			}
		})
	}
}

func TestRecordsWatchSnapshotDeliversRowsWrittenBefore(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			records, _ := newStratumRecords(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := tc.insert(ctx, records, tc.row); err != nil {
				t.Fatalf("write: %v", err)
			}
			feed, err := tc.watch(ctx, records)
			if err != nil {
				t.Fatalf("opening the watch: %v", err)
			}
			next, stop := iter.Pull2(feed)
			defer stop()
			c := nextChange(t, next)
			if !c.Snapshot || c.Row == nil || !proto.Equal(*c.Row, tc.row) {
				t.Fatalf("watch delivered %+v; want the stored row as a snapshot item", c)
			}
		})
	}
}

func TestRecordsWritesAreRefusedWhileRedoIsUnreachable(t *testing.T) {
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			records, fake := newStratumRecords(t)
			ctx := context.Background()
			if err := tc.insert(ctx, records, tc.row); err != nil {
				t.Fatalf("write before the outage: %v", err)
			}

			fake.mu.Lock()
			fake.redoDown = true
			fake.mu.Unlock()

			err := tc.replace(ctx, records, tc.row, tc.changed)
			if !errors.Is(err, ErrRecordStoreUnavailable) {
				t.Fatalf("replace while redo is unreachable: got %v, want ErrRecordStoreUnavailable", err)
			}
			if got, err := tc.read(ctx, records); err != nil || !proto.Equal(got, tc.row) {
				t.Fatalf("read while redo is unreachable: got %v, %v; want the stored row", got, err)
			}
		})
	}
}

func TestRecordsInsertOfAnExistingKeyIsRefused(t *testing.T) {
	records, _ := newStratumRecords(t)
	ctx := context.Background()
	for _, tc := range recordCases()[1:] { // Blob is Put, never inserted
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.insert(ctx, records, tc.row); err != nil {
				t.Fatalf("first insert: %v", err)
			}
			if err := tc.insert(ctx, records, tc.changed); !errors.Is(err, ErrRecordExists) {
				t.Fatalf("second insert of the key: got %v, want ErrRecordExists", err)
			}
		})
	}
}

func TestRecordsReplaceOfAnAbsentRowIsNotFound(t *testing.T) {
	records, _ := newStratumRecords(t)
	for _, tc := range recordCases() {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.replace(context.Background(), records, tc.row, tc.changed)
			if !errors.Is(err, ErrRecordNotFound) {
				t.Fatalf("replace of an absent row: got %v, want ErrRecordNotFound", err)
			}
		})
	}
}

// TestRecordStoreAnswersMapToTheirErrors pins the mapping the callers branch on,
// FAILED_PRECONDITION included: stratum answers it for a write owed REMOTE
// durability while the cross-region redo does not exist yet (redo#1 step 2).
// Nothing here declares REMOTE, but a write the store cannot make durable is a
// store that cannot take the write, not a bug in the caller.
func TestRecordStoreAnswersMapToTheirErrors(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want error
	}{
		{codes.AlreadyExists, ErrRecordExists},
		{codes.Aborted, ErrRecordChanged},
		{codes.NotFound, ErrRecordNotFound},
		{codes.Unavailable, ErrRecordStoreUnavailable},
		{codes.FailedPrecondition, ErrRecordStoreUnavailable},
	} {
		err := storeError("writing", status.Error(tc.code, "x"))
		if !errors.Is(err, tc.want) {
			t.Errorf("%v: got %v, want %v", tc.code, err, tc.want)
		}
		if status.Code(err) != tc.code {
			t.Errorf("%v: the gRPC status is lost: %v", tc.code, err)
		}
	}
}

func keyOf(m proto.Message) string {
	switch r := m.(type) {
	case *blobstore_pb.Blob:
		return r.GetBlobId()
	case *blobstore_pb.UploadSession:
		return r.GetUploadId()
	case *blobstore_pb.BlobLink:
		return r.GetTokenHash()
	case *blobstore_pb.BlobLinkStats:
		return r.GetTokenHash()
	}
	return ""
}
