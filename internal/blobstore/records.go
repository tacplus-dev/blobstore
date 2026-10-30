package blobstore

import (
	"context"
	"errors"
	"fmt"
	"iter"

	query_gc "github.com/tacplus-dev/blobstore/tacplus/s2sm/grpc/clients/vservices/query"
	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	query_pb "github.com/tacplus-dev/blobstore/tacplus/types/vservices/query"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Records is this service's whole relationship with stratum: write its own rows
// through stratum's generated write surface, and read them back through the same
// plane (Get<T> by key, the query plane for lists).
//
// A WRITE IS FINAL AND SYNCHRONOUS (tacplus 2.5, #6). The store answers once the
// write is durable at the type's declared level (LOCAL, the default: on the redo
// service's disk), or refuses it with the reason, and the reasons are errors the
// callers branch on: ErrRecordExists, ErrRecordChanged, ErrRecordNotFound and
// ErrRecordStoreUnavailable. Nothing is published on a record's topic any more:
// stratum declares no_bus_ingest, so a published record would reach no store.
//
// A ROW READ AND THEN CHANGED IS REPLACED, NOT PUT. Every update here is
// Replace<T>(prev, next): the store writes next only if the row is still prev,
// so two pods (or a request and the sweep) settling the same row cannot both
// win, and the loser learns it from ErrRecordChanged instead of overwriting.
//
// FAIL-CLOSED (R11). Every method here returns its error, and every caller turns
// a read or write failure into 503 rather than proceeding. A blob whose Blob row
// did not reach stratum is bytes nobody can find, which is worse than a refused
// upload.
type Records interface {
	FetchBlob(ctx context.Context, blobID string) (*blobstore_pb.Blob, error)
	// ListBlobsInGroup returns every non-deleted blob for one tenant in one
	// group. It is the quota reader; see Quota.
	ListBlobsInGroup(ctx context.Context, group, namespace string) ([]*blobstore_pb.Blob, error)
	FetchLink(ctx context.Context, tokenHash string) (*blobstore_pb.BlobLink, error)
	ListLinksForBlob(ctx context.Context, blobID, namespace string) ([]*blobstore_pb.BlobLink, error)
	FetchSession(ctx context.Context, uploadID string) (*blobstore_pb.UploadSession, error)
	// ListExpiredSessions returns PENDING-looking sessions whose expiry has
	// passed, for the sweep to settle.
	ListExpiredSessions(ctx context.Context, nowMs int64) ([]*blobstore_pb.UploadSession, error)
	FetchLinkStats(ctx context.Context, tokenHash string) (*blobstore_pb.BlobLinkStats, error)

	// PutBlob writes a promoted blob's row whatever is stored under its key. A
	// Blob is content-addressed: a second upload of the same bytes by the same
	// tenant into the same group IS the same row, and so is a re-upload of a
	// deleted blob, which must bring it back.
	PutBlob(ctx context.Context, rec *blobstore_pb.Blob) error
	ReplaceBlob(ctx context.Context, prev, next *blobstore_pb.Blob) error
	InsertSession(ctx context.Context, rec *blobstore_pb.UploadSession) error
	ReplaceSession(ctx context.Context, prev, next *blobstore_pb.UploadSession) error
	InsertLink(ctx context.Context, rec *blobstore_pb.BlobLink) error
	ReplaceLink(ctx context.Context, prev, next *blobstore_pb.BlobLink) error
	InsertLinkStats(ctx context.Context, rec *blobstore_pb.BlobLinkStats) error
	ReplaceLinkStats(ctx context.Context, prev, next *blobstore_pb.BlobLinkStats) error
}

// The store's refusals, as the callers branch on them. Each wraps the gRPC
// status it came from, so the code and the store's message survive.
var (
	// ErrRecordExists is ALREADY_EXISTS: an insert of a key that is stored.
	ErrRecordExists = errors.New("record already exists")
	// ErrRecordChanged is ABORTED: a replace whose row changed since it was read.
	ErrRecordChanged = errors.New("record changed since it was read")
	// ErrRecordNotFound is NOT_FOUND: a replace of a row that is not stored.
	ErrRecordNotFound = errors.New("record not found")
	// ErrRecordStoreUnavailable is a store that cannot take the write now:
	// UNAVAILABLE (stratum, or the redo it writes through, is unreachable) and
	// FAILED_PRECONDITION (the declared durability cannot be reached yet: REMOTE
	// until redo#1 step 2).
	ErrRecordStoreUnavailable = errors.New("record store cannot take the write")
)

// storeError names what a store call was doing and maps its answer onto the
// errors above.
func storeError(what string, err error) error {
	var kind error
	switch status.Code(err) {
	case codes.AlreadyExists:
		kind = ErrRecordExists
	case codes.Aborted:
		kind = ErrRecordChanged
	case codes.NotFound:
		kind = ErrRecordNotFound
	case codes.Unavailable, codes.FailedPrecondition:
		kind = ErrRecordStoreUnavailable
	default:
		return fmt.Errorf("%s: %w", what, err)
	}
	return fmt.Errorf("%s: %w: %w", what, kind, err)
}

// recordPageSize is the page the store is asked for on a multi-row read. It is
// the same 100 ListLinks declares as its max_items, so a link listing is one
// page in the common case.
const recordPageSize = 100

// recordStore is Records over the generated plane. Nothing here speaks to
// stratum directly.
type recordStore struct {
	records *query_gc.RecordMapClient
}

// NewRecords wires Records over the generated record plane client.
func NewRecords(records *query_gc.RecordMapClient) Records {
	return &recordStore{records: records}
}

// FetchBlob reads one blob row by key with Get<T>. A missing row is (nil, nil) —
// the callers all turn that into 404 or a not-found RPC error, and none of them
// wants an error value they have to inspect to tell "absent" from "broken".
//
// DELETED ROWS ARE RETURNED. A deleted blob must resolve to "gone", not to
// "never existed", on the paths that care (DeleteBlob's idempotency, and
// refusing a link to a deleted blob), so the row is read and the flag checked:
// Get<T> answers by key whatever the deleted flag says.
//
// The namespace beside the key is left empty: blobstore is a /system peer, whose
// reads are fleet-wide, and the byte paths that read by token do not know the
// tenant until they hold the row.
func (r *recordStore) FetchBlob(ctx context.Context, blobID string) (*blobstore_pb.Blob, error) {
	resp, err := r.records.GetBlob(ctx, &query_pb.GetBlobRequest{BlobId: blobID})
	return absentIsNil(resp.GetItem(), err, "reading Blob")
}

func (r *recordStore) ListBlobsInGroup(ctx context.Context, group, namespace string) ([]*blobstore_pb.Blob, error) {
	handle, err := r.records.CreateBlobQuery(ctx, &query_pb.OpenBlobQueryRequest{
		Filter: &query_pb.BlobFilter{Group: &group, Namespace: &namespace},
	})
	if err != nil {
		return nil, fmt.Errorf("opening Blob query: %w", err)
	}
	defer r.closeQuery("Blob", handle, r.records.DeleteBlobQuery)
	return drain(ctx, handle.GetQueryId(), r.records.ListBlobPage, func(queryID, token string) *query_pb.PullBlobRequest {
		return &query_pb.PullBlobRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
}

func (r *recordStore) FetchLink(ctx context.Context, tokenHash string) (*blobstore_pb.BlobLink, error) {
	resp, err := r.records.GetBlobLink(ctx, &query_pb.GetBlobLinkRequest{TokenHash: tokenHash})
	// The key is a hash of a credential, so it is never put in an error.
	return absentIsNil(resp.GetItem(), err, "reading BlobLink")
}

func (r *recordStore) ListLinksForBlob(ctx context.Context, blobID, namespace string) ([]*blobstore_pb.BlobLink, error) {
	handle, err := r.records.CreateBlobLinkQuery(ctx, &query_pb.OpenBlobLinkQueryRequest{
		Filter: &query_pb.BlobLinkFilter{BlobId: &blobID, Namespace: &namespace},
	})
	if err != nil {
		return nil, fmt.Errorf("opening BlobLink query: %w", err)
	}
	defer r.closeQuery("BlobLink", handle, r.records.DeleteBlobLinkQuery)
	return drain(ctx, handle.GetQueryId(), r.records.ListBlobLinkPage, func(queryID, token string) *query_pb.PullBlobLinkRequest {
		return &query_pb.PullBlobLinkRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
}

func (r *recordStore) FetchSession(ctx context.Context, uploadID string) (*blobstore_pb.UploadSession, error) {
	resp, err := r.records.GetUploadSession(ctx, &query_pb.GetUploadSessionRequest{UploadId: uploadID})
	return absentIsNil(resp.GetItem(), err, "reading UploadSession")
}

// ListExpiredSessions uses the record's RANGE filter on expires_ms rather than
// reading every session and filtering here: the predicate belongs in the store,
// and the sweep runs on a timer against a table that only grows.
func (r *recordStore) ListExpiredSessions(ctx context.Context, nowMs int64) ([]*blobstore_pb.UploadSession, error) {
	from := int64(1)
	handle, err := r.records.CreateUploadSessionQuery(ctx, &query_pb.OpenUploadSessionQueryRequest{
		Filter: &query_pb.UploadSessionFilter{ExpiresMsFrom: &from, ExpiresMsTo: &nowMs},
	})
	if err != nil {
		return nil, fmt.Errorf("opening UploadSession query: %w", err)
	}
	defer r.closeQuery("UploadSession", handle, r.records.DeleteUploadSessionQuery)
	return drain(ctx, handle.GetQueryId(), r.records.ListUploadSessionPage, func(queryID, token string) *query_pb.PullUploadSessionRequest {
		return &query_pb.PullUploadSessionRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
}

func (r *recordStore) FetchLinkStats(ctx context.Context, tokenHash string) (*blobstore_pb.BlobLinkStats, error) {
	resp, err := r.records.GetBlobLinkStats(ctx, &query_pb.GetBlobLinkStatsRequest{TokenHash: tokenHash})
	return absentIsNil(resp.GetItem(), err, "reading BlobLinkStats")
}

// absentIsNil turns Get<T>'s answer into the (row, error) the callers want:
// NOT_FOUND is no row, not a failure.
func absentIsNil[T any](row *T, err error, what string) (*T, error) {
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(what, err)
	}
	if row == nil {
		return nil, fmt.Errorf("%s: the store answered with no row", what)
	}
	return row, nil
}

// The writes. No durability is asked for: the generated wrapper writes at the
// type's declared floor, and asking for a level here would be a second place to
// state it.

func (r *recordStore) PutBlob(ctx context.Context, rec *blobstore_pb.Blob) error {
	_, err := r.records.PutBlob(ctx, &query_pb.PutBlobRequest{Item: rec})
	return writeError("writing Blob "+rec.GetBlobId(), err)
}

func (r *recordStore) ReplaceBlob(ctx context.Context, prev, next *blobstore_pb.Blob) error {
	_, err := r.records.ReplaceIfUnchangedBlob(ctx, &query_pb.ReplaceIfUnchangedBlobRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedBlobRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing Blob "+next.GetBlobId(), err)
}

func (r *recordStore) InsertSession(ctx context.Context, rec *blobstore_pb.UploadSession) error {
	_, err := r.records.InsertIfAbsentUploadSession(ctx, &query_pb.InsertIfAbsentUploadSessionRequest{Item: rec})
	return writeError("inserting UploadSession "+rec.GetUploadId(), err)
}

func (r *recordStore) ReplaceSession(ctx context.Context, prev, next *blobstore_pb.UploadSession) error {
	_, err := r.records.ReplaceIfUnchangedUploadSession(ctx, &query_pb.ReplaceIfUnchangedUploadSessionRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedUploadSessionRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing UploadSession "+next.GetUploadId(), err)
}

func (r *recordStore) InsertLink(ctx context.Context, rec *blobstore_pb.BlobLink) error {
	_, err := r.records.InsertIfAbsentBlobLink(ctx, &query_pb.InsertIfAbsentBlobLinkRequest{Item: rec})
	return writeError("inserting BlobLink for "+rec.GetBlobId(), err)
}

func (r *recordStore) ReplaceLink(ctx context.Context, prev, next *blobstore_pb.BlobLink) error {
	_, err := r.records.ReplaceIfUnchangedBlobLink(ctx, &query_pb.ReplaceIfUnchangedBlobLinkRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedBlobLinkRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing BlobLink for "+next.GetBlobId(), err)
}

func (r *recordStore) InsertLinkStats(ctx context.Context, rec *blobstore_pb.BlobLinkStats) error {
	_, err := r.records.InsertIfAbsentBlobLinkStats(ctx, &query_pb.InsertIfAbsentBlobLinkStatsRequest{Item: rec})
	return writeError("inserting BlobLinkStats", err)
}

func (r *recordStore) ReplaceLinkStats(ctx context.Context, prev, next *blobstore_pb.BlobLinkStats) error {
	_, err := r.records.ReplaceIfUnchangedBlobLinkStats(ctx, &query_pb.ReplaceIfUnchangedBlobLinkStatsRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedBlobLinkStatsRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing BlobLinkStats", err)
}

func writeError(what string, err error) error {
	if err == nil {
		return nil
	}
	return storeError(what, err)
}

// RecordChange is one item of a record type's change feed: a row of the
// snapshot the feed starts with, or a write the store acked after it.
type RecordChange[T any] struct {
	Row *T
	// Snapshot is a row that was stored when the watch opened.
	Snapshot bool
	// Deleted is a row the store removed (Delete<T>), not one flagged deleted.
	Deleted bool
	// Revision is the store's revision of a change; 0 on a snapshot row.
	Revision int64
}

// The change feeds (Watch<T>Changes): every row of the type, then every acked
// write, until ctx ends or the stream fails. This is how blobstore learns of a
// change it did not make itself, rather than from a record topic. Nothing in the
// service needs one yet: every path reads the row it acts on when it acts.

func (r *recordStore) WatchBlobs(ctx context.Context) (iter.Seq2[RecordChange[blobstore_pb.Blob], error], error) {
	return changes(r.records.WatchBlobChanges(ctx, &query_pb.WatchBlobChangesRequest{}))
}

func (r *recordStore) WatchSessions(ctx context.Context) (iter.Seq2[RecordChange[blobstore_pb.UploadSession], error], error) {
	return changes(r.records.WatchUploadSessionChanges(ctx, &query_pb.WatchUploadSessionChangesRequest{}))
}

func (r *recordStore) WatchLinks(ctx context.Context) (iter.Seq2[RecordChange[blobstore_pb.BlobLink], error], error) {
	return changes(r.records.WatchBlobLinkChanges(ctx, &query_pb.WatchBlobLinkChangesRequest{}))
}

func (r *recordStore) WatchLinkStats(ctx context.Context) (iter.Seq2[RecordChange[blobstore_pb.BlobLinkStats], error], error) {
	return changes(r.records.WatchBlobLinkStatsChanges(ctx, &query_pb.WatchBlobLinkStatsChangesRequest{}))
}

// watchEvent and watchDelta are the generated <T>WatchEvent and <T>WatchChange
// of any record type, as changes reads them.
type watchEvent[T any, D watchDelta[T]] interface {
	GetSnapshotItem() *T
	GetDelta() D
}

type watchDelta[T any] interface {
	GetType() query_pb.RecordChangeType
	GetItem() *T
	GetRevision() int64
}

// changes turns one generated watch stream into RecordChanges. snapshot_done
// carries no row and is not passed on.
func changes[T any, D watchDelta[T], E watchEvent[T, D]](events iter.Seq2[E, error], err error) (iter.Seq2[RecordChange[T], error], error) {
	if err != nil {
		return nil, storeError("opening watch", err)
	}
	return func(yield func(RecordChange[T], error) bool) {
		for ev, err := range events {
			if err != nil {
				yield(RecordChange[T]{}, storeError("watching", err))
				return
			}
			var c RecordChange[T]
			if row := ev.GetSnapshotItem(); row != nil {
				c = RecordChange[T]{Row: row, Snapshot: true}
			} else if d := ev.GetDelta(); d.GetItem() != nil {
				c = RecordChange[T]{
					Row:      d.GetItem(),
					Deleted:  d.GetType() == query_pb.RecordChangeType_RECORD_CHANGE_TYPE_DELETE,
					Revision: d.GetRevision(),
				}
			} else {
				continue
			}
			if !yield(c, nil) {
				return
			}
		}
	}, nil
}

// closeQuery closes a standing query on every path, success included: a query
// left open is a predicate stratum keeps evaluating for a caller that has gone
// away.
func (r *recordStore) closeQuery(
	kind string,
	handle *query_pb.QueryHandle,
	del func(context.Context, *query_pb.QueryHandle, ...grpc.CallOption) (*query_pb.QueryStats, error),
) {
	// A fresh context: the caller's may already be cancelled, and the close has
	// to happen anyway.
	if _, err := del(context.WithoutCancel(context.Background()), &query_pb.QueryHandle{QueryId: handle.GetQueryId()}); err != nil {
		zap.L().Warn("failed to close record query", zap.String("record", kind), zap.Error(err))
	}
}

// drain pulls every page of a query. The page token is what stops it being a
// read of the first hundred rows that silently looks complete.
func drain[T any, R any](
	ctx context.Context,
	queryID string,
	page func(context.Context, R, ...grpc.CallOption) ([]*T, string, int64, error),
	req func(queryID, pageToken string) R,
) ([]*T, error) {
	var out []*T
	token := ""
	for {
		rows, next, _, err := page(ctx, req(queryID, token))
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if next == "" {
			return out, nil
		}
		token = next
	}
}
