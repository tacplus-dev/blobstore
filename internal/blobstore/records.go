package blobstore

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/tacplus-dev/blobstore/tacplus/chassis/serviceconnection"
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
//
// EVERY CALL NAMES ITS TENANT (#8). Stratum is tenant_owned: each tenant has its
// own instance, and a row lives in the instance of the tenant its namespace
// belongs to (/tenants/<tid>). tenant is that tid, and it is the only thing that
// decides which instance a call reaches. The RPCs take it from the request's
// namespace, the byte paths from the path (/b/<tid>/<token>), and the sweep from
// the tenants kernel reports a Stratum for.
type Records interface {
	FetchBlob(ctx context.Context, tenant, blobID string) (*blobstore_pb.Blob, error)
	// ListBlobsInGroup returns every non-deleted blob for one tenant in one
	// group. It is the quota reader; see Quota.
	ListBlobsInGroup(ctx context.Context, tenant, group, namespace string) ([]*blobstore_pb.Blob, error)
	FetchLink(ctx context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLink, error)
	ListLinksForBlob(ctx context.Context, tenant, blobID, namespace string) ([]*blobstore_pb.BlobLink, error)
	FetchSession(ctx context.Context, tenant, uploadID string) (*blobstore_pb.UploadSession, error)
	// ListExpiredSessions returns one tenant's PENDING-looking sessions whose
	// expiry has passed, for the sweep to settle.
	ListExpiredSessions(ctx context.Context, tenant string, nowMs int64) ([]*blobstore_pb.UploadSession, error)
	FetchLinkStats(ctx context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLinkStats, error)

	// PutBlob writes a promoted blob's row whatever is stored under its key. A
	// Blob is content-addressed: a second upload of the same bytes by the same
	// tenant into the same group IS the same row, and so is a re-upload of a
	// deleted blob, which must bring it back.
	PutBlob(ctx context.Context, tenant string, rec *blobstore_pb.Blob) error
	ReplaceBlob(ctx context.Context, tenant string, prev, next *blobstore_pb.Blob) error
	InsertSession(ctx context.Context, tenant string, rec *blobstore_pb.UploadSession) error
	ReplaceSession(ctx context.Context, tenant string, prev, next *blobstore_pb.UploadSession) error
	InsertLink(ctx context.Context, tenant string, rec *blobstore_pb.BlobLink) error
	ReplaceLink(ctx context.Context, tenant string, prev, next *blobstore_pb.BlobLink) error
	InsertLinkStats(ctx context.Context, tenant string, rec *blobstore_pb.BlobLinkStats) error
	ReplaceLinkStats(ctx context.Context, tenant string, prev, next *blobstore_pb.BlobLinkStats) error
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
	// ErrRecordOutsideTenant is a call that names no tenant, or a row whose
	// namespace is not in /tenants/<tenant> of the call that writes it. It is
	// refused before it is sent, and the store refuses it too
	// (PERMISSION_DENIED): one tenant's row never reaches another's Stratum.
	ErrRecordOutsideTenant = errors.New("record is outside the tenant the call is routed to")
)

// NamespaceTenant returns the tenant a namespace belongs to: the tid of
// /tenants/<tid>, or of any namespace below it. Any other namespace (/system,
// "/") belongs to no tenant, and blobstore keeps no row there: there is no
// Stratum to hold it.
func NamespaceTenant(namespace string) (string, bool) {
	return serviceconnection.NamespaceTenant(namespace)
}

// ValidTenantID reports whether s is a tid: 8 characters of [0-9a-z].
func ValidTenantID(s string) bool { return serviceconnection.ValidTenantID(s) }

// inTenant reports whether namespace is tenant's.
func inTenant(tenant, namespace string) bool {
	tid, ok := NamespaceTenant(namespace)
	return ok && tid == tenant
}

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
	case codes.PermissionDenied:
		kind = ErrRecordOutsideTenant
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
//
// THE TENANT IS CHECKED HERE ON BOTH SIDES OF A CALL. A call that names no tid is
// refused before it is sent; a write of a row outside /tenants/<tenant> is
// refused before it is sent; and a row read back that is not the tenant's is
// treated as absent (a list or a watch drops it), which is what that tenant's
// own Stratum would answer. The plane of the 2.5.2 compiler has no target_tenant
// field: until the 2.5 line carries tacplus-dev/tacplus#207
// (tacplus-dev/tacplus#235), a call reaches whichever instance answers, and these
// checks are what keep one tenant's token from resolving another tenant's row.
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
// The namespace beside the key is left empty: the key is unique within the
// tenant's store, and the tenant is what routes the call.
func (r *recordStore) FetchBlob(ctx context.Context, tenant, blobID string) (*blobstore_pb.Blob, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	resp, err := r.records.GetBlob(ctx, &query_pb.GetBlobRequest{BlobId: blobID})
	return absentIsNil(tenant, resp.GetItem(), err, "reading Blob")
}

func (r *recordStore) ListBlobsInGroup(ctx context.Context, tenant, group, namespace string) ([]*blobstore_pb.Blob, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	handle, err := r.records.CreateBlobQuery(ctx, &query_pb.OpenBlobQueryRequest{
		Filter: &query_pb.BlobFilter{Group: &group, Namespace: &namespace},
	})
	if err != nil {
		return nil, fmt.Errorf("opening Blob query: %w", err)
	}
	defer r.closeQuery("Blob", handle, r.records.DeleteBlobQuery)
	rows, err := drain(ctx, handle.GetQueryId(), r.records.ListBlobPage, func(queryID, token string) *query_pb.PullBlobRequest {
		return &query_pb.PullBlobRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
	return tenantRows(tenant, rows, err)
}

func (r *recordStore) FetchLink(ctx context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLink, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	resp, err := r.records.GetBlobLink(ctx, &query_pb.GetBlobLinkRequest{TokenHash: tokenHash})
	// The key is a hash of a credential, so it is never put in an error.
	return absentIsNil(tenant, resp.GetItem(), err, "reading BlobLink")
}

func (r *recordStore) ListLinksForBlob(ctx context.Context, tenant, blobID, namespace string) ([]*blobstore_pb.BlobLink, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	handle, err := r.records.CreateBlobLinkQuery(ctx, &query_pb.OpenBlobLinkQueryRequest{
		Filter: &query_pb.BlobLinkFilter{BlobId: &blobID, Namespace: &namespace},
	})
	if err != nil {
		return nil, fmt.Errorf("opening BlobLink query: %w", err)
	}
	defer r.closeQuery("BlobLink", handle, r.records.DeleteBlobLinkQuery)
	rows, err := drain(ctx, handle.GetQueryId(), r.records.ListBlobLinkPage, func(queryID, token string) *query_pb.PullBlobLinkRequest {
		return &query_pb.PullBlobLinkRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
	return tenantRows(tenant, rows, err)
}

func (r *recordStore) FetchSession(ctx context.Context, tenant, uploadID string) (*blobstore_pb.UploadSession, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	resp, err := r.records.GetUploadSession(ctx, &query_pb.GetUploadSessionRequest{UploadId: uploadID})
	return absentIsNil(tenant, resp.GetItem(), err, "reading UploadSession")
}

// ListExpiredSessions uses the record's RANGE filter on expires_ms rather than
// reading every session and filtering here: the predicate belongs in the store,
// and the sweep runs on a timer against a table that only grows.
func (r *recordStore) ListExpiredSessions(ctx context.Context, tenant string, nowMs int64) ([]*blobstore_pb.UploadSession, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	from := int64(1)
	handle, err := r.records.CreateUploadSessionQuery(ctx, &query_pb.OpenUploadSessionQueryRequest{
		Filter: &query_pb.UploadSessionFilter{ExpiresMsFrom: &from, ExpiresMsTo: &nowMs},
	})
	if err != nil {
		return nil, fmt.Errorf("opening UploadSession query: %w", err)
	}
	defer r.closeQuery("UploadSession", handle, r.records.DeleteUploadSessionQuery)
	rows, err := drain(ctx, handle.GetQueryId(), r.records.ListUploadSessionPage, func(queryID, token string) *query_pb.PullUploadSessionRequest {
		return &query_pb.PullUploadSessionRequest{QueryId: queryID, PageSize: recordPageSize, PageToken: token}
	})
	return tenantRows(tenant, rows, err)
}

func (r *recordStore) FetchLinkStats(ctx context.Context, tenant, tokenHash string) (*blobstore_pb.BlobLinkStats, error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	resp, err := r.records.GetBlobLinkStats(ctx, &query_pb.GetBlobLinkStatsRequest{TokenHash: tokenHash})
	return absentIsNil(tenant, resp.GetItem(), err, "reading BlobLinkStats")
}

// namespaced is every record type here: each carries its owning namespace.
type namespaced interface{ GetNamespace() string }

// routable refuses a call that names no tenant, before it is sent.
func routable(tenant string) error {
	if !ValidTenantID(tenant) {
		return fmt.Errorf("record plane call names no tenant id (%q): %w", tenant, ErrRecordOutsideTenant)
	}
	return nil
}

// tenantRows keeps the tenant's rows of a list.
func tenantRows[T namespaced](tenant string, rows []T, err error) ([]T, error) {
	if err != nil {
		return nil, err
	}
	out := rows[:0]
	for _, row := range rows {
		if inTenant(tenant, row.GetNamespace()) {
			out = append(out, row)
		}
	}
	return out, nil
}

// writable refuses, before it is sent, a write that names no tenant or whose
// rows (the one written and, for a replace, the one expected) are not the
// tenant's.
func writable(tenant string, rows ...namespaced) error {
	if err := routable(tenant); err != nil {
		return err
	}
	for _, row := range rows {
		if !inTenant(tenant, row.GetNamespace()) {
			return fmt.Errorf("row of namespace %q written to tenant %s: %w", row.GetNamespace(), tenant, ErrRecordOutsideTenant)
		}
	}
	return nil
}

// absentIsNil turns Get<T>'s answer into the (row, error) the callers want:
// NOT_FOUND is no row, not a failure, and so is another tenant's row, which is
// absent from this tenant's store.
func absentIsNil[T any, P interface {
	*T
	namespaced
}](tenant string, row P, err error, what string) (P, error) {
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, storeError(what, err)
	}
	if row == nil {
		return nil, fmt.Errorf("%s: the store answered with no row", what)
	}
	if !inTenant(tenant, row.GetNamespace()) {
		return nil, nil
	}
	return row, nil
}

// The writes. No durability is asked for: the generated wrapper writes at the
// type's declared floor, and asking for a level here would be a second place to
// state it.

func (r *recordStore) PutBlob(ctx context.Context, tenant string, rec *blobstore_pb.Blob) error {
	if err := writable(tenant, rec); err != nil {
		return err
	}
	_, err := r.records.PutBlob(ctx, &query_pb.PutBlobRequest{Item: rec})
	return writeError("writing Blob "+rec.GetBlobId(), err)
}

func (r *recordStore) ReplaceBlob(ctx context.Context, tenant string, prev, next *blobstore_pb.Blob) error {
	if err := writable(tenant, prev, next); err != nil {
		return err
	}
	_, err := r.records.ReplaceIfUnchangedBlob(ctx, &query_pb.ReplaceIfUnchangedBlobRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedBlobRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing Blob "+next.GetBlobId(), err)
}

func (r *recordStore) InsertSession(ctx context.Context, tenant string, rec *blobstore_pb.UploadSession) error {
	if err := writable(tenant, rec); err != nil {
		return err
	}
	_, err := r.records.InsertIfAbsentUploadSession(ctx, &query_pb.InsertIfAbsentUploadSessionRequest{Item: rec})
	return writeError("inserting UploadSession "+rec.GetUploadId(), err)
}

func (r *recordStore) ReplaceSession(ctx context.Context, tenant string, prev, next *blobstore_pb.UploadSession) error {
	if err := writable(tenant, prev, next); err != nil {
		return err
	}
	_, err := r.records.ReplaceIfUnchangedUploadSession(ctx, &query_pb.ReplaceIfUnchangedUploadSessionRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedUploadSessionRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing UploadSession "+next.GetUploadId(), err)
}

func (r *recordStore) InsertLink(ctx context.Context, tenant string, rec *blobstore_pb.BlobLink) error {
	if err := writable(tenant, rec); err != nil {
		return err
	}
	_, err := r.records.InsertIfAbsentBlobLink(ctx, &query_pb.InsertIfAbsentBlobLinkRequest{Item: rec})
	return writeError("inserting BlobLink for "+rec.GetBlobId(), err)
}

func (r *recordStore) ReplaceLink(ctx context.Context, tenant string, prev, next *blobstore_pb.BlobLink) error {
	if err := writable(tenant, prev, next); err != nil {
		return err
	}
	_, err := r.records.ReplaceIfUnchangedBlobLink(ctx, &query_pb.ReplaceIfUnchangedBlobLinkRequest{
		Item:     next,
		Expected: &query_pb.ReplaceIfUnchangedBlobLinkRequest_ExpectedItem{ExpectedItem: prev},
	})
	return writeError("replacing BlobLink for "+next.GetBlobId(), err)
}

func (r *recordStore) InsertLinkStats(ctx context.Context, tenant string, rec *blobstore_pb.BlobLinkStats) error {
	if err := writable(tenant, rec); err != nil {
		return err
	}
	_, err := r.records.InsertIfAbsentBlobLinkStats(ctx, &query_pb.InsertIfAbsentBlobLinkStatsRequest{Item: rec})
	return writeError("inserting BlobLinkStats", err)
}

func (r *recordStore) ReplaceLinkStats(ctx context.Context, tenant string, prev, next *blobstore_pb.BlobLinkStats) error {
	if err := writable(tenant, prev, next); err != nil {
		return err
	}
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

// The change feeds (Watch<T>Changes): every row of the tenant's type, then every
// acked write, until ctx ends or the stream fails. This is how blobstore learns of a
// change it did not make itself, rather than from a record topic. Nothing in the
// service needs one yet: every path reads the row it acts on when it acts.

func (r *recordStore) WatchBlobs(ctx context.Context, tenant string) (iter.Seq2[RecordChange[blobstore_pb.Blob], error], error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	events, err := r.records.WatchBlobChanges(ctx, &query_pb.WatchBlobChangesRequest{})
	return changes[blobstore_pb.Blob, *blobstore_pb.Blob](tenant, events, err)
}

func (r *recordStore) WatchSessions(ctx context.Context, tenant string) (iter.Seq2[RecordChange[blobstore_pb.UploadSession], error], error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	events, err := r.records.WatchUploadSessionChanges(ctx, &query_pb.WatchUploadSessionChangesRequest{})
	return changes[blobstore_pb.UploadSession, *blobstore_pb.UploadSession](tenant, events, err)
}

func (r *recordStore) WatchLinks(ctx context.Context, tenant string) (iter.Seq2[RecordChange[blobstore_pb.BlobLink], error], error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	events, err := r.records.WatchBlobLinkChanges(ctx, &query_pb.WatchBlobLinkChangesRequest{})
	return changes[blobstore_pb.BlobLink, *blobstore_pb.BlobLink](tenant, events, err)
}

func (r *recordStore) WatchLinkStats(ctx context.Context, tenant string) (iter.Seq2[RecordChange[blobstore_pb.BlobLinkStats], error], error) {
	if err := routable(tenant); err != nil {
		return nil, err
	}
	events, err := r.records.WatchBlobLinkStatsChanges(ctx, &query_pb.WatchBlobLinkStatsChangesRequest{})
	return changes[blobstore_pb.BlobLinkStats, *blobstore_pb.BlobLinkStats](tenant, events, err)
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

// changes turns one generated watch stream into the tenant's RecordChanges.
// snapshot_done carries no row and is not passed on, and neither is another
// tenant's row.
func changes[T any, P interface {
	*T
	namespaced
}, D watchDelta[T], E watchEvent[T, D]](tenant string, events iter.Seq2[E, error], err error) (iter.Seq2[RecordChange[T], error], error) {
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
			if !inTenant(tenant, P(c.Row).GetNamespace()) {
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
