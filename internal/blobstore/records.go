package blobstore

import (
	"context"
	"fmt"

	query_gc "gitlab.com/seabeam/schematic/platform/services/blobstore/model/s2sm/grpc/clients/vservices/query"
	blobstore_pb "gitlab.com/seabeam/schematic/platform/services/blobstore/model/types/services/blobstore"
	query_pb "gitlab.com/seabeam/schematic/platform/services/blobstore/model/types/vservices/query"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Records is this service's whole relationship with stratum: read its own rows
// back through the generated record plane, and write them by publishing onto each
// record's synthesized topic.
//
// A RECORD HAS NO WRITE RPC. Publishing IS writing (R1), and a publish returns
// once the record is ACCEPTED into the generated publisher's bounded retry queue
// — not once stratum has stored the row. So an error from a publish here means
// the record was not accepted for delivery at all, which is the failure worth
// refusing to continue past; success means delivery is underway and will not be
// dropped short of the process dying. This is the same contract letsencrypt's
// recordStore documents, and it is why nothing in this service reads a row back
// to confirm a write.
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

	PublishBlob(ctx context.Context, rec *blobstore_pb.Blob) error
	PublishSession(ctx context.Context, rec *blobstore_pb.UploadSession) error
	PublishLink(ctx context.Context, rec *blobstore_pb.BlobLink) error
	PublishLinkStats(ctx context.Context, rec *blobstore_pb.BlobLinkStats) error
}

// recordPageSize is the page the store is asked for on a multi-row read. It is
// the same 100 ListLinks declares as its max_items, so a link listing is one
// page in the common case.
const recordPageSize = 100

// recordStore is Records over the generated plane. Nothing here speaks to
// stratum directly.
type recordStore struct {
	records *query_gc.RecordMapClient

	publishBlob      func(context.Context, *blobstore_pb.Blob) error
	publishSession   func(context.Context, *blobstore_pb.UploadSession) error
	publishLink      func(context.Context, *blobstore_pb.BlobLink) error
	publishLinkStats func(context.Context, *blobstore_pb.BlobLinkStats) error
}

// NewRecords wires the read client and the four publishers the ServiceManager
// exposes for this file's records.
func NewRecords(
	records *query_gc.RecordMapClient,
	publishBlob func(context.Context, *blobstore_pb.Blob) error,
	publishSession func(context.Context, *blobstore_pb.UploadSession) error,
	publishLink func(context.Context, *blobstore_pb.BlobLink) error,
	publishLinkStats func(context.Context, *blobstore_pb.BlobLinkStats) error,
) Records {
	return &recordStore{
		records:          records,
		publishBlob:      publishBlob,
		publishSession:   publishSession,
		publishLink:      publishLink,
		publishLinkStats: publishLinkStats,
	}
}

// FetchBlob reads one blob row by key. A missing row is (nil, nil) — the callers
// all turn that into 404 or a not-found RPC error, and none of them wants an
// error value they have to inspect to tell "absent" from "broken".
//
// INCLUDE DELETED. A deleted blob must resolve to "gone", not to "never existed",
// on the paths that care (DeleteBlob's idempotency, and refusing a link to a
// deleted blob), so the row is read and the flag checked rather than filtered out
// by the query.
func (r *recordStore) FetchBlob(ctx context.Context, blobID string) (*blobstore_pb.Blob, error) {
	handle, err := r.records.CreateBlobQuery(ctx, &query_pb.OpenBlobQueryRequest{
		Filter:         &query_pb.BlobFilter{BlobId: &blobID},
		IncludeDeleted: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening Blob query: %w", err)
	}
	defer r.closeQuery("Blob", handle, r.records.DeleteBlobQuery)

	// PageSize 2, not 1: the key is unique, so a second row means the plane
	// answered with a row for a different key and the read cannot be trusted.
	// Asking for one row could not tell the difference. (letsencrypt's read makes
	// the same argument.)
	rows, _, _, err := r.records.ListBlobPage(ctx, &query_pb.PullBlobRequest{
		QueryId:  handle.GetQueryId(),
		PageSize: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("reading Blob rows: %w", err)
	}
	return exactlyOne("Blob", blobID, rows)
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
	handle, err := r.records.CreateBlobLinkQuery(ctx, &query_pb.OpenBlobLinkQueryRequest{
		Filter:         &query_pb.BlobLinkFilter{TokenHash: &tokenHash},
		IncludeDeleted: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening BlobLink query: %w", err)
	}
	defer r.closeQuery("BlobLink", handle, r.records.DeleteBlobLinkQuery)
	rows, _, _, err := r.records.ListBlobLinkPage(ctx, &query_pb.PullBlobLinkRequest{
		QueryId:  handle.GetQueryId(),
		PageSize: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("reading BlobLink rows: %w", err)
	}
	// The key is a hash, so it is never logged or echoed on a mismatch.
	return exactlyOne("BlobLink", "<token_hash>", rows)
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
	handle, err := r.records.CreateUploadSessionQuery(ctx, &query_pb.OpenUploadSessionQueryRequest{
		Filter:         &query_pb.UploadSessionFilter{UploadId: &uploadID},
		IncludeDeleted: true,
	})
	if err != nil {
		return nil, fmt.Errorf("opening UploadSession query: %w", err)
	}
	defer r.closeQuery("UploadSession", handle, r.records.DeleteUploadSessionQuery)
	rows, _, _, err := r.records.ListUploadSessionPage(ctx, &query_pb.PullUploadSessionRequest{
		QueryId:  handle.GetQueryId(),
		PageSize: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("reading UploadSession rows: %w", err)
	}
	return exactlyOne("UploadSession", uploadID, rows)
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
	handle, err := r.records.CreateBlobLinkStatsQuery(ctx, &query_pb.OpenBlobLinkStatsQueryRequest{
		Filter: &query_pb.BlobLinkStatsFilter{TokenHash: &tokenHash},
	})
	if err != nil {
		return nil, fmt.Errorf("opening BlobLinkStats query: %w", err)
	}
	defer r.closeQuery("BlobLinkStats", handle, r.records.DeleteBlobLinkStatsQuery)
	rows, _, _, err := r.records.ListBlobLinkStatsPage(ctx, &query_pb.PullBlobLinkStatsRequest{
		QueryId:  handle.GetQueryId(),
		PageSize: 2,
	})
	if err != nil {
		return nil, fmt.Errorf("reading BlobLinkStats rows: %w", err)
	}
	return exactlyOne("BlobLinkStats", "<token_hash>", rows)
}

func (r *recordStore) PublishBlob(ctx context.Context, rec *blobstore_pb.Blob) error {
	if err := r.publishBlob(ctx, rec); err != nil {
		return fmt.Errorf("publishing Blob %s: %w", rec.GetBlobId(), err)
	}
	return nil
}

func (r *recordStore) PublishSession(ctx context.Context, rec *blobstore_pb.UploadSession) error {
	if err := r.publishSession(ctx, rec); err != nil {
		return fmt.Errorf("publishing UploadSession %s: %w", rec.GetUploadId(), err)
	}
	return nil
}

func (r *recordStore) PublishLink(ctx context.Context, rec *blobstore_pb.BlobLink) error {
	if err := r.publishLink(ctx, rec); err != nil {
		return fmt.Errorf("publishing BlobLink for %s: %w", rec.GetBlobId(), err)
	}
	return nil
}

func (r *recordStore) PublishLinkStats(ctx context.Context, rec *blobstore_pb.BlobLinkStats) error {
	if err := r.publishLinkStats(ctx, rec); err != nil {
		return fmt.Errorf("publishing BlobLinkStats: %w", err)
	}
	return nil
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

// exactlyOne turns a keyed read's page into the one row it should hold. Two rows
// for a unique key is a broken read, not a row to pick from.
func exactlyOne[T any](kind, key string, rows []*T) (*T, error) {
	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		return rows[0], nil
	default:
		return nil, fmt.Errorf("%s key %s matched %d rows", kind, key, len(rows))
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
