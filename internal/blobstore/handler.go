package blobstore

import (
	"context"
	"errors"
	"sort"
	"time"

	"gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/chassis/collection"

	"gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/chassis/servicemanager"
	blobstore_fe "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/s2sm/grpc/servers/services/blobstore"
	eventbus_support_pb "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/types/generated/eventbus_support"
	blobstore_pb "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/types/services/blobstore"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is blobstore's implementation of the generated Handler interface — the
// ONE hand-written seam tacplus allows. Transport, TLS, auth, validation, config
// delivery and health are all generated; what is here is the service's own logic.
var _ blobstore_fe.BlobStoreHandler = (*Handler)(nil)

// Handler carries only the Service. It is a separate type from Service so that
// the RPC surface (which needs a caller identity on every call) and the byte paths
// (which are authorized by a token and have no caller) cannot be confused for one
// another.
type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler { return &Handler{svc: svc} }

// Service is the service this handler drives, for main.go's listener wiring.
func (h *Handler) Service() *Service { return h.svc }

// ---------------------------------------------------------------------------
// The owning-service check
// ---------------------------------------------------------------------------

// callerPrincipal is the caller's location-agnostic service path — docmanager,
// not docmanager's pod. That is what goes in Blob.owner_principal, so ownership
// survives a pod restart, a rescheduling and a location change.
//
// The tenant half of the check is already done before this handler runs: every RPC
// declares (spec.rpc).target_namespace_field, so the generated authorizer has held
// the caller to its own namespace or below. What is left for the handler is the
// SERVICE half — that this caller is the one that owns this row (R6).
func callerPrincipal(caller *servicemanager.Caller) (string, error) {
	if caller == nil || caller.Token() == nil {
		return "", status.Error(codes.Unauthenticated, "no caller identity")
	}
	path, err := caller.Token().ServicePath()
	if err != nil {
		return "", status.Error(codes.PermissionDenied, "caller is not a service identity")
	}
	return path, nil
}

// requireOwner refuses a caller that is not the row's owner.
//
// IT ANSWERS NOT_FOUND, not PERMISSION_DENIED, when the row belongs to somebody
// else. A service asking about another service's blob must not be able to tell
// "exists but is not yours" from "does not exist" — that difference is the same
// content oracle the per-tenant content addressing was chosen to avoid (R5).
func requireOwner(rowOwner, rowNamespace, caller, namespace, what string) error {
	if rowOwner != caller || rowNamespace != namespace {
		return status.Errorf(codes.NotFound, "no such %s", what)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

// CreateUpload is the doc's BeginUpload (R9 step 1), renamed for the closed verb
// registry — see the service comment in blobstore.proto.
func (h *Handler) CreateUpload(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.CreateUploadRequest) (*blobstore_pb.CreateUploadResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	conf := h.svc.conf()
	if conf == nil {
		return nil, status.Error(codes.Unavailable, "config has not been delivered")
	}
	if err := validateBlobKey(req.GetGroup(), req.GetNamespace(), req.GetContentHash()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	vg, err := conf.groups.CheckDeclared(req.GetGroup(), req.GetContentType(), req.GetSizeBytes())
	if err != nil {
		// A group that is not declared, a type it does not allow, or a size past
		// its ceiling are all the caller asking for something the group's rules
		// forbid — FAILED_PRECONDITION rather than INVALID_ARGUMENT, because the
		// request is well-formed and the configuration is what refuses it.
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// The token is minted FIRST because its sha256 is the session's key: the row
	// and the credential are one thing, exactly as CreateLink's are (R3, R9).
	uploadToken, uploadID, err := MintUploadToken()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Reserve BEFORE minting the token: a token that exists without a reservation
	// is permission to exceed the quota (R9 step 1).
	if err := h.svc.quota.Reserve(ctx, uploadID, req.GetGroup(), req.GetNamespace(), req.GetSizeBytes(), vg.GetTenantQuotaBytes()); err != nil {
		// Over quota and "cannot read the store to find out" are different answers
		// to the caller: the first is final and the second is worth retrying. Both
		// refuse the upload — fail closed (R11) — but a store this service cannot
		// read must not be reported as the tenant being full.
		if errors.Is(err, ErrQuotaExceeded) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	release := func() { h.svc.quota.Release(uploadID, req.GetGroup(), req.GetNamespace()) }

	now := h.svc.now()
	expiresMs := conf.uploadExpiry(now)
	session := &blobstore_pb.UploadSession{
		UploadId:       uploadID,
		Group:          req.GetGroup(),
		ContentHash:    req.GetContentHash(),
		SizeBytes:      req.GetSizeBytes(),
		ContentType:    req.GetContentType(),
		ReservedBytes:  req.GetSizeBytes(),
		State:          blobstore_pb.UploadState_UPLOAD_STATE_PENDING,
		CreatedMs:      now.UnixMilli(),
		ExpiresMs:      expiresMs,
		OwnerPrincipal: owner,
		Namespace:      req.GetNamespace(),
	}
	if err := h.svc.records.PublishSession(ctx, session); err != nil {
		release()
		// Fail closed on stratum (R11). A token handed out without a durable
		// session is now worse than a reservation nothing can release: the row IS
		// the grant, so the token would never be accepted by anything.
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	key := BlobKey{Group: req.GetGroup(), Namespace: req.GetNamespace(), Hash: req.GetContentHash()}
	existing, err := h.svc.records.FetchBlob(ctx, key.String())
	if err != nil {
		release()
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &blobstore_pb.CreateUploadResponse{
		UploadId:    uploadID,
		UploadToken: uploadToken,
		UploadPath:  PathUpload + uploadToken,
		ExpiresMs:   expiresMs,
		// already_present lets a caller skip the transfer entirely — the whole
		// payoff of declaring the hash up front. The token is still valid, so a
		// caller that ignores this is simply doing a dedup.
		AlreadyPresent: existing != nil && !existing.GetDeleted(),
		BlobId:         key.String(),
	}, nil
}

// CancelUpload releases the reservation and settles the session (R9 step 4).
// IDEMPOTENT: a session that has already settled answers cancelled=false.
func (h *Handler) CancelUpload(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.CancelUploadRequest) (*blobstore_pb.CancelUploadResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	session, err := h.svc.records.FetchSession(ctx, req.GetUploadId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if session == nil {
		return nil, status.Error(codes.NotFound, "no such upload")
	}
	if err := requireOwner(session.GetOwnerPrincipal(), session.GetNamespace(), owner, req.GetNamespace(), "upload"); err != nil {
		return nil, err
	}
	if session.GetState() != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		return &blobstore_pb.CancelUploadResponse{Cancelled: false}, nil
	}
	h.svc.quota.Release(session.GetUploadId(), session.GetGroup(), session.GetNamespace())
	if err := h.svc.settleSession(ctx, session, blobstore_pb.UploadState_UPLOAD_STATE_ABORTED); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &blobstore_pb.CancelUploadResponse{Cancelled: true}, nil
}

// ---------------------------------------------------------------------------
// Links
// ---------------------------------------------------------------------------

// CreateLink mints a durable bearer link (R7). The raw token is returned once and
// stored nowhere; the row holds sha256(token).
func (h *Handler) CreateLink(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.CreateLinkRequest) (*blobstore_pb.CreateLinkResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	if err := ValidateFilename(req.GetFilename()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := h.ownedBlob(ctx, req.GetBlobId(), owner, req.GetNamespace()); err != nil {
		return nil, err
	}

	token, tokenHash, err := MintLinkToken()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createdMs := h.svc.now().UnixMilli()
	link := &blobstore_pb.BlobLink{
		TokenHash:      tokenHash,
		BlobId:         req.GetBlobId(),
		Filename:       req.GetFilename(),
		Policy:         blobstore_pb.LinkPolicy_LINK_POLICY_BEARER,
		CreatedMs:      createdMs,
		OwnerPrincipal: owner,
		Namespace:      req.GetNamespace(),
	}
	if err := h.svc.records.PublishLink(ctx, link); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &blobstore_pb.CreateLinkResponse{
		Token:     token,
		LinkPath:  PathBlob + token,
		TokenHash: tokenHash,
		CreatedMs: createdMs,
	}, nil
}

// CreateBlobUrl mints a short-lived self-use link — the doc's GetBlobUrl. Same row
// type as CreateLink; only expires_ms differs.
func (h *Handler) CreateBlobUrl(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.CreateBlobUrlRequest) (*blobstore_pb.CreateBlobUrlResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	if err := ValidateFilename(req.GetFilename()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if _, err := h.ownedBlob(ctx, req.GetBlobId(), owner, req.GetNamespace()); err != nil {
		return nil, err
	}

	token, tokenHash, err := MintLinkToken()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	now := h.svc.now()
	expiresMs := now.Add(time.Duration(req.GetTtlSeconds()) * time.Second).UnixMilli()
	link := &blobstore_pb.BlobLink{
		TokenHash:      tokenHash,
		BlobId:         req.GetBlobId(),
		Filename:       req.GetFilename(),
		Policy:         blobstore_pb.LinkPolicy_LINK_POLICY_BEARER,
		CreatedMs:      now.UnixMilli(),
		ExpiresMs:      expiresMs,
		OwnerPrincipal: owner,
		Namespace:      req.GetNamespace(),
	}
	if err := h.svc.records.PublishLink(ctx, link); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &blobstore_pb.CreateBlobUrlResponse{
		Token:     token,
		LinkPath:  PathBlob + token,
		TokenHash: tokenHash,
		ExpiresMs: expiresMs,
	}, nil
}

// RevokeLink stops a link resolving immediately (R7). The row is KEPT with
// revoked_ms set, for audit. IDEMPOTENT.
func (h *Handler) RevokeLink(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.RevokeLinkRequest) (*blobstore_pb.RevokeLinkResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	link, err := h.svc.records.FetchLink(ctx, req.GetTokenHash())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if link == nil {
		return nil, status.Error(codes.NotFound, "no such link")
	}
	if err := requireOwner(link.GetOwnerPrincipal(), link.GetNamespace(), owner, req.GetNamespace(), "link"); err != nil {
		return nil, err
	}
	if link.GetRevokedMs() != 0 {
		return &blobstore_pb.RevokeLinkResponse{Revoked: false, RevokedMs: link.GetRevokedMs()}, nil
	}
	revokedMs := h.svc.now().UnixMilli()
	next := cloneLink(link)
	next.RevokedMs = revokedMs
	if err := h.svc.records.PublishLink(ctx, next); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &blobstore_pb.RevokeLinkResponse{Revoked: true, RevokedMs: revokedMs}, nil
}

// ListLinks returns one page of the caller's links for a blob, without tokens
// (R7).
//
// THE SEAM IS THE GENERATED PAGE SEAM. The wrapper has already defaulted and
// clamped the page size and decoded the opaque page_token into a keyset cursor;
// this handler returns the window and the cursor to resume after, and the wrapper
// re-encodes it. Nothing here parses or emits a token.
//
// The window is applied AFTER the record read rather than inside it, because the
// record plane's BlobLink filter has no keyset predicate on token_hash — it EQ- and
// RANGE-filters the fields the record declares and token_hash is the key, read whole
// by ListLinksForBlob. That read is bounded by the number of links one blob has, so
// the cost is a sort of that set per page rather than a scan of the store.
func (h *Handler) ListLinks(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.ListLinksRequest, page collection.PageParams) (collection.Page[*blobstore_pb.BlobLink], error) {
	var empty collection.Page[*blobstore_pb.BlobLink]
	owner, err := callerPrincipal(caller)
	if err != nil {
		return empty, err
	}
	if _, err := h.ownedBlob(ctx, req.GetBlobId(), owner, req.GetNamespace()); err != nil {
		return empty, err
	}
	links, err := h.svc.records.ListLinksForBlob(ctx, req.GetBlobId(), req.GetNamespace())
	if err != nil {
		return empty, status.Error(codes.Unavailable, err.Error())
	}

	// The caller's own rows only. The generated egress filter on
	// (spec.store_principal) is the backstop; this is the predicate.
	owned := make([]*blobstore_pb.BlobLink, 0, len(links))
	for _, l := range links {
		if l.GetOwnerPrincipal() == owner {
			owned = append(owned, l)
		}
	}
	// order_by is ["token_hash"], so the page order is that order and the cursor
	// is that one key component.
	sort.Slice(owned, func(i, j int) bool { return owned[i].GetTokenHash() < owned[j].GetTokenHash() })

	// Total is the size of the WHOLE set, taken before the cursor is applied: it
	// answers "how many links does this blob have", which does not change as the
	// caller pages through them.
	total := int64(len(owned))

	if page.After != nil && len(page.After.Keys) == 1 {
		after := page.After.Keys[0]
		idx := sort.Search(len(owned), func(i int) bool { return owned[i].GetTokenHash() > after })
		owned = owned[idx:]
	}

	out := owned
	var next *collection.Cursor
	if page.Limit > 0 && len(out) > page.Limit {
		out = out[:page.Limit]
		next = &collection.Cursor{Keys: []string{out[len(out)-1].GetTokenHash()}}
	}
	return collection.Page[*blobstore_pb.BlobLink]{Items: out, NextAfter: next, Total: total}, nil
}

// DeleteBlob unlinks the bytes from BOTH pods and flags the row deleted.
//
// The peer is asked FIRST and a failure refuses the whole delete: a delete that
// happened on one pod and not the other leaves bytes the owner believes are gone,
// which is worse than a delete the owner has to retry. The record flag is written
// last, so the row still points at bytes for as long as any bytes remain.
func (h *Handler) DeleteBlob(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.DeleteBlobRequest) (*blobstore_pb.DeleteBlobResponse, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	blob, err := h.svc.records.FetchBlob(ctx, req.GetBlobId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if blob == nil {
		return nil, status.Error(codes.NotFound, "no such blob")
	}
	if err := requireOwner(blob.GetOwnerPrincipal(), blob.GetNamespace(), owner, req.GetNamespace(), "blob"); err != nil {
		return nil, err
	}
	if blob.GetDeleted() {
		return &blobstore_pb.DeleteBlobResponse{Deleted: false}, nil
	}
	key, err := ParseBlobKey(blob.GetBlobId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := h.svc.mirror.DeleteOnPeer(ctx, key); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if err := h.svc.store.Remove(key.Group, key.Namespace, key.Hash); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	next := cloneBlob(blob)
	next.Deleted = true
	if err := h.svc.records.PublishBlob(ctx, next); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &blobstore_pb.DeleteBlobResponse{Deleted: true}, nil
}

// GetLinkStats answers with the stored rollup PLUS this pod's unflushed delta
// (R10), so a caller reading its own stats right after a download is not told zero
// for up to a whole flush interval. It is still only this pod's view of the other
// pod's contribution — see BlobLinkStats's own comment.
func (h *Handler) GetLinkStats(ctx context.Context, caller *servicemanager.Caller, req *blobstore_pb.GetLinkStatsRequest) (*blobstore_pb.BlobLinkStats, error) {
	owner, err := callerPrincipal(caller)
	if err != nil {
		return nil, err
	}
	// The LINK is what the caller owns; the stats row is derived from it. Checking
	// the link means a stats row that has never been flushed still refuses a
	// caller who does not own the link.
	link, err := h.svc.records.FetchLink(ctx, req.GetTokenHash())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if link == nil {
		return nil, status.Error(codes.NotFound, "no such link")
	}
	if err := requireOwner(link.GetOwnerPrincipal(), link.GetNamespace(), owner, req.GetNamespace(), "link"); err != nil {
		return nil, err
	}
	stored, err := h.svc.records.FetchLinkStats(ctx, req.GetTokenHash())
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resolutions, bytesServed, lastResolvedMs := h.svc.stats.PendingFor(req.GetTokenHash())
	if stored.GetLastResolvedMs() > lastResolvedMs {
		lastResolvedMs = stored.GetLastResolvedMs()
	}
	return &blobstore_pb.BlobLinkStats{
		TokenHash:      req.GetTokenHash(),
		Resolutions:    stored.GetResolutions() + resolutions,
		BytesServed:    stored.GetBytesServed() + bytesServed,
		LastResolvedMs: lastResolvedMs,
		OwnerPrincipal: link.GetOwnerPrincipal(),
		Namespace:      link.GetNamespace(),
	}, nil
}

// ownedBlob reads a blob and refuses it unless the caller owns it and it is live.
func (h *Handler) ownedBlob(ctx context.Context, blobID, owner, namespace string) (*blobstore_pb.Blob, error) {
	blob, err := h.svc.records.FetchBlob(ctx, blobID)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if blob == nil || blob.GetDeleted() {
		return nil, status.Error(codes.NotFound, "no such blob")
	}
	if err := requireOwner(blob.GetOwnerPrincipal(), blob.GetNamespace(), owner, namespace, "blob"); err != nil {
		return nil, err
	}
	return blob, nil
}

func cloneLink(l *blobstore_pb.BlobLink) *blobstore_pb.BlobLink {
	return &blobstore_pb.BlobLink{
		TokenHash:      l.GetTokenHash(),
		BlobId:         l.GetBlobId(),
		Filename:       l.GetFilename(),
		Policy:         l.GetPolicy(),
		CreatedMs:      l.GetCreatedMs(),
		ExpiresMs:      l.GetExpiresMs(),
		RevokedMs:      l.GetRevokedMs(),
		OwnerPrincipal: l.GetOwnerPrincipal(),
		Deleted:        l.GetDeleted(),
		Namespace:      l.GetNamespace(),
	}
}

func cloneBlob(b *blobstore_pb.Blob) *blobstore_pb.Blob {
	return &blobstore_pb.Blob{
		BlobId:         b.GetBlobId(),
		Group:          b.GetGroup(),
		ContentHash:    b.GetContentHash(),
		SizeBytes:      b.GetSizeBytes(),
		ContentType:    b.GetContentType(),
		CreatedMs:      b.GetCreatedMs(),
		OwnerPrincipal: b.GetOwnerPrincipal(),
		Deleted:        b.GetDeleted(),
		Namespace:      b.GetNamespace(),
	}
}

// ---------------------------------------------------------------------------
// The generated lifecycle interface
// ---------------------------------------------------------------------------

// BlobStoreGrpcImpl hands the RPC surface to the generated wrapper.
func (h *Handler) BlobStoreGrpcImpl() blobstore_fe.BlobStoreHandler { return h }

// BlobStoreConfig receives the typed config before the server starts and on every
// update. An error on the first call stops the service from starting, which is what
// a bad group rendering or a missing token key should do.
func (h *Handler) BlobStoreConfig(cfg *blobstore_pb.Config) error { return h.svc.Configure(cfg) }

// The <Record>BusState services below are the topic endpoints tacplus generates
// for every (spec.record) declared in this package, and they are registered on
// this service's gRPC server by the generated Run().
//
// BLOBSTORE IS NOT THE BROKER. A record's topic is hosted by eventbus; this
// service's relationship with its own records is "publish onto the topic" (which
// is what CreateBlobstore*Publisher does) and "read the stored rows back" (which
// is the record plane). Nothing in the fleet dials blobstore for these, and
// implementing them would mean this service pretending to be a message broker.
//
// So they are refused explicitly, UNIMPLEMENTED, rather than left nil. A nil
// handler behind the generated wrapper is a panic waiting for the first caller;
// this is an answer. The generated Handler interface requires them, which is a
// tacplus gap for a Go service that both declares a record and serves its own
// RPCs — reported in workers/blobstore-result.md rather than worked around
// further.
func (h *Handler) BlobBusStateGrpcImpl() blobstore_fe.BlobBusStateServiceHandler {
	return blobBusNotHosted{}
}
func (h *Handler) BlobBusStateConfig(*blobstore_pb.Config) error { return nil }
func (h *Handler) UploadSessionBusStateGrpcImpl() blobstore_fe.UploadSessionBusStateServiceHandler {
	return uploadSessionBusNotHosted{}
}
func (h *Handler) UploadSessionBusStateConfig(*blobstore_pb.Config) error { return nil }
func (h *Handler) BlobLinkBusStateGrpcImpl() blobstore_fe.BlobLinkBusStateServiceHandler {
	return blobLinkBusNotHosted{}
}
func (h *Handler) BlobLinkBusStateConfig(*blobstore_pb.Config) error { return nil }
func (h *Handler) BlobLinkStatsBusStateGrpcImpl() blobstore_fe.BlobLinkStatsBusStateServiceHandler {
	return blobLinkStatsBusNotHosted{}
}
func (h *Handler) BlobLinkStatsBusStateConfig(*blobstore_pb.Config) error { return nil }

// errBusNotHosted is the one answer all eight bus methods give.
var errBusNotHosted = status.Error(codes.Unimplemented,
	"blobstore does not host its records' topics; eventbus is the broker")

// Four types rather than one, because the generated interfaces differ in their
// stream MESSAGE type and a Go method set cannot be generic over that. Each is
// two lines; a shared implementation would need the stream types to be parameters
// of the interface, which they are not.
type blobBusNotHosted struct{}

func (blobBusNotHosted) PublishToStream(*servicemanager.Caller, grpc.BidiStreamingServer[blobstore_pb.Blob, eventbus_support_pb.PublishResponse]) error {
	return errBusNotHosted
}

func (blobBusNotHosted) SubscribeToStream(*servicemanager.Caller, grpc.BidiStreamingServer[eventbus_support_pb.SubscribeRequest, blobstore_pb.BlobBusStreamSubscribeRecord]) error {
	return errBusNotHosted
}

type uploadSessionBusNotHosted struct{}

func (uploadSessionBusNotHosted) PublishToStream(*servicemanager.Caller, grpc.BidiStreamingServer[blobstore_pb.UploadSession, eventbus_support_pb.PublishResponse]) error {
	return errBusNotHosted
}

func (uploadSessionBusNotHosted) SubscribeToStream(*servicemanager.Caller, grpc.BidiStreamingServer[eventbus_support_pb.SubscribeRequest, blobstore_pb.UploadSessionBusStreamSubscribeRecord]) error {
	return errBusNotHosted
}

type blobLinkBusNotHosted struct{}

func (blobLinkBusNotHosted) PublishToStream(*servicemanager.Caller, grpc.BidiStreamingServer[blobstore_pb.BlobLink, eventbus_support_pb.PublishResponse]) error {
	return errBusNotHosted
}

func (blobLinkBusNotHosted) SubscribeToStream(*servicemanager.Caller, grpc.BidiStreamingServer[eventbus_support_pb.SubscribeRequest, blobstore_pb.BlobLinkBusStreamSubscribeRecord]) error {
	return errBusNotHosted
}

type blobLinkStatsBusNotHosted struct{}

func (blobLinkStatsBusNotHosted) PublishToStream(*servicemanager.Caller, grpc.BidiStreamingServer[blobstore_pb.BlobLinkStats, eventbus_support_pb.PublishResponse]) error {
	return errBusNotHosted
}

func (blobLinkStatsBusNotHosted) SubscribeToStream(*servicemanager.Caller, grpc.BidiStreamingServer[eventbus_support_pb.SubscribeRequest, blobstore_pb.BlobLinkStatsBusStreamSubscribeRecord]) error {
	return errBusNotHosted
}

// BlobstoreConfigStateGrpcImpl is the generated per-service config state surface.
// It is served by the config store (kernel), not by this service, and is refused
// here for the same reason the bus services are.
func (h *Handler) BlobstoreConfigStateGrpcImpl() blobstore_fe.BlobstoreConfigStateServiceHandler {
	return configStateNotHosted{}
}
func (h *Handler) BlobstoreConfigStateConfig(*blobstore_pb.Config) error { return nil }

type configStateNotHosted struct{}

func (configStateNotHosted) GetConfig(context.Context, *servicemanager.Caller) (*blobstore_pb.Config, error) {
	return nil, errConfigStateNotHosted
}

func (configStateNotHosted) SetConfig(context.Context, *servicemanager.Caller, *blobstore_pb.Config) (*blobstore_pb.Config, error) {
	return nil, errConfigStateNotHosted
}

func (configStateNotHosted) WatchConfig(*servicemanager.Caller, grpc.ServerStreamingServer[blobstore_pb.Config]) error {
	return errConfigStateNotHosted
}

var errConfigStateNotHosted = status.Error(codes.Unimplemented,
	"blobstore's config state is served by the config store, not by blobstore")

// Shutdown drains what the service holds: the last stats flush is what matters,
// because those counters live only in memory.
func (h *Handler) Shutdown(ctx context.Context) {
	h.svc.stats.Flush(ctx)
	zap.L().Info("blobstore shut down")
}
