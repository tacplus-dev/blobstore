package blobstore

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"go.uber.org/zap"
)

// The two public byte paths (R3). Nothing else is served on this listener.
const (
	PathUpload = "/upload/"
	PathBlob   = "/b/"
)

// UploadPath and BlobPath are the paths the RPCs hand out: the tenant, then the
// token, `/upload/<tid>/<token>` and `/b/<tid>/<token>`.
//
// THE TENANT IS IN THE PATH (#8). A token is raw randomness; the row it hashes to
// is in its tenant's own Stratum, which has to be named before the row can be
// read (tacplus-dev/roadmap decisions/tenant-routing-2026-10-26.md: the request
// names its tenant). The tid is not a secret and authorizes nothing: the token
// is still the whole grant, and a token presented under another tid is a 404,
// because that tenant's store holds no such row.
func UploadPath(tenant, token string) string { return PathUpload + tenant + "/" + token }
func BlobPath(tenant, token string) string   { return PathBlob + tenant + "/" + token }

// bearerPath splits what follows a byte path's prefix into its tenant and
// token. Anything else (no tenant, a tenant that is not a tid, an empty token,
// a further segment) is not a path this service hands out.
func bearerPath(rest string) (tenant, token string, ok bool) {
	tenant, token, _ = strings.Cut(rest, "/")
	if !ValidTenantID(tenant) || token == "" || strings.Contains(token, "/") {
		return "", "", false
	}
	return tenant, token, true
}

// BytesHandler is the public listener's router. It is deliberately a hand-written
// prefix match rather than a mux: there are exactly two paths, both take the
// tenant and the credential as the whole remainder of the path, and a
// pattern-matching mux would have to be told not to normalise or unescape that
// remainder.
func (s *Service) BytesHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, PathUpload):
			s.serveUpload(w, r, strings.TrimPrefix(r.URL.Path, PathUpload))
		case strings.HasPrefix(r.URL.Path, PathBlob):
			s.serveBlob(w, r, strings.TrimPrefix(r.URL.Path, PathBlob))
		default:
			http.NotFound(w, r)
		}
	})
	return mux
}

// serveBlob resolves a bearer link and serves the bytes (R3, R7, R8).
//
// EVERY REFUSAL IS THE SAME 404. Unknown token, revoked link, expired link,
// deleted blob, a blob this pod does not hold: one answer, no body, no header that
// distinguishes them. The doc requires "unknown and revoked indistinguishable
// 404s", and the reason generalises — any distinction at all turns this endpoint
// into an oracle a holder of one token can use to learn about links it does not
// hold.
//
// The one exception is 503, for a stratum this pod cannot read: that is not
// information about the token, it is information about us (R11).
func (s *Service) serveBlob(w http.ResponseWriter, r *http.Request, rest string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	tenant, token, ok := bearerPath(rest)
	if !ok {
		http.NotFound(w, r)
		return
	}

	link, err := s.records.FetchLink(r.Context(), tenant, LinkTokenHash(token))
	if err != nil {
		zap.L().Error("failed to read BlobLink", zap.Error(err))
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	nowMs := s.now().UnixMilli()
	if link == nil || link.GetDeleted() || link.GetRevokedMs() != 0 ||
		(link.GetExpiresMs() != 0 && link.GetExpiresMs() <= nowMs) {
		http.NotFound(w, r)
		return
	}

	blob, err := s.records.FetchBlob(r.Context(), tenant, link.GetBlobId())
	if err != nil {
		zap.L().Error("failed to read Blob", zap.Error(err))
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	if blob == nil || blob.GetDeleted() {
		http.NotFound(w, r)
		return
	}

	key, err := ParseBlobKey(blob.GetBlobId())
	if err != nil {
		// A stored blob_id this pod cannot parse is our bug, not the caller's.
		zap.L().Error("stored blob_id is not parseable", zap.String("blob_id", blob.GetBlobId()), zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	f, info, err := s.store.Open(key.Group, key.Namespace, key.Hash)
	if err != nil {
		// A record that points at bytes this pod does not hold. Anti-entropy is
		// what closes this, and it runs at start-up — so this line means a gap
		// opened since, which is worth an error even though the caller only sees
		// a 404.
		zap.L().Error("blob record has no bytes on this pod",
			zap.String("blob_id", blob.GetBlobId()), zap.Error(err))
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	// The headers the review fixed on (R3, R8), set before ServeContent so a Range
	// response carries them too.
	h := w.Header()
	h.Set("Content-Type", blob.GetContentType())
	h.Set("Content-Disposition", ContentDisposition(link.GetFilename()))
	// no-store AND private: no-store keeps it out of the browser's disk cache,
	// private keeps it out of any shared cache between here and there. A bearer
	// link is a credential in a URL; a cached copy outlives the revocation.
	h.Set("Cache-Control", "no-store, private")
	// The token is IN THE URL, so a referrer header would hand it to whatever the
	// document links to next.
	h.Set("Referrer-Policy", "no-referrer")
	// The content type is asserted by the Blob record and was checked against the
	// bytes at upload; sniffing could still override it in an old browser.
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Accept-Ranges", "bytes")

	if r.Method == http.MethodHead {
		// HEAD authorizes and answers headers, and takes NO metric (R3, R10).
		// ServeContent with a HEAD writes the headers and no body.
		http.ServeContent(w, r, link.GetFilename(), info.ModTime(), f)
		return
	}

	counted := &countingWriter{inner: w}
	http.ServeContent(responseWriterWith(w, counted), r, link.GetFilename(), info.ModTime(), f)
	s.stats.Record(tenant, link.GetTokenHash(), link.GetOwnerPrincipal(), link.GetNamespace(), counted.written)
}

// serveUpload receives the bytes for one upload, mirrors them to the peer, and
// promotes and records them only when both pods have them (R2, R9).
func (s *Service) serveUpload(w http.ResponseWriter, r *http.Request, rest string) {
	if r.Method != http.MethodPut {
		w.Header().Set("Allow", "PUT")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	conf := s.conf()
	if conf == nil {
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}

	tenant, token, ok := bearerPath(rest)
	if !ok {
		http.NotFound(w, r)
		return
	}

	// VERIFICATION IS A LOOKUP (R3, R9). The token is raw randomness; the row it
	// hashes to, in the tenant's store, is the grant, and the row's fields are the
	// upload's terms. There is no signature and no key.
	session, err := s.records.FetchSession(r.Context(), tenant, UploadTokenHash(token))
	if err != nil {
		// A stratum this pod cannot read is information about US, not about the
		// token — 503, same as serveBlob (R11).
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	if session == nil {
		// Unknown, cancelled-and-purged, swept: one 404, same as a bad link
		// token and for the same oracle reason.
		http.NotFound(w, r)
		return
	}
	if session.GetExpiresMs() <= s.now().UnixMilli() {
		http.NotFound(w, r)
		return
	}
	if session.GetState() != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		// Single use, durably (R9 step 3): the row leaves PENDING when the upload
		// settles. 409 rather than 404 — the caller holds a real token and is
		// being told the upload already happened, which is actionable, and the
		// token's own existence is not a secret from its holder.
		http.Error(w, "upload already completed", http.StatusConflict)
		return
	}
	// The row was written by this service and validated at CreateUpload. It is
	// re-checked here anyway because what comes next builds a filesystem path out
	// of it, and the cost of being wrong about that is a path traversal.
	if err := validateBlobKey(session.GetGroup(), session.GetNamespace(), session.GetContentHash()); err != nil {
		http.NotFound(w, r)
		return
	}
	uploadID := session.GetUploadId()
	if _, replayed := s.consumed.LoadOrStore(uploadID, struct{}{}); replayed {
		// The in-flight half of single use: the row above is still PENDING while
		// the first transfer streams.
		http.Error(w, "upload already completed", http.StatusConflict)
		return
	}

	vg, err := conf.groups.Lookup(session.GetGroup())
	if err != nil {
		// A live session for a group that is no longer configured. That is a
		// deployment state, not a bad token, so it is 503 and a log line rather
		// than a silent 404 — an operator who removed a group mid-flight needs to
		// see this.
		zap.L().Warn("upload session names a group that is not configured",
			zap.String("group", session.GetGroup()), zap.Error(err))
		s.consumed.Delete(uploadID)
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}

	// BOTH PODS OR NEITHER (R2). Asked before a byte of the body is read, so a
	// doomed upload does not first stream through the gateway.
	if !s.mirror.Healthy(r.Context()) {
		zap.L().Warn("refusing upload: peer is not reachable", zap.String("peer", s.mirror.Peer()))
		s.consumed.Delete(uploadID)
		http.Error(w, "blob store peer unavailable", http.StatusServiceUnavailable)
		return
	}

	blobID, status, err := s.receiveAndPromote(r, tenant, token, session, vg.GetMaxBlobBytes())
	if err != nil {
		// The reservation goes back and the session settles ABORTED: a failed
		// upload must not leave a tenant's quota spent (R9 step 3).
		s.quota.Release(uploadID, session.GetGroup(), session.GetNamespace())
		s.consumed.Delete(uploadID)
		s.abortSession(r, tenant, session)
		zap.L().Warn("upload failed", zap.String("upload_id", uploadID), zap.Error(err))
		http.Error(w, err.Error(), status)
		return
	}

	s.quota.Commit(uploadID, session.GetGroup(), session.GetNamespace())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	// A tiny hand-written JSON object rather than a marshalled struct: it has one
	// field, and the field is a validated blob id.
	fmt.Fprintf(w, "{\"blob_id\":%q}\n", blobID)
}

// receiveAndPromote is the upload's body: tee to the peer and the local temp file,
// check everything, promote on the peer first and locally second, then write the
// Blob row.
//
// THE PROMOTION ORDER IS THE WRITE RULE. The peer renames into place before it
// answers, so by the time this pod renames, both pods have the bytes fsynced — and
// the Blob record is written after both, so a record never points at bytes that
// are not on both pods (R9). The reverse order would make a window where this pod
// has a promoted file and a written record and the peer has nothing.
//
// If the peer promoted and this pod then failed, no record is written and the
// peer keeps a file nothing points at. That is harmless by construction: the file
// is named by its own hash, so a retry writes the identical bytes and the peer's
// copy is a dedup rather than a conflict.
func (s *Service) receiveAndPromote(
	r *http.Request,
	tenant, token string,
	session *blobstore_pb.UploadSession,
	maxBlobBytes int64,
) (blobID string, status int, err error) {
	tmp, err := s.store.TempFile(session.GetUploadId())
	if err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("creating temp file: %w", err)
	}
	mirrorW, finishMirror, abortMirror, err := s.mirror.ForwardUpload(r.Context(), tenant, token)
	if err != nil {
		s.store.DiscardTemp(tmp)
		return "", http.StatusServiceUnavailable, fmt.Errorf("opening peer stream: %w", err)
	}

	promoted := false
	defer func() {
		if !promoted {
			s.store.DiscardTemp(tmp)
		}
	}()

	size, digest, head, err := receive(io.MultiWriter(tmp, mirrorW), r.Body, maxBlobBytes)
	if err != nil {
		abortMirror()
		if errors.Is(err, ErrTooLarge) {
			return "", http.StatusRequestEntityTooLarge, err
		}
		return "", http.StatusBadRequest, fmt.Errorf("receiving upload: %w", err)
	}
	if size != session.GetSizeBytes() {
		abortMirror()
		return "", http.StatusBadRequest, fmt.Errorf("upload is %d bytes, declared %d", size, session.GetSizeBytes())
	}
	if digest != session.GetContentHash() {
		abortMirror()
		return "", http.StatusBadRequest, fmt.Errorf("upload hashes to %s, declared %s", digest, session.GetContentHash())
	}
	if err := CheckMagicBytes(session.GetContentType(), head); err != nil {
		abortMirror()
		return "", http.StatusBadRequest, err
	}

	// The peer runs every one of the checks above on its own copy of the stream,
	// reading the same session row this pod read. This waits for its verdict.
	if err := finishMirror(); err != nil {
		return "", http.StatusServiceUnavailable, err
	}

	if err := s.store.Promote(tmp, session.GetGroup(), session.GetNamespace(), session.GetContentHash()); err != nil {
		return "", http.StatusInternalServerError, err
	}
	promoted = true

	key := BlobKey{Group: session.GetGroup(), Namespace: session.GetNamespace(), Hash: session.GetContentHash()}
	nowMs := s.now().UnixMilli()
	blob := &blobstore_pb.Blob{
		BlobId:         key.String(),
		Group:          session.GetGroup(),
		ContentHash:    session.GetContentHash(),
		SizeBytes:      size,
		ContentType:    session.GetContentType(),
		CreatedMs:      nowMs,
		OwnerPrincipal: session.GetOwnerPrincipal(),
		Namespace:      session.GetNamespace(),
	}
	if err := s.records.PutBlob(r.Context(), tenant, blob); err != nil {
		// The bytes are on both pods and the record is not. Fail the upload: the
		// caller must not believe it has a blob it cannot find. The bytes stay,
		// and a retry of the same content promotes to the same path and writes
		// the row again.
		return "", http.StatusServiceUnavailable, err
	}

	// Re-read rather than settling the row this request started from: the sweep
	// or a CancelUpload may have moved it while the bytes were in flight.
	fresh, err := s.records.FetchSession(r.Context(), tenant, session.GetUploadId())
	if err != nil || fresh == nil {
		// The Blob row is written, which is what the caller needs. A session row
		// that could not be settled is an audit-trail gap and the expiry sweep
		// will settle it, so it is logged rather than failing a successful upload.
		zap.L().Warn("could not read the upload session to settle it",
			zap.String("upload_id", session.GetUploadId()), zap.Error(err))
		return blob.GetBlobId(), http.StatusCreated, nil
	}
	if err := s.settleSession(r.Context(), tenant, fresh, blobstore_pb.UploadState_UPLOAD_STATE_COMMITTED); err != nil {
		zap.L().Warn("could not settle the committed upload session",
			zap.String("upload_id", session.GetUploadId()), zap.Error(err))
	}
	return blob.GetBlobId(), http.StatusCreated, nil
}

// abortSession settles a failed upload's session, best effort: the reservation is
// already released in memory, and the expiry sweep settles the row if this does
// not.
func (s *Service) abortSession(r *http.Request, tenant string, session *blobstore_pb.UploadSession) {
	fresh, err := s.records.FetchSession(r.Context(), tenant, session.GetUploadId())
	if err != nil || fresh == nil {
		return
	}
	if fresh.GetState() != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
		return
	}
	if err := s.settleSession(r.Context(), tenant, fresh, blobstore_pb.UploadState_UPLOAD_STATE_ABORTED); err != nil {
		zap.L().Warn("could not settle the aborted upload session",
			zap.String("upload_id", session.GetUploadId()), zap.Error(err))
	}
}

// uploadTTL is the token lifetime from config.
func (c *runtimeConfig) uploadExpiry(now time.Time) int64 {
	return now.Add(c.uploadTTL).UnixMilli()
}
