package blobstore

import (
	"fmt"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

// PeerHandler is the mirror-and-anti-entropy listener (R2).
//
// IT IS A SEPARATE LISTENER, not more prefixes on the public one, because
// /peer/hashes enumerates every blob this pod holds for every tenant. The gateway
// proxies only /upload/ and /b/, so this port has no route from outside the
// cluster at all.
//
// AUTHORIZATION ON THE MIRROR PATH IS THE TOKEN ITSELF. A peer forwards the same
// signed upload token it received, and this pod verifies the signature rather
// than trusting the hop — so a peer cannot ask its peer to store bytes that no
// CreateUpload authorized, even if the peer itself were compromised. The listing
// and fetch paths carry no credential: they are reachable only from inside the
// cluster, and the thing they protect is enumeration rather than content (a
// caller who can reach this port can already reach the upload path).
func (s *Service) PeerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(PeerPathHealth, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc(PeerPathHashes, s.servePeerHashes)
	mux.HandleFunc(PeerPathBlob, s.servePeerBlob)
	mux.HandleFunc(PeerPathUpload, func(w http.ResponseWriter, r *http.Request) {
		s.servePeerUpload(w, r, strings.TrimPrefix(r.URL.Path, PeerPathUpload))
	})
	return mux
}

// servePeerHashes answers the anti-entropy listing: one `<group>/<ns>/<sha256>`
// per line.
//
// The format is deliberately the blob_id form and nothing else — no sizes, no
// timestamps, no JSON. A peer filling a replaced disk needs exactly the set of
// names, and it re-hashes every file it fetches, so anything else on the line
// would be data the receiver must not trust anyway.
func (s *Service) servePeerHashes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	keys, err := s.store.List()
	if err != nil {
		zap.L().Error("failed to list local blobs for a peer", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, k := range keys {
		fmt.Fprintln(w, k.String())
	}
}

// servePeerBlob serves or deletes one blob's bytes for the peer.
func (s *Service) servePeerBlob(w http.ResponseWriter, r *http.Request) {
	key, err := ParseBlobKey(strings.TrimPrefix(r.URL.Path, PeerPathBlob))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		f, info, err := s.store.Open(key.Group, key.Namespace, key.Hash)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, key.Hash, info.ModTime(), f)
	case http.MethodDelete:
		// Idempotent: Store.Remove treats a missing file as done, because
		// DeleteBlob may be retried and the peer may already have unlinked it.
		if err := s.store.Remove(key.Group, key.Namespace, key.Hash); err != nil {
			zap.L().Error("failed to remove a blob for the peer", zap.Error(err))
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

// servePeerUpload is the mirror sink: receive the forwarded stream, run the SAME
// checks the public path runs, and promote.
//
// IT WRITES NO RECORD AND FORWARDS NOTHING. The pod that received the client's
// PUT owns the record write and is the only one that talks to stratum for this
// upload; forwarding again would be a loop, and a second write would be a
// second Blob write for one upload.
func (s *Service) servePeerUpload(w http.ResponseWriter, r *http.Request, rest string) {
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

	// The same lookup the public path makes, against the same row in the same
	// tenant's store: the path is the public one's, `<tid>/<token>`. The hop is not
	// trusted: this pod resolves the token itself rather than believing terms
	// another pod sent it. The cost is one stratum read per mirrored upload —
	// which is new, and is what the old self-describing token bought.
	tenant, token, ok := bearerPath(rest)
	if !ok {
		http.NotFound(w, r)
		return
	}
	session, err := s.records.FetchSession(r.Context(), tenant, UploadTokenHash(token))
	if err != nil {
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}
	if session == nil {
		http.NotFound(w, r)
		return
	}
	if err := validateBlobKey(session.GetGroup(), session.GetNamespace(), session.GetContentHash()); err != nil {
		http.NotFound(w, r)
		return
	}
	// Expiry is NOT re-checked here, and that is deliberate: the public path
	// checked it when the client's request arrived, and a large upload can
	// legitimately still be streaming after the token's expiry — failing it at
	// minute 16 having transferred everything is the behaviour UploadSession's own
	// comment rules out. The row's STATE is not checked either: it is still
	// PENDING for the whole of the legitimate hop, so a check here would only
	// duplicate the public path's, which has already run.
	vg, err := conf.groups.Lookup(session.GetGroup())
	if err != nil {
		http.Error(w, "", http.StatusServiceUnavailable)
		return
	}

	tmp, err := s.store.TempFile(session.GetUploadId())
	if err != nil {
		zap.L().Error("failed to create a temp file for a mirrored upload", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	promoted := false
	defer func() {
		if !promoted {
			s.store.DiscardTemp(tmp)
		}
	}()

	size, digest, head, err := receive(tmp, r.Body, vg.GetMaxBlobBytes())
	switch {
	case err != nil:
		zap.L().Warn("mirrored upload failed while receiving", zap.Error(err))
		http.Error(w, "", http.StatusBadRequest)
		return
	case size != session.GetSizeBytes():
		http.Error(w, "", http.StatusBadRequest)
		return
	case digest != session.GetContentHash():
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err := CheckMagicBytes(session.GetContentType(), head); err != nil {
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	// Promote BEFORE answering: the answer is what tells the other pod the bytes
	// are fsynced and in place here, and that is what it waits for before
	// promoting its own copy (R9 step 3).
	if err := s.store.Promote(tmp, session.GetGroup(), session.GetNamespace(), session.GetContentHash()); err != nil {
		zap.L().Error("failed to promote a mirrored upload", zap.Error(err))
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	promoted = true
	w.WriteHeader(http.StatusOK)
}
