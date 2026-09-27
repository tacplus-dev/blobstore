package blobstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Peer paths. They live on the peer listener, never on the public one: the
// hashes listing enumerates every blob this pod holds, for every tenant, and the
// gateway proxies only `/upload/` and `/b/`.
const (
	PeerPathUpload = "/peer/upload/"
	PeerPathHashes = "/peer/hashes"
	PeerPathBlob   = "/peer/blob/"
	PeerPathHealth = "/peer/healthz"
)

// peerHealthTimeout bounds the pre-flight check a PUT makes before it reads a
// byte of the client's body.
//
// THE PRE-FLIGHT IS WHY IT EXISTS. An upload is acked only when both pods have
// the bytes (R2), so a PUT that arrives while the peer is down is going to fail.
// Discovering that after streaming 50 MiB through the gateway wastes the transfer
// and takes a gateway connection for its duration; asking first costs one
// round-trip on a healthy cluster. The streaming request's own failure is still
// handled — this only makes the common case of "peer is simply down" fast and
// unambiguous.
const peerHealthTimeout = 2 * time.Second

// Mirror is this pod's relationship with the other one: the peer address, a
// client for forwarding uploads and for anti-entropy, and the anti-entropy sync
// itself.
//
// TWO PODS, AND THE CODE SAYS SO. `peers` from config lists every replica
// including this pod, and Peer() is the one that is not this pod. A third replica
// is not silently half-supported: NewMirror refuses a list it cannot reduce to
// exactly one peer, because "stream to my peer" and "stream to all my peers" are
// different protocols and the design specifies the first (R2).
type Mirror struct {
	peer   string
	client *http.Client
}

// NewMirror picks this pod's peer out of the configured list. `self` is the
// pod's hostname; a peer entry matches it when the entry's first DNS label is
// that hostname, which is the shape the operator renders
// (`blobstore-0.blobstore:8081`).
func NewMirror(peers []string, self string, client *http.Client) (*Mirror, error) {
	if client == nil {
		client = &http.Client{}
	}
	var others []string
	sawSelf := false
	for _, p := range peers {
		if peerIsSelf(p, self) {
			sawSelf = true
			continue
		}
		others = append(others, p)
	}
	if !sawSelf {
		return nil, fmt.Errorf("blobstore: peers %v does not include this pod (%s); the operator renders every replica's address, this pod's included", peers, self)
	}
	if len(others) != 1 {
		return nil, fmt.Errorf("blobstore: peers %v leaves %d peers after removing this pod (%s); the mirror is a PAIR", peers, len(others), self)
	}
	return &Mirror{peer: others[0], client: client}, nil
}

// peerIsSelf compares a `host:port` peer entry's first DNS label against this
// pod's hostname.
func peerIsSelf(peer, self string) bool {
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	label := host
	if i := strings.IndexByte(host, '.'); i >= 0 {
		label = host[:i]
	}
	return label == self
}

// Peer is the peer's `host:port`.
func (m *Mirror) Peer() string { return m.peer }

func (m *Mirror) url(path string) string { return "http://" + m.peer + path }

// Healthy reports whether the peer is answering. A false answer is what turns a
// PUT into 503 (R2).
func (m *Mirror) Healthy(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, peerHealthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url(PeerPathHealth), nil)
	if err != nil {
		return false
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return false
	}
	defer drainAndClose(resp.Body)
	return resp.StatusCode == http.StatusOK
}

// ForwardUpload starts a streaming PUT of the same token to the peer and returns
// the writer the caller tees the client's bytes into, plus a function that waits
// for the peer's verdict.
//
// THE BODY IS A PIPE, so nothing is buffered twice: the bytes the caller writes
// go straight out on the peer connection as they arrive from the client (R2 —
// "streams the bytes to its peer while writing them locally, no second
// buffering").
//
// The caller MUST call either finish (normal end of stream) or abort (any
// failure). abort closes the pipe with an error, which truncates the peer's body
// read and makes the peer discard its own temp — the peer never sees a
// short stream as a complete one, because it checks size and hash like the public
// path does.
func (m *Mirror) ForwardUpload(ctx context.Context, token string) (w io.Writer, finish func() error, abort func(), err error) {
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, m.url(PeerPathUpload+token), pr)
	if err != nil {
		pr.Close()
		pw.Close()
		return nil, nil, nil, err
	}
	// The length is unknown to the transport: this is a streamed forward and the
	// peer reads to EOF. Chunked is what http.Client uses for a body with no
	// ContentLength, which is what we want.
	req.Header.Set("Content-Type", "application/octet-stream")

	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := m.client.Do(req)
		if err != nil {
			// Unblock the writer: without this a peer that died mid-upload leaves
			// the tee blocking on a pipe nobody reads.
			pr.CloseWithError(err)
			done <- result{err: err}
			return
		}
		defer drainAndClose(resp.Body)
		done <- result{status: resp.StatusCode}
	}()

	finish = func() error {
		if err := pw.Close(); err != nil {
			return fmt.Errorf("closing peer upload stream: %w", err)
		}
		r := <-done
		if r.err != nil {
			return fmt.Errorf("peer upload to %s failed: %w", m.peer, r.err)
		}
		if r.status != http.StatusOK && r.status != http.StatusCreated {
			return fmt.Errorf("peer upload to %s answered %d", m.peer, r.status)
		}
		return nil
	}
	abort = func() {
		pw.CloseWithError(errors.New("upload aborted"))
		<-done
	}
	return pw, finish, abort, nil
}

// DeleteOnPeer asks the peer to unlink a blob's bytes. A failure is reported, not
// swallowed: a delete that only happened on one pod leaves bytes the owner
// believes are gone.
func (m *Mirror) DeleteOnPeer(ctx context.Context, k BlobKey) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, m.url(PeerPathBlob+k.String()), nil)
	if err != nil {
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("deleting %s on peer %s: %w", k, m.peer, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer %s answered %d deleting %s", m.peer, resp.StatusCode, k)
	}
	return nil
}

// SyncFromPeer is the anti-entropy pass (R2). It exists for ONE case: a lost or
// replaced disk. It is not a repair loop and does not run on a timer — a file
// either has its hash or does not, so there is nothing to reconcile on a healthy
// pod beyond what it is missing, and what it is missing only changes when a PVC
// is replaced or an upload half-failed.
//
// It returns the number of blobs copied. A peer that cannot be reached is
// reported as an error and the CALLER decides: at start-up that is a warning and
// the pod serves anyway, because refusing to serve would mean a pod whose peer
// died can never come back to serve the reads it does hold. See Service.Start.
func (m *Mirror) SyncFromPeer(ctx context.Context, store *Store) (int, error) {
	want, err := m.listPeerHashes(ctx)
	if err != nil {
		return 0, err
	}
	copied := 0
	for _, k := range want {
		if store.Has(k.Group, k.Namespace, k.Hash) {
			continue
		}
		if err := m.copyOne(ctx, store, k); err != nil {
			return copied, err
		}
		copied++
	}
	return copied, nil
}

func (m *Mirror) listPeerHashes(ctx context.Context) ([]BlobKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url(PeerPathHashes), nil)
	if err != nil {
		return nil, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing peer %s hashes: %w", m.peer, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s answered %d listing hashes", m.peer, resp.StatusCode)
	}
	var out []BlobKey
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		k, err := ParseBlobKey(line)
		if err != nil {
			// A line this pod cannot parse is the peer's bug, not a reason to
			// abandon the whole sync.
			zap.L().Warn("peer listed an unparseable blob key", zap.Error(err))
			continue
		}
		out = append(out, k)
	}
	return out, sc.Err()
}

func (m *Mirror) copyOne(ctx context.Context, store *Store, k BlobKey) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url(PeerPathBlob+k.String()), nil)
	if err != nil {
		return err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s from peer %s: %w", k, m.peer, err)
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer %s answered %d for %s", m.peer, resp.StatusCode, k)
	}
	// WriteFrom re-hashes: the peer's naming is not trusted.
	return store.WriteFrom(k, resp.Body)
}

// drainAndClose releases a response body so the connection can be reused. The
// drain is bounded: an unbounded io.Copy here would let a misbehaving peer hold
// this goroutine open.
func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
	_ = body.Close()
}
