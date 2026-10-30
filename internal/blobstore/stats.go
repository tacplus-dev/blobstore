package blobstore

import (
	"context"
	"sync"
	"time"

	blobstore_pb "github.com/tacplus-dev/blobstore/tacplus/types/services/blobstore"
	"go.uber.org/zap"
)

// Stats is the resolution counter (R10): per-token counts held in memory on the
// pod that served the request, flushed to BlobLinkStats at most once per flush
// interval per token.
//
// WHY IN MEMORY. A `/b/` resolve must not cost a record write. A download is the
// hot path and the number it increments is a metric, not an accounting entry —
// nobody needs it exact, and paying a stratum round-trip per byte range a PDF
// viewer asks for would be paying it many times per document.
//
// WHAT IS COUNTED. `resolutions` counts requests that served bytes. A HEAD does
// NOT count — the doc says so in as many words: "HEAD authorizes and answers
// headers, no metric". `bytes_served` counts what actually went out, which for a
// Range request is the range and not the file; it is honest here in a way it could
// not have been in the v2 signed-URL design, where we could only count URLs
// handed out.
//
// WHAT IS LOST. Whatever has not been flushed when a pod dies. That is the
// accepted cost of the design, and the alternative is the write per resolve this
// exists to avoid.
type Stats struct {
	records Records
	now     func() time.Time

	mu      sync.Mutex
	pending map[string]*statsEntry
}

// statsEntry is one token's unflushed delta plus the identity the row needs. The
// owner and namespace are carried because a flush may be the row's FIRST write
// and a record cannot be written without them.
type statsEntry struct {
	ownerPrincipal string
	namespace      string
	resolutions    int64
	bytesServed    int64
	lastResolvedMs int64
}

func NewStats(records Records, now func() time.Time) *Stats {
	if now == nil {
		now = time.Now
	}
	return &Stats{records: records, now: now, pending: map[string]*statsEntry{}}
}

// Record adds one resolution. Called after the bytes are on their way out, not
// before: a request that failed to serve is not a resolution.
func (s *Stats) Record(tokenHash, ownerPrincipal, namespace string, bytesServed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.pending[tokenHash]
	if e == nil {
		e = &statsEntry{ownerPrincipal: ownerPrincipal, namespace: namespace}
		s.pending[tokenHash] = e
	}
	e.resolutions++
	e.bytesServed += bytesServed
	e.lastResolvedMs = s.now().UnixMilli()
}

// Flush writes every pending delta and clears it.
//
// READ-MODIFY-WRITE, not write. The other pod flushes the same row, so a blind
// write would discard whatever it last contributed. Reading first, and writing
// only if the row is still what was read, means a flush can only ever move the
// counters forward — it cannot make the row the SUM at any instant (nothing
// could, for an in-memory counter on two pods), but it never loses the other
// pod's work, even when the two flush at once.
//
// A delta whose flush fails is PUT BACK, so a stratum blip costs a delay rather
// than a count. The one thing it does not do is retry inside the flush: the next
// tick is the retry, and blocking here would stall every other token's flush
// behind one bad row.
func (s *Stats) Flush(ctx context.Context) {
	s.mu.Lock()
	pending := s.pending
	s.pending = map[string]*statsEntry{}
	s.mu.Unlock()

	for tokenHash, delta := range pending {
		if err := s.flushOne(ctx, tokenHash, delta); err != nil {
			zap.L().Warn("failed to flush link stats; delta kept for the next tick", zap.Error(err))
			s.restore(tokenHash, delta)
		}
	}
}

func (s *Stats) flushOne(ctx context.Context, tokenHash string, delta *statsEntry) error {
	current, err := s.records.FetchLinkStats(ctx, tokenHash)
	if err != nil {
		return err
	}
	row := &blobstore_pb.BlobLinkStats{
		TokenHash:      tokenHash,
		Resolutions:    current.GetResolutions() + delta.resolutions,
		BytesServed:    current.GetBytesServed() + delta.bytesServed,
		LastResolvedMs: delta.lastResolvedMs,
		OwnerPrincipal: delta.ownerPrincipal,
		Namespace:      delta.namespace,
	}
	// A row the other pod wrote more recently keeps its timestamp: "last resolved"
	// is the later of the two, not the later of the two to flush.
	if current.GetLastResolvedMs() > row.LastResolvedMs {
		row.LastResolvedMs = current.GetLastResolvedMs()
	}
	// Conditional on the row read above: if the other pod flushed in between,
	// the write is refused (ErrRecordExists, ErrRecordChanged) and the delta is
	// kept for the next tick, which reads the other pod's flush and adds to it.
	if current == nil {
		return s.records.InsertLinkStats(ctx, row)
	}
	return s.records.ReplaceLinkStats(ctx, current, row)
}

// restore merges a failed delta back into pending, adding to whatever arrived
// while the flush was in flight rather than overwriting it.
func (s *Stats) restore(tokenHash string, delta *statsEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.pending[tokenHash]
	if e == nil {
		s.pending[tokenHash] = delta
		return
	}
	e.resolutions += delta.resolutions
	e.bytesServed += delta.bytesServed
	if delta.lastResolvedMs > e.lastResolvedMs {
		e.lastResolvedMs = delta.lastResolvedMs
	}
}

// PendingFor returns the unflushed delta for one token, so GetLinkStats can add
// it to the stored row and answer with what this pod knows rather than only with
// what it has written. Without it a caller reading its own stats immediately
// after a download sees nothing for up to a full flush interval.
func (s *Stats) PendingFor(tokenHash string) (resolutions, bytesServed, lastResolvedMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.pending[tokenHash]
	if e == nil {
		return 0, 0, 0
	}
	return e.resolutions, e.bytesServed, e.lastResolvedMs
}
