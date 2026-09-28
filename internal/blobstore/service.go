package blobstore

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	blobstore_pb "gitlab.com/secondstarcavalier/schematic/platform/services/blobstore/model/types/services/blobstore"
	"go.uber.org/zap"
)

// Service is blobstore's whole behaviour: the RPC surface, the two byte paths,
// the peer paths, and the two background loops (the stats rollup and the session
// sweep).
//
// EVERYTHING IT DEPENDS ON IS INJECTED — store, mirror, records, clock. That is
// what makes the behaviours the decision doc asks for testable without a cluster:
// a test stands up two Services on two temp directories with a fake Records and
// exercises "write refused with one pod down", "hash mismatch cleans both pods",
// "anti-entropy rebuilds an empty PVC" against the real code paths.
type Service struct {
	store   *Store
	mirror  *Mirror
	records Records
	quota   *Quota
	stats   *Stats
	now     func() time.Time

	// cfg is swapped wholesale on every config delivery. An atomic pointer rather
	// than a mutex-guarded struct: every request reads it and none of them mutates
	// it, so a reader must never be able to see half of an update.
	cfg atomic.Pointer[runtimeConfig]

	// consumed is the IN-FLIGHT half of the single-use ledger for upload tokens
	// (R9 step 3). The durable half is the UploadSession row's own state, which
	// leaves PENDING when the upload settles — but it settles only at the END of
	// a transfer, so a second PUT arriving while the first is still streaming
	// would find the row PENDING. This map is what closes that window, and it is
	// per-pod and in memory because the window it covers is one pod's in-flight
	// request.
	consumed sync.Map // upload id (the token's sha256) -> struct{}
}

// runtimeConfig is the part of Config this service reads per request, resolved
// once per delivery so a request never parses config.
type runtimeConfig struct {
	groups       *Groups
	uploadTTL    time.Duration
	statsFlush   time.Duration
	sessionSweep time.Duration
}

// NewService builds the service. It does NOT read config: Configure does, and it
// is called by the generated handler before the server starts and again on every
// update.
func NewService(store *Store, mirror *Mirror, records Records, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		store:   store,
		mirror:  mirror,
		records: records,
		quota:   NewQuota(records),
		stats:   NewStats(records, now),
		now:     now,
	}
}

// Configure resolves a delivered Config into the snapshot requests read.
//
// A BAD CONFIG IS AN ERROR, not a partial application: the generated lifecycle
// refuses to start on the first call's error and keeps the previous config on a
// later one, which is exactly the behaviour a bad group rendering should get. A
// cluster whose config has no groups at all is NOT an error — it accepts no
// uploads, which is what "no group, no upload" means (R4) — but it is worth a
// line in the log, because a cluster that expected groups and has none looks
// identical to a broken upload path from the outside.
func (s *Service) Configure(cfg *blobstore_pb.Config) error {
	groups, err := NewGroups(cfg.GetVolumeGroups())
	if err != nil {
		return err
	}
	s.cfg.Store(&runtimeConfig{
		groups:       groups,
		uploadTTL:    time.Duration(cfg.GetUploadTokenTtlSeconds()) * time.Second,
		statsFlush:   time.Duration(cfg.GetStatsFlushIntervalSeconds()) * time.Second,
		sessionSweep: time.Duration(cfg.GetSessionSweepIntervalSeconds()) * time.Second,
	})
	if groups.Len() == 0 {
		zap.L().Warn("no volume groups are configured; every upload will be refused until a consuming service declares one")
	} else {
		zap.L().Info("volume groups configured", zap.Int("groups", groups.Len()))
	}
	return nil
}

// conf returns the current snapshot, or nil before the first Configure.
func (s *Service) conf() *runtimeConfig { return s.cfg.Load() }

// SyncOnStart runs the anti-entropy pass before the pod serves reads (R2).
//
// A PEER THAT CANNOT BE REACHED IS A WARNING, NOT A FAILURE, and the reasoning
// matters: the doc says a pod with an empty or stale PVC "lists its peer's hashes,
// copies what it lacks, and only then accepts reads", which is an ordering
// constraint on a REACHABLE peer. If the peer is down, refusing to serve would
// mean that after losing one pod the survivor could never restart into service —
// the cluster would go from "uploads fail, reads work" to "nothing works", which
// is the opposite of what the write rule is protecting. So a missing peer is
// logged loudly and this pod serves the blobs it has; a read for a blob it lacks
// is a 404 either way.
func (s *Service) SyncOnStart(ctx context.Context) {
	copied, err := s.mirror.SyncFromPeer(ctx, s.store)
	if err != nil {
		zap.L().Warn("anti-entropy could not reach the peer; serving the blobs this pod holds",
			zap.String("peer", s.mirror.Peer()), zap.Int("copied", copied), zap.Error(err))
		return
	}
	if copied > 0 {
		zap.L().Info("anti-entropy filled gaps from the peer",
			zap.String("peer", s.mirror.Peer()), zap.Int("copied", copied))
	}
}

// RunBackground drives the two loops until ctx is done: the stats rollup (R10)
// and the expired-session sweep (R9 step 4). It blocks, so callers run it on the
// ServiceManager's task group.
func (s *Service) RunBackground(ctx context.Context) {
	conf := s.conf()
	if conf == nil {
		zap.L().Error("background loops started before config was delivered")
		return
	}
	statsTick := time.NewTicker(conf.statsFlush)
	defer statsTick.Stop()
	sweepTick := time.NewTicker(conf.sessionSweep)
	defer sweepTick.Stop()

	for {
		select {
		case <-ctx.Done():
			// One last flush on the way out, so a graceful shutdown does not throw
			// away a whole interval's counters.
			s.stats.Flush(context.WithoutCancel(ctx))
			return
		case <-statsTick.C:
			s.stats.Flush(ctx)
		case <-sweepTick.C:
			s.sweepSessions(ctx)
		}
	}
}

// sweepSessions settles sessions whose token has expired: it releases the quota
// reservation and flags the row ABORTED. Temp files are swept at the same time,
// which is the other half of "start-up sweeps temp files" — a pod that has been
// up for a week has had crashes too.
func (s *Service) sweepSessions(ctx context.Context) {
	if err := s.store.SweepTemps(); err != nil {
		zap.L().Warn("failed to sweep temp files", zap.Error(err))
	}
	sessions, err := s.records.ListExpiredSessions(ctx, s.now().UnixMilli())
	if err != nil {
		zap.L().Warn("failed to list expired upload sessions", zap.Error(err))
		return
	}
	for _, sess := range sessions {
		if sess.GetState() != blobstore_pb.UploadState_UPLOAD_STATE_PENDING {
			continue
		}
		s.quota.Release(sess.GetUploadId(), sess.GetGroup(), sess.GetNamespace())
		if err := s.settleSession(ctx, sess, blobstore_pb.UploadState_UPLOAD_STATE_ABORTED); err != nil {
			zap.L().Warn("failed to settle expired upload session",
				zap.String("upload_id", sess.GetUploadId()), zap.Error(err))
		}
	}
}

// settleSession republishes a session row in a terminal state. The row is flagged
// deleted at the same time: the purge clock
// (residency.deleted_max_age_seconds) is what keeps the reservation ledger
// bounded, and a settled session has no reader left.
func (s *Service) settleSession(ctx context.Context, sess *blobstore_pb.UploadSession, state blobstore_pb.UploadState) error {
	next := &blobstore_pb.UploadSession{
		UploadId:       sess.GetUploadId(),
		Group:          sess.GetGroup(),
		ContentHash:    sess.GetContentHash(),
		SizeBytes:      sess.GetSizeBytes(),
		ContentType:    sess.GetContentType(),
		ReservedBytes:  0,
		State:          state,
		CreatedMs:      sess.GetCreatedMs(),
		ExpiresMs:      sess.GetExpiresMs(),
		OwnerPrincipal: sess.GetOwnerPrincipal(),
		Deleted:        true,
		Namespace:      sess.GetNamespace(),
	}
	return s.records.PublishSession(ctx, next)
}

// Store, Mirror and Stats are exposed for the tests that assert on what landed on
// disk and what the peer was asked for. Nothing in the service reaches for them
// through these.
func (s *Service) Store() *Store   { return s.store }
func (s *Service) Mirror() *Mirror { return s.mirror }
func (s *Service) Stats() *Stats   { return s.stats }

// SelfHostname is the pod's hostname, used to pick this pod out of the configured
// peer list. HOSTNAME is read first because that is what Kubernetes sets to the
// pod name; os.Hostname is the fallback for a local run.
func SelfHostname() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// PeersFromEnvOrConfig returns the peer list, preferring BLOBSTORE_PEERS.
//
// The env var is the operator's delivery path — the same shape stratum's
// STRATUM_SEED_MEMBERS uses, because peer wiring is deployment topology the
// operator already knows. Config.peers is the local-run and test path. Two
// sources with a stated precedence, rather than a merge: a merge of two topology
// lists is a topology neither side declared.
func PeersFromEnvOrConfig(cfg *blobstore_pb.Config) []string {
	if env := os.Getenv("BLOBSTORE_PEERS"); env != "" {
		return splitPeers(env)
	}
	return cfg.GetPeers()
}

func splitPeers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// httpClientForPeer is the client the mirror uses.
//
// NO OVERALL TIMEOUT, deliberately: http.Client.Timeout covers the whole
// request INCLUDING the streamed body, so any value at all would kill a large
// upload on a slow link at that wall clock. The bounded pieces are what matter and
// they are set — dial, TLS-less handshake, and response headers — plus the
// request context, which the gateway's own timeout and the client hanging up both
// cancel.
func httpClientForPeer() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   8,
		},
	}
}
