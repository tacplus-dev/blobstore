// blobstore — the platform's binary blob primitive. See
// looper/schematic/decisions/blob-store-2026-09-25.md (v3) and
// platform/proto/services/blobstore.proto.
//
// WHY GO. Two reasons, both about matching what the fleet already does:
// letsencrypt is the fleet's existing example of exactly this shape — a Go
// service that keeps its records in stratum and runs a plain HTTP listener beside the generated s2s surface — and the
// operator, the other piece this work touches, is Go too. The byte paths also
// want nothing but the standard library: streaming bodies, SHA-256 on the stream,
// fsync of a file and of its directory, and an atomic rename are all stdlib in Go,
// where the fleet's Rust services each pull in an async stack to get there.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	internal "github.com/tacplus-dev/blobstore/internal/blobstore"
	loggerchassis "github.com/tacplus-dev/blobstore/tacplus/chassis/logger"
	sm "github.com/tacplus-dev/blobstore/tacplus/s2sm"
	host_pb "github.com/tacplus-dev/blobstore/tacplus/types/sys/host"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("blobstore: %v", err)
	}
}

func run() error {
	// The chassis logger, not zap directly: the chassis wraps the sink in a
	// redactor. This service handles credentials in URL paths — an upload token
	// and a `/b/` bearer token — so an error path that logged a request URL would
	// log a credential. Same reason letsencrypt and kmaker route through it.
	logger, err := loggerchassis.InstallFromConfig(nil)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync() //nolint:errcheck

	s, err := sm.New()
	if err != nil {
		return fmt.Errorf("servicemanager: %w", err)
	}
	defer s.Close()

	// The typed Config comes from kernel, never from a file. The operator seeds
	// blobstore's entry in kernel's services holder, and kernel republishes that
	// entry to this service's own BlobstoreConfigState boundary; bootstrap.yaml
	// carries only the identity needed to reach kernel. letsencrypt's
	// service.yaml is the fleet's ONE exception to that, not its model.
	//
	// Fetch, not Get: Get reads the Watch cache, which is empty until the first
	// message lands, so on a cold boot it returns nil — that nil is what
	// crash-looped. Fetch is the blocking RPC against the state authority, and it
	// already retries with the watch-restart backoff for
	// config_fetch_timeout_seconds while a fetched value fails its own generated
	// validation, which is what covers a boot racing kernel's own convergence.
	// Nothing to hand-roll here: the generated wrapper is the timeout idiom.
	ctx := context.Background()
	cfg, err := s.StateClients().Services().BlobstoreConfigState().Config().Fetch(ctx)
	if err != nil {
		return fmt.Errorf("fetching blobstore config from BlobstoreConfigState: %w", err)
	}

	store, err := internal.NewStore(cfg.GetDataDir())
	if err != nil {
		return err
	}
	self := internal.SelfHostname()
	mirror, err := internal.NewMirror(internal.PeersFromEnvOrConfig(cfg), self, nil)
	if err != nil {
		return err
	}
	zap.L().Info("blobstore mirror",
		zap.String("self", self), zap.String("peer", mirror.Peer()), zap.String("data_dir", store.Root()))

	// Reads and writes alike go to stratum's record plane: its write surface is
	// the only way a row reaches the store (#6).
	records := internal.NewRecords(s.Grpc().Clients().Vservices().RecordMapClient())

	// The sweep walks the tenants kernel reports a Stratum for: each tenant's
	// sessions are in its own store (#8).
	tenants := internal.StratumTenants(s.StateClients().Sys().ServiceHealth().Proto)

	svc := internal.NewService(store, mirror, records, tenants, time.Now)
	handler := internal.NewHandler(svc)

	// The config the generated lifecycle will deliver is the authority, but the
	// two listeners are bound before Run() blocks, so Configure has to have run
	// once already — the token key and the groups are needed by the first request
	// either listener accepts.
	if err := svc.Configure(cfg); err != nil {
		return err
	}

	// Anti-entropy BEFORE the listeners bind (R2): a pod with a replaced disk
	// fills from its peer and only then accepts reads.
	svc.SyncOnStart(ctx)

	if err := serveHTTP("bytes", cfg.GetHttpListener(), svc.BytesHandler()); err != nil {
		return err
	}
	if err := serveHTTP("peer", cfg.GetPeerListener(), svc.PeerHandler()); err != nil {
		return err
	}

	// The rollup and the sweep, on the ServiceManager's task group so they stop
	// with the service rather than outliving it.
	s.GoTask(svc.RunBackground)

	// Run() blocks: it serves the generated gRPC surface, delivers config, drives
	// health, and handles shutdown. Unlike letsencrypt this service DOES call it —
	// it has RPCs to serve — so SetServing is Run's job and is not called here.
	return s.Run(handler)
}

// serveHTTP binds one plaintext listener and serves it in the background.
//
// BOUND HERE RATHER THAN INSIDE THE GOROUTINE, so a port already in use kills
// start-up instead of being lost in a background task — the same reason the
// gateway binds its port-80 listener before the TLS one.
//
// PLAINTEXT is correct for both: the bytes listener is reached only through the
// gateway, which terminates the client's TLS, and the peer listener is reached
// only from the other pod. Neither has a route from outside the cluster. Giving
// them the service's own TLS identity would be TLS between two processes that are
// already only reachable from inside the cluster, and it is not what the design
// asks for.
func serveHTTP(name string, h *host_pb.Host, handler http.Handler) error {
	if h == nil {
		return fmt.Errorf("blobstore: no %s listener in config", name)
	}
	addr := net.JoinHostPort(h.GetHost(), fmt.Sprintf("%d", h.GetPort()))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("binding %s listener on %s: %w", name, addr, err)
	}
	srv := &http.Server{
		Handler: handler,
		// No WriteTimeout and no ReadTimeout: both are wall-clock caps on the
		// whole request, and this server's requests are a 50 MiB upload and a 50
		// MiB download. ReadHeaderTimeout is the one that bounds the attack this
		// would otherwise be open to — a client that opens a connection and never
		// finishes its request line.
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			zap.L().Error("http listener stopped", zap.String("listener", name), zap.Error(err))
		}
	}()
	zap.L().Info("listening", zap.String("listener", name), zap.String("addr", addr))
	return nil
}
