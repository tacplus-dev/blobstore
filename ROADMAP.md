# Roadmap: blobstore

Owned by Jody; changed only by an agent at Jody's request. Loopers propose in PROPOSALS.md.

## Now
The platform's binary blob primitive: a two-pod mirror of content-addressed files, metadata in Stratum records.
Ported from schematic 2026-10-23. Unreleased; tacplus floor 2.4 (declared in tacplus.yaml; #1 still open). Not yet in seatrials.

## Next
- Nothing unblocked; see Later.

## Later
- Declare the tacplus floor 2.4 (#1), blocked by tacplus-dev/tacplus#188
- Work without SeaBeam: no gitlab.com/seabeam paths, sources or registries (#3), blocked by #1
- Patch gRPC (to >=1.83.2) and golang.org/x dependencies (#4), blocked by #1
- Base image off SeaBeam's private schematic-alpine-1-2 to Alpine 3.24 (#5), blocked by #1
- Release v0.1.0 and publish its image (#2), blocked by tacplus-dev/looper#12 (needs Jody's decision), #1
- Decide whether blobstore is a platform service; if so, port and prove it in seatrials (tacplus-dev/roadmap:decisions/tacplus-dev-stable-gate-2026-10-25.md)
- Write its records to Stratum directly, not by publishing on the bus (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md)

## Not doing
- A cloud object store or signed cloud URLs: self-hosted by design (blob-store-2026-09-25 v3, cited in the README)
