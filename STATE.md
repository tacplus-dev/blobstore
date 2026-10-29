# State: blobstore

Overwritten to stay current (loopers and agents). Last updated: 2026-10-29 by looper (#7).

- **Version / line:** unreleased (no vX.Y.Z tag; v0.1.0 is #2); tacplus floor 2.5 (tacplus.yaml, #7)
- **Works:** `just gates` passes with tacplus 2.5.1 run where `go` is installed (generate, no tracked generated files, gofmt, vet, build, test)
- **Released / deployed:** nowhere; not yet in seatrials
- **Known gaps:** `just gates` in the looper stops at goimports because the compiler image has no `go` (tacplus-dev/tacplus#188); vulnerable grpc/x/net/x/text pins (#4); SeaBeam paths (#3); records still published on the bus for Stratum (#6)
- **Blocked on:** tacplus-dev/tacplus#188 (gates in the looper)
