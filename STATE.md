# State: blobstore

Overwritten to stay current (loopers and agents). Last updated: 2026-10-29 by looper (#5).

- **Version / line:** unreleased (no vX.Y.Z tag; v0.1.0 is #2); tacplus floor 2.5 (tacplus.yaml, #7)
- **Works:** `just gates` passes with tacplus 2.5.1 run where `go` is installed (generate, no tracked generated files, no upstream references, gofmt, vet, build, test), also with gitlab.com unreachable; `govulncheck ./...` (source and module mode) finds no vulnerabilities (#4); the release image builds on the public `alpine:3.24` base (`BASE_IMAGE` build argument) and the real binary starts in it (#5)
- **Module / sources:** `github.com/tacplus-dev/blobstore` (go 1.26.0; grpc v1.83.2, x/net v0.62.0, x/text v0.43.0, x/sys v0.49.0, letsencrypt's pins, #4), generated package under `tacplus/` (pkg derived from go.mod); own proto in `proto/`, dependencies' protos from tacplus-dev via `depends` (#3)
- **Released / deployed:** nowhere; not yet in seatrials
- **Known gaps:** `just gates` in the looper stops at goimports because the compiler image has no `go` (tacplus-dev/tacplus#188); records still published on the bus for Stratum (#6)
- **Blocked on:** tacplus-dev/tacplus#188 (gates in the looper)
