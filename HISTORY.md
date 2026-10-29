# History: blobstore

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-29 looper issue Gates fail on main: stratum needs tacplus 2.5; move the floor from 2.4 to 2.5 (#7, b729a4e)
- 2026-10-29 looper issue Work without SeaBeam: no gitlab.com/seabeam module paths, sources or registries (#3, edc8e57)
- 2026-10-29 looper issue Patch vulnerable gRPC and golang.org/x dependencies (grpc 1.79.1 → ≥1.83.2, x/net, x/text, x/sys) (#4, efa527c)
- 2026-10-29 looper issue Dockerfile: base image is SeaBeam's private schematic-alpine-1-2:latest, not the latest Alpine line (3.24) (#5, 4a1ded4)
