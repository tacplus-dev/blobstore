# Proposals: blobstore

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **ROADMAP.md: the floor is 2.5, not 2.4** (2026-10-29, #7): tacplus.yaml now declares `tacplus: "2.5"` (stratum, a dependency, needs 2.5). The "Now" line still says "tacplus floor 2.4 ... #1 still open", and the "Later" line "Declare the tacplus floor 2.4 (#1)" is replaced by #7; #3, #4, #5 and #2 are listed as blocked by #1, which #7 supersedes. Needs an agent at Jody's request.
- **ROADMAP.md: #3 is done; #5 may already be** (2026-10-29, #3): the module is `github.com/tacplus-dev/blobstore` and no SeaBeam reference is left outside PROVENANCE.md and the planning records (the gates now check it). ROADMAP.md's "Later" line for #3 can go. #5 is done too (Dockerfile base `alpine:3.24` as a build argument, gates and image build verified 2026-10-29), so a ROADMAP.md line for it can go as well. Needs an agent at Jody's request.
- **Old xz and klauspost/compress in the module graph** (2026-10-29, #4): `go list -m all` still lists github.com/ulikunitz/xz v0.5.6 and github.com/klauspost/compress v1.4.1 (GHSA advisories), which come from github.com/dsnet/compress v0.0.1's go.mod. They are not in the build: `govulncheck -scan module` and `./...` report nothing. Moving off dsnet/compress, or to a release of it that drops them, would also take them out of the graph. That should happen together with letsencrypt, whose pins these follow.

## Promoted / declined
