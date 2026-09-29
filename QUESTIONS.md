# Blobstore Questions

## 1. Upload-token single use has a cross-pod in-flight window (2026-09-29)

**What is tested:** second use of a settled token, over real HTTP (`httptest` on
`BytesHandler`):
- same pod: `TestUploadTokenIsSingleUse` (mirror_test.go) and
  `TestUploadTokenIsRefusedAfterItHasBeenConsumed` (also with the in-memory
  ledger cleared);
- the OTHER pod: `TestUploadTokenIsRefusedWhenReplayedOnThePeer`, added now. The
  gateway round-robins `PUT /upload/`, so a replay can land on either pod. It
  passes: the settled `UploadSession` row refuses it with 409.

**What is not enforced:** a second PUT that lands on the *other* pod *while the
first is still streaming*. The row stays `PENDING` until the transfer settles.
The in-flight guard, `Service.consumed`, is a per-pod `sync.Map`. The comment on
it in `service.go` says it covers "one pod's in-flight request". Pod B's public
path does not check pod A's map, and `servePeerUpload` never records into
`consumed`. So pod B accepts the replay too.

The impact looks small. The session fixes the content hash and size, so both
transfers must carry identical bytes. Temp files have random suffixes
(`os.CreateTemp`), so they do not collide. Promote writes the same
content-addressed path, and `Quota.Commit` is an idempotent delete. The
remaining effects: the token is used twice, both pods take a second full
transfer, and `PublishBlob` plus `settleSession` run twice for one upload. I
have not checked whether a second `PublishBlob` of the same `blob_id` is safe.

**Default taken:** not fixed, not tested (a test for it would fail today). It is
not a small, clear bug, and the impact looks small. A fix needs a design choice: a durable claim on the row
before the body is read (a stratum compare-and-swap from `PENDING` to an
in-flight state), or the peer path taking part in `consumed`. The second option
makes a racing pair both fail, and then the loser's abort settles the winner's
row as `ABORTED`. Which does the decision want?
