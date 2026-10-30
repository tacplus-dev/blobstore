# blobstore

The platform's binary blob primitive: a two-pod mirror of content-addressed
files, with its metadata in stratum records.

Design: `looper/schematic/decisions/blob-store-2026-09-25.md` (v3 — self-hosted,
no cloud object store, no signed cloud URLs). Surface:
`proto/services/blobstore.proto`, held in this repository (`src: proto`; its
dependencies' protos come from tacplus-dev through `depends`). Read both before changing anything
here; every non-obvious choice in this service traces to a numbered requirement
in that doc and the code cites them as `R1`…`R13`.

## The shape, in one paragraph

Two pods, each with its own PVC, both holding the same files under
`<data_dir>/<group>/<namespace>/<sha256>`. An upload is acked only once BOTH pods
have the bytes fsynced and renamed into place, so one pod down means uploads fail
503 while reads continue from the pod that is up. Bytes move over two plain HTTP
paths the gateway proxies — `PUT /upload/<tid>/<token>` and `GET|HEAD /b/<tid>/<token>` —
guarded by our own tokens: the upload token is an HMAC-signed envelope carrying
the upload's terms, and a `/b/` token is 256 raw random bits whose SHA-256 is the
`BlobLink` record's key. The tenant id comes before the token, because each
tenant's rows are in its own Stratum and the path has to name it (#8). Every upload names a **volume group**, declared at build
time by the consuming service and rendered into this service's config by the
operator; the group is what bounds content types, blob size and per-tenant quota.
No group, no upload.

## Layout

| file | what is in it |
|---|---|
| `main.go` | wiring: config, store, mirror, records, the two listeners, `sm.Run` |
| `internal/blobstore/store.go` | the PVC: paths, temp files, fsync-then-rename promotion, the temp sweep |
| `internal/blobstore/validate.go` | the single gate between a wire string and a path |
| `internal/blobstore/token.go` | upload tokens (HMAC envelope) and link tokens (raw entropy) |
| `internal/blobstore/groups.go` | volume-group rules and the magic-byte table |
| `internal/blobstore/filename.go` | filename validation and `Content-Disposition` |
| `internal/blobstore/receive.go` | the copy loop: hash, count, cap on the wire |
| `internal/blobstore/mirror.go` | the peer: health, upload forwarding, delete, anti-entropy |
| `internal/blobstore/http_bytes.go` | `PUT /upload/<tid>/<token>`, `GET|HEAD /b/<tid>/<token>` |
| `internal/blobstore/http_peer.go` | `/peer/*` — the mirror sink and the anti-entropy source |
| `internal/blobstore/records.go` | stratum: the record reads and writes, through its record plane |
| `internal/blobstore/quota.go` | per-tenant, per-group quota and its reservations |
| `internal/blobstore/stats.go` | in-memory resolution counters and the rollup |
| `internal/blobstore/handler.go` | the RPC surface and the generated lifecycle |

## Local development

```
just generate     # regenerate tacplus/ from the proto (tacplus)
go build ./...
go test ./...
```

`tacplus/` is generated and gitignored. The tests need no cluster: they stand up
two real Services on two temp directories with a real peer listener between them,
and stub only stratum.
