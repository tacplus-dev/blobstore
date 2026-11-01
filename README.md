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

One instance per tenant (#9): blobstore is `tenant_owned`, so each tenant has
its own instance in `/tenants/<tid>`, with its own two pods, its own PVCs and
its own Stratum for the metadata. Nothing in `/system` holds or serves a
tenant's blobs, and no instance calls into another tenant (tacplus-dev/roadmap
`decisions/s2s-never-crosses-tenants-2026-10-31.md`, addendum 2026-11-01). The
two pods of an instance each hold the same files under `<data_dir>/<group>/<sha256>`:
the disk is the tenant's, so there is no namespace level. An upload is acked only
once BOTH pods have the bytes fsynced and renamed into place, so one pod down
means uploads fail 503 while reads continue from the pod that is up. Bytes move
over two plain HTTP paths the gateway proxies — `PUT /upload/<tid>/<token>` and
`GET|HEAD /b/<tid>/<token>` — guarded by our own tokens: each is 256 raw random
bits whose SHA-256 is the key of the row that holds the grant (`UploadSession`
for an upload, `BlobLink` for a `/b/` link). The tid comes before the token so
the gateway can send the path to that tenant's instance, by the same rule as the
`tenant` route field every RPC carries; a token under another tid, or at another
tenant's instance, is a 404. An instance answers RPCs only from services of its
own tenant: a `/system` caller is refused. Every upload names a **volume group**,
declared at build time by the consuming service and rendered into this service's
config by the operator; the group is what bounds content types, blob size and
the tenant's quota, which each instance enforces for its own tenant. No group, no
upload.

## Layout

| file | what is in it |
|---|---|
| `main.go` | wiring: config, store, mirror, records, the two listeners, `sm.Run` |
| `internal/blobstore/store.go` | the PVC: `<group>/<sha256>` paths, temp files, fsync-then-rename promotion, the temp sweep |
| `internal/blobstore/validate.go` | the single gate between a wire string and a path |
| `internal/blobstore/token.go` | upload and link tokens: raw entropy, keyed by their SHA-256 |
| `internal/blobstore/groups.go` | volume-group rules and the magic-byte table |
| `internal/blobstore/filename.go` | filename validation and `Content-Disposition` |
| `internal/blobstore/receive.go` | the copy loop: hash, count, cap on the wire |
| `internal/blobstore/mirror.go` | the peer: health, upload forwarding, delete, anti-entropy |
| `internal/blobstore/http_bytes.go` | `PUT /upload/<tid>/<token>`, `GET|HEAD /b/<tid>/<token>` |
| `internal/blobstore/http_peer.go` | `/peer/*` — the mirror sink and the anti-entropy source |
| `internal/blobstore/records.go` | stratum: the record reads and writes, through its record plane, routed to the instance's tenant |
| `internal/blobstore/quota.go` | the instance's per-group quota (its tenant's) and its reservations |
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
and stub only stratum. The cross-tenant tests (`tenant_owned_test.go`) stand up two
tenants' instances that way, each with its own disks and its own stub Stratum.
