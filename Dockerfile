# Blobstore Release Image
# Layers the blobstore binary onto a minimal Alpine base.

# The base is a build argument: a fleet may hand in its own hardened base, the
# default is the public one (no private registry in a public contract's repo).
ARG BASE_IMAGE=alpine:3.24
FROM ${BASE_IMAGE}

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Jody Scott <jody@thescottsweb.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="tacplus.dev" \
      org.opencontainers.image.title="BlobStore" \
      org.opencontainers.image.description="Content-addressed blob store for the Schematic control plane"

RUN apk add --no-cache ca-certificates

# blobstore needs no root: it runs as a fixed UID/GID, not an adduser default,
# so the operator's securityContext (runAsUser/fsGroup 65532) can name it and
# own the data_dir volume it mounts at /app/data (#18). /app/data is created
# owned by it, so a volume that takes the image's ownership is writable too.
RUN addgroup -S -g 65532 blobstore \
 && adduser -S -D -H -u 65532 -G blobstore -h /app -s /sbin/nologin blobstore \
 && mkdir -p /app/data/blobs \
 && chown -R 65532:65532 /app/data

COPY bin/blobstore /usr/local/bin/blobstore

USER 65532:65532
ENTRYPOINT ["blobstore"]
