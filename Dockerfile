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
      org.opencontainers.image.authors="Seabeam LLC <cloudctlplane@seabeam.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Seabeam LLC" \
      org.opencontainers.image.title="BlobStore" \
      org.opencontainers.image.description="Content-addressed blob store for the Schematic control plane"

RUN apk add --no-cache ca-certificates
COPY bin/blobstore /usr/local/bin/blobstore

ENTRYPOINT ["blobstore"]
