# Blobstore Release Image
# Layers the blobstore binary onto a minimal Alpine base.

FROM registry.gitlab.com/seabeam/schematic/containers/alpine/schematic-alpine-1-2:latest

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Second Star Cavalier LLC <cloudctlplane@seabeam.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Second Star Cavalier LLC" \
      org.opencontainers.image.title="BlobStore" \
      org.opencontainers.image.description="Content-addressed blob store for the Schematic control plane"

COPY bin/blobstore /usr/local/bin/blobstore

ENTRYPOINT ["blobstore"]
