# syntax=docker/dockerfile:1

# ---- build stage ----
# Build on the native builder arch and cross-compile to the target, so
# multi-arch builds don't run the Go compiler under slow QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /src

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Pure-Go static binary (CGO disabled). The optional OpenSSL TLS engine is a
# build-tagged extra that needs libssl and is intentionally left out of the
# default image to keep it portable and cgo-free.
ARG TARGETOS
ARG TARGETARCH
# The commit the image is built from, shown in the first line of its log.
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w -X github.com/musix/backhaul/cmd.BuildCommit=${COMMIT}" -o /out/backhaul ./main.go

# ---- runtime stage ----
FROM alpine:3.20

# Root CA bundle is required for outbound TLS (wss/wssmux to a CDN, etc.).
RUN apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=builder /out/backhaul /usr/local/bin/backhaul

# Mount your config at /config/config.toml (see CMD).
ENTRYPOINT ["/usr/local/bin/backhaul"]
CMD ["-c", "/config/config.toml"]
