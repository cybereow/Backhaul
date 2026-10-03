# syntax=docker/dockerfile:1

# ---- build stage ----
# Debian 12 ships OpenSSL 3.0, the same major as the runtime image below, so the
# binary links against the library it will actually run with.
FROM golang:1.24-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# The server terminates TLS with OpenSSL through cgo, so CGO must be on.
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /out/backhaul ./main.go

# ---- runtime stage ----
FROM debian:bookworm-slim

# libssl3: the OpenSSL the binary links against. ca-certificates: outbound TLS
# verification (wss/wssmux clients).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/backhaul /usr/local/bin/backhaul

# Mount your config at /config/config.toml (see CMD).
ENTRYPOINT ["/usr/local/bin/backhaul"]
CMD ["-c", "/config/config.toml"]
