# Build stage
FROM golang:1.27.0-alpine AS builder

# Install git and ca-certificates (needed for private repos and HTTPS)
RUN apk add --no-cache git ca-certificates tzdata

# Create appuser for security
RUN adduser -D -g '' appuser

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download
RUN go mod verify

# Copy source code
COPY . .

# Build metadata
ARG VERSION=dev
ARG BUILD_TIME=unknown
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown

# Build the application. The -X targets must name the package that declares the
# variables (internal/buildinfo); the linker silently ignores unknown symbols, so a
# wrong path here produces a binary reporting "unknown" with no build error.
# `make release-check` verifies these paths.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-w -s -X github.com/tmoeish/tsq/v4/internal/buildinfo.version=${VERSION} -X github.com/tmoeish/tsq/v4/internal/buildinfo.buildTime=${BUILD_TIME} -X github.com/tmoeish/tsq/v4/internal/buildinfo.gitCommit=${GIT_COMMIT} -X github.com/tmoeish/tsq/v4/internal/buildinfo.gitBranch=${GIT_BRANCH}" \
    -o tsq ./cmd/tsq

# Final stage
FROM scratch

# Import from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/passwd /etc/passwd

# Copy the binary
COPY --from=builder /build/tsq /tsq

# Use an unprivileged user
USER appuser

# Expose port (if needed for web interface in future)
# EXPOSE 8080

# Run the binary
ENTRYPOINT ["/tsq"] 
