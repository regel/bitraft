ARG BASE=alpine:latest
ARG GOOS=linux
ARG GOARCH=amd64
 
# Build the purge binary
FROM golang:1.15 as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go version.go server.go ./

# Build
RUN CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} GO111MODULE=on go build -a -o bitraft

RUN mkdir data

FROM $BASE 
WORKDIR /app
COPY --from=builder /workspace/bitraft .
COPY --from=builder --chown=65532:65532 /workspace/data /data

EXPOSE 4920/tcp

USER 65532:65532

VOLUME /data

ENTRYPOINT ["/app/bitraft"]
CMD ["-d", "/data"]
