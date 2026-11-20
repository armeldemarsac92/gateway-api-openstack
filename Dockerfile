FROM docker.io/library/golang:1.27.4-trixie@sha256:f18c3d73a184fb0582828510722e723b63a93f7188d11dd223d9be6f6880e65d AS builder

WORKDIR /workspace
ARG VERSION=dev
ENV GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY api ./api
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=readonly \
    -buildvcs=false \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/openstack-gateway-controller \
    ./cmd/openstack-gateway-controller \
    && touch -d @0 /out/openstack-gateway-controller

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

LABEL org.opencontainers.image.source="https://github.com/gateway-api-openstack/gateway-api-openstack"

COPY --from=builder /out/openstack-gateway-controller /openstack-gateway-controller

USER 65532:65532
ENTRYPOINT ["/openstack-gateway-controller"]
