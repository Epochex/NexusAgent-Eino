# Build each executable with --build-arg TARGET=server|worker|cli|migrate.
FROM golang@sha256:fb612b7831d53a89cbc0aaa7855b69ad7b0caf603715860cf538df854d047b84 AS builder
ENV GOTOOLCHAIN=local
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=server
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${TARGET} && mkdir -p /out/data/files /out/tmp && chmod 1777 /out/tmp

FROM scratch
ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/Epochex/NexusAgent-Eino" org.opencontainers.image.revision="${VCS_REF}"
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
WORKDIR /app
COPY --from=builder /out/app ./app
COPY --chown=65532:65532 configs ./configs
COPY --from=builder --chown=65532:65532 /out/data ./data
COPY --from=builder --chown=65532:65532 /out/tmp /tmp
USER 65532:65532
ENTRYPOINT ["./app"]