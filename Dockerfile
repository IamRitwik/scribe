FROM golang:1.27-alpine AS build
WORKDIR /go/src/scribe
COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/scribe ./cmd/scribe
RUN CGO_ENABLED=0 go install github.com/grpc-ecosystem/grpc-health-probe@latest

FROM scratch
COPY --from=build /go/bin/scribe /bin/scribe
COPY --from=build /go/bin/grpc-health-probe /bin/grpc_health_probe
ENTRYPOINT ["/bin/scribe"]