FROM golang:1.20-alpine as builder

WORKDIR /project

COPY go.mod go.sum ./
COPY main.go ./
COPY config_k8s_default.yml ./config.yml

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o dcp main.go

FROM alpine:3.17.0

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/dcp .
COPY --from=builder --chown=nobody:nobody /project/config.yml ./config.yml

ENTRYPOINT ["./dcp"]