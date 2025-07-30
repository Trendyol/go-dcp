FROM golang:1.24-alpine AS builder

WORKDIR /project

COPY go.mod go.sum ./
COPY main.go ./
COPY config.yml ./config.yml

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o example main.go

FROM alpine:3.17.1

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/example .
COPY --from=builder --chown=nobody:nobody /project/config.yml ./config.yml

ENTRYPOINT ["./example"]