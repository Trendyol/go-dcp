FROM golang:1.19-alpine as builder

WORKDIR /app

COPY .. .
RUN go mod download

WORKDIR /app/example

RUN CGO_ENABLED=0 go build -a -o godcpclient main.go

FROM alpine:3.17.0

RUN apk --no-cache add ca-certificates
RUN apk --no-cache add curl

USER nobody

COPY --from=builder --chown=nobody:nobody /app/example/godcpclient .
COPY --from=builder --chown=nobody:nobody /app/example/config_k8s_leader_election.yml ./example/config.yml

ENTRYPOINT ["./godcpclient"]