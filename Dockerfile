FROM golang:1.19-alpine as builder

WORKDIR /project

COPY . .

WORKDIR /project/example

RUN rm ./config.yml
RUN mv ./config_k8s_default.yml ./config.yml

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o godcpclient main.go

FROM alpine:3.17.0

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/example/godcpclient .
COPY --from=builder --chown=nobody:nobody /project/example/config.yml ./config.yml

ENTRYPOINT ["./godcpclient"]