FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/udp-splitter .

FROM alpine:3.22
RUN apk add --no-cache ffmpeg tzdata && addgroup -S app && adduser -S -G app app && mkdir -p /etc/udp-splitter /logs && chown -R app:app /etc/udp-splitter /logs
COPY --from=build /out/udp-splitter /usr/local/bin/udp-splitter
USER app
EXPOSE 9101
ENTRYPOINT ["/usr/local/bin/udp-splitter"]
