FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/control-api .

FROM alpine:3.22
RUN apk add --no-cache tzdata ffmpeg && addgroup -S app && adduser -S -G app app && mkdir -p /data /app/web && chown -R app:app /data /app
COPY --from=build /out/control-api /usr/local/bin/control-api
COPY web /app/web
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/control-api"]
