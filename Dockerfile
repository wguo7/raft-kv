FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/kvnode ./cmd/kvbench

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
ENTRYPOINT ["kvnode"]
