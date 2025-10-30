FROM golang:1.24.3-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY="https://goproxy.cn,direct" go mod download

COPY . .
WORKDIR /src/ordercenter
RUN go build -o /bin/ordercenter main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /bin/ordercenter /app/ordercenter

CMD ["/app/ordercenter"]

