# 阶段1：构建环境
FROM golang:1.26.3-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main ./apps/agent

# 阶段2：极简运行环境
FROM alpine:latest

RUN apk --no-cache add tzdata ca-certificates curl

ENV TZ=Asia/Shanghai

WORKDIR /apps

COPY --from=builder /build/main .
COPY --from=builder /build/apps/config.yaml .

EXPOSE 8050

CMD ["./main"]