# 阶段1：构建环境
# build context 必须设置为项目根目录（docker-compose 的 build.context: ../..）
FROM golang:1.26.3-alpine AS builder

WORKDIR /build

# 先复制依赖文件，利用 Docker 层缓存
COPY go.mod go.sum ./
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download

# 复制全部源码
COPY . .

# 编译（CGO_ENABLED=0 静态链接，alpine 运行环境无需 glibc）
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./apps/gateway

# 阶段2：极简运行环境
FROM alpine:latest

RUN apk --no-cache add tzdata ca-certificates curl

ENV TZ=Asia/Shanghai

WORKDIR /apps

# 从构建阶段复制二进制（路径与 WORKDIR /build 一致）
COPY --from=builder /build/main .
# 复制配置文件（运行时需要）
COPY --from=builder /build/apps/config.yaml .

EXPOSE 8080

CMD ["./main"]