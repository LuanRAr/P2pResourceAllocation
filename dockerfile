# Stage 1: compilação
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY . .

ARG TARGET

RUN go mod download && \
    go build -o main ./${TARGET}/${TARGET}.go

# Stage 2: imagem mínima de runtime
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
EXPOSE 5000
CMD ["./main"]