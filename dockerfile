FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY . .

# Argumento para definir o que buildar (broker ou sensor)
ARG TARGET
RUN GO111MODULE=off go build -o main $(find . -name "${TARGET}.go")
# --- É ESSENCIAL que esta linha abaixo esteja em uma nova linha ---
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .

CMD ["./main"]