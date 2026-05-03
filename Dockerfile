FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/queuescope ./cmd/queuescope

FROM alpine:3.22

RUN adduser -D -H -u 10001 queuescope

WORKDIR /app
COPY --from=build /out/queuescope /app/queuescope

USER queuescope
EXPOSE 8080

CMD ["/app/queuescope"]

