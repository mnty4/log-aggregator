# syntax=docker/dockerfile:1

FROM golang:1.24

WORKDIR /app

COPY . ./

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /log-aggregator

ENTRYPOINT ["/log-aggregator"]
CMD ["-logFile", "example.log"]