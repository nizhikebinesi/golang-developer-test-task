FROM golang:1.18.3-alpine as build
RUN apk add --update --no-cache git

ENV GO111MODULE=on

WORKDIR /usr/src/app
RUN mkdir bin
COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .
RUN go build -o bin/app

# FROM alpine:latest
#RUN apk add ca-certificates
#RUN apk add --update --no-cache curl
# COPY --from=build /usr/src/app/bin/app /usr/local/bin/app

CMD ["/usr/src/app/bin/app"]
