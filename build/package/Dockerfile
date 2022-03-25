FROM golang:1.17 as build
WORKDIR /usr/src/app
COPY go.* ./
RUN go mod download && go mod tidy
COPY . .
RUN CGO_ENABLED=0 go build -v -o /usr/local/bin/app ./cmd/http/main.go

FROM alpine:latest
COPY --from=build /usr/local/bin/app /bin/app
COPY --from=build /usr/src/app/build/package/entrypoint.sh /bin/entrypoint.sh
EXPOSE 8080/tcp
CMD /bin/app -hostname $(hostname -i)
