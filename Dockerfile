# CONTAINER FOR BUILDING BINARY
# FROM golang:1.21 AS build

# INSTALL DEPENDENCIES
# RUN go install github.com/gobuffalo/packr/v2/packr2@v2.8.3
# COPY go.mod go.sum /src/
# RUN cd /src && go mod download

# BUILD BINARY
# COPY . /src
# RUN cd /src && make build-dsrelay

# CONTAINER FOR RUNNING BINARY
# FROM alpine:3.18.4
# COPY --from=build /src/dist/dsrelay /app/dsrelay
# COPY --from=build /src/config/environments/testnet/config.toml /app/sample.config.toml
# EXPOSE 7900
# CMD ["/bin/sh", "-c", "/app/dsrelay"]
