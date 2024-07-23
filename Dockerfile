# CONTAINER FOR BUILDING BINARY
FROM golang:1.21 AS build

# INSTALL DEPENDENCIES
COPY go.mod go.sum /src/
RUN cd /src && go mod download

# BUILD BINARY
COPY relay /src/relay
COPY datastreamer /src/datastreamer
COPY log /src/log
COPY Makefile version.go config/environments/testnet/config.toml /src/
RUN cd /src && make build-dsrelay


# CONTAINER FOR RUNNING BINARY
FROM alpine:3.19.0

COPY --from=build /src/dist/dsrelay /app/dsrelay
COPY --from=build /src/config.toml /app/sample.config.toml

ARG USER=dsrelay
ENV HOME /home/$USER
RUN adduser -D $USER
USER $USER
WORKDIR $HOME

EXPOSE 7900
CMD ["/app/dsrelay"]
