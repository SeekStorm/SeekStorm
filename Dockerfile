FROM rust:slim-buster AS build

LABEL org.opencontainers.image.title="SeekStorm server"
LABEL maintainer="SeekStorm, Sp. z o.o. <info@seekstorm.com>"
LABEL org.opencontainers.image.vendor="SeekStorm, Sp. z o.o."
LABEL org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /seekstorm

COPY . /seekstorm

RUN cargo build --release

FROM debian:buster-slim

COPY --from=build /seekstorm/target/release/seekstorm_server /
USER root

ENTRYPOINT ["./seekstorm_server","local_ip=0.0.0.0","local_port=80","index_path=seekstorm_index"]
