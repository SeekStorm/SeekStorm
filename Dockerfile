FROM rust:slim-buster AS build

WORKDIR /code

COPY . /code

RUN cargo build --release

# Copy the binary into a new container for a smaller docker image
FROM debian:buster-slim

COPY --from=build /code/target/release/seekstorm_server /
USER root

ENTRYPOINT ["./seekstorm_server","local_ip=0.0.0.0","local_port=80","index_path=seekstorm_index"]
