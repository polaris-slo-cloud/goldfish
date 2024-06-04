FROM rust:1.70.0-slim as builder

WORKDIR /usr/src

RUN USER=root cargo new medium-rust-dockerize


ENV PROTOC_ZIP=protoc-23.3-linux-x86_64.zip
RUN apt-get update && apt-get install -y unzip curl musl-tools git
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v23.3/$PROTOC_ZIP \
    && unzip $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP \
RUN apt-get install -y python3.8
RUN curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash

COPY wasm-actor/Cargo.toml wasm-actor/Cargo.lock /usr/src/medium-rust-dockerize/

WORKDIR /usr/src/medium-rust-dockerize

RUN rustup target add x86_64-unknown-linux-musl


RUN cargo build  --target x86_64-unknown-linux-musl --release

RUN rm /usr/src/medium-rust-dockerize/Cargo.toml
COPY wasm-actor/src /usr/src/medium-rust-dockerize/src/

RUN touch /usr/src/medium-rust-dockerize/src/lib

RUN cargo build --target x86_64-unknown-linux-musl --release

FROM alpine:3.16.0 AS runtime

COPY --from=builder /usr/src/medium-rust-dockerize/target/x86_64-unknown-linux-musl/release/wasm-actor /usr/local/bin

CMD ["/usr/local/bin/wasm-actor"]