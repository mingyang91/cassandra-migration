FROM rust:latest AS builder

WORKDIR /usr/src/
RUN rustup target add x86_64-unknown-linux-musl
RUN USER=root cargo new cassandra-migration

WORKDIR /usr/src/cassandra-migration

COPY Cargo.toml Cargo.lock ./
RUN cargo build --release

COPY src src

RUN cargo install --target x86_64-unknown-linux-musl --path .

FROM scratch
COPY --from=builder /usr/local/cargo/bin/cassandra-migration .
USER 1000
ENTRYPOINT ["./cassandra-migration"]



