# Build from the repo root:
#   docker build -f deploy/operator/Dockerfile -t pt-operator:dev .
FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p pt-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/pt-operator /usr/local/bin/pt-operator
USER nonroot
ENTRYPOINT ["/usr/local/bin/pt-operator"]
