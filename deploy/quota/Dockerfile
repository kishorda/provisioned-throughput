# Build from the repo root:
#   docker build -f deploy/quota/Dockerfile -t pt-quota:dev .
FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p pt-quota

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/pt-quota-coordinator /usr/local/bin/pt-quota-coordinator
USER nonroot
ENTRYPOINT ["/usr/local/bin/pt-quota-coordinator"]
