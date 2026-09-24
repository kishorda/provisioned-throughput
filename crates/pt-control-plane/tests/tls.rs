//! TLS to PostgreSQL/CockroachDB (ADR-021). Needs a server with TLS and these roles (see
//! CLAUDE.md): `tlsonly` (hostssl only) and `certuser` (client-certificate auth). Set
//! `PT_TEST_POSTGRES_ADDR` (for example `127.0.0.1:55432`) and `PT_TEST_TLS_DIR` (holding
//! `ca.pem`, `rogue.pem`, `client.pem`, `client.pk8.key`). Skipped otherwise.

use pt_control_plane::sql::SqlStore;
use pt_control_plane::store::StoreError;

fn env() -> Option<(String, String)> {
    match (
        std::env::var("PT_TEST_POSTGRES_ADDR"),
        std::env::var("PT_TEST_TLS_DIR"),
    ) {
        (Ok(a), Ok(d)) => Some((a, d)),
        _ => {
            eprintln!("PT_TEST_POSTGRES_ADDR or PT_TEST_TLS_DIR not set; skipping");
            None
        }
    }
}

async fn tls_in_use(store: &SqlStore) -> bool {
    sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(store.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn verified_tls_to_postgres() {
    let Some((addr, dir)) = env() else {
        return;
    };
    let url = |user: &str, params: &str| format!("postgres://{user}@{addr}/pt?{params}");

    // Verified TLS: the server's certificate chains to our CA and names 127.0.0.1.
    let store = SqlStore::connect_checked(
        &url(
            "tlsonly",
            &format!("sslmode=verify-full&sslrootcert={dir}/ca.pem"),
        ),
        2,
        false,
    )
    .await
    .unwrap();
    assert!(tls_in_use(&store).await);

    // The server refuses plaintext for this role.
    let plain = SqlStore::connect(&url("tlsonly", "sslmode=disable"), 1).await;
    assert!(matches!(plain, Err(StoreError::Unavailable(_))));

    // A certificate from another CA isn't accepted.
    let rogue = SqlStore::connect(
        &url(
            "tlsonly",
            &format!("sslmode=verify-full&sslrootcert={dir}/rogue.pem"),
        ),
        1,
    )
    .await;
    assert!(
        rogue.is_err(),
        "server certificate must chain to the configured CA"
    );

    // Mutual TLS: the server authenticates the client by certificate (CN=certuser).
    let mtls = SqlStore::connect(
        &url(
            "certuser",
            &format!(
                "sslmode=verify-full&sslrootcert={dir}/ca.pem&sslcert={dir}/client.pem&sslkey={dir}/client.pk8.key"
            ),
        ),
        1,
    )
    .await
    .unwrap();
    assert!(tls_in_use(&mtls).await);
    let no_cert = SqlStore::connect(
        &url(
            "certuser",
            &format!("sslmode=verify-full&sslrootcert={dir}/ca.pem"),
        ),
        1,
    )
    .await;
    assert!(
        no_cert.is_err(),
        "certificate auth without a client certificate"
    );
}
