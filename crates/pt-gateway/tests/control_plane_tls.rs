//! Gateway → control plane over mutual TLS (ADR-022): snapshots, heartbeats, and usage
//! export. Certificates are generated in the test, so it always runs.

use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pt_control_plane::clock::SystemClock;
use pt_control_plane::tls::{server_config, ServerTlsConfig, TlsListener};
use pt_control_plane::{app, in_memory, ControlPlaneConfig};
use pt_core::cost::TierCapacity;
use pt_core::{
    Coefficients, Outcome, PerformanceProfile, Timings, TokenBreakdown, TrafficClass, UsageRecord,
};
use pt_entitlement::client_tls::ControlPlaneTls;
use pt_gateway::config::{EntitlementSourceConfig, ServerConfig, UsageExportConfig};
use pt_gateway::health::HeartbeatClient;
use pt_gateway::sync::{SnapshotClient, SyncError};
use pt_gateway::usage::{HttpSink, MemorySink, UsageSink};
use pt_gateway::{AppState, GatewayConfig};
use pt_telemetry::UsageStore;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
};

struct Pki {
    dir: PathBuf,
}

impl Pki {
    fn path(&self, name: &str) -> String {
        self.dir.join(name).display().to_string()
    }
}

fn write(dir: &Path, name: &str, pem: String) {
    std::fs::write(dir.join(name), pem).unwrap();
}

/// A CA, a rogue CA, a server certificate for 127.0.0.1, and a client certificate.
fn pki() -> Pki {
    let dir = std::env::temp_dir().join(format!("pt-cp-tls-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    let ca = |name: &str| {
        let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate().unwrap();
        let cert = p.self_signed(&key).unwrap();
        (cert, key)
    };
    let (ca_cert, ca_key) = ca("pt-test-ca");
    let (rogue, _) = ca("rogue-ca");
    write(&dir, "ca.pem", ca_cert.pem());
    write(&dir, "rogue.pem", rogue.pem());

    let mut server = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server.signed_by(&server_key, &ca_cert, &ca_key).unwrap();
    write(&dir, "server.pem", server_cert.pem());
    write(&dir, "server.key", server_key.serialize_pem());

    let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();
    client
        .distinguished_name
        .push(DnType::CommonName, "gateway-eu-west");
    client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client.signed_by(&client_key, &ca_cert, &ca_key).unwrap();
    write(&dir, "client.pem", client_cert.pem());
    write(&dir, "client.key", client_key.serialize_pem());
    Pki { dir }
}

fn tls(pki: &Pki, ca: Option<&str>, client: bool) -> ControlPlaneTls {
    ControlPlaneTls {
        ca_cert: ca.map(|f| pki.path(f)),
        client_cert: client.then(|| pki.path("client.pem")),
        client_key: client.then(|| pki.path("client.key")),
        allow_insecure_transport: false,
    }
}

fn source(url: &str, tls: ControlPlaneTls, public_key: String) -> EntitlementSourceConfig {
    EntitlementSourceConfig {
        control_plane_url: url.into(),
        region: "eu-west".into(),
        token: "region-token-eu-west-dev".into(),
        public_key,
        extra_public_keys: vec![],
        cache_path: None,
        wait_secs: 1,
        heartbeat_interval_ms: 0,
        engine_health_path: "/healthz".into(),
        tls,
    }
}

fn gateway(source: EntitlementSourceConfig) -> AppState {
    let config = GatewayConfig {
        server: ServerConfig {
            listen: "127.0.0.1:0".into(),
            engine_url: "http://127.0.0.1:1".into(),
            payg_engine_url: None,
            usage_log: None,
            wu_per_cu: 1_000.0,
        },
        profiles: vec![PerformanceProfile {
            name: "llama-4-maverick.b200.trtllm-1.2.tp8".into(),
            coefficients: Coefficients {
                a: 1.0,
                b: 0.1,
                c: 1.0,
                d: 0.0,
            },
            capacity_wu_per_s: TierCapacity::default(),
        }],
        entitlements: Some(source),
        quota: None,
        tokenization: Default::default(),
        prefix_cache: Default::default(),
        usage_export: None,
        reservations: vec![],
        deployments: vec![],
    };
    AppState::new(&config, Arc::new(MemorySink::default())).unwrap()
}

#[tokio::test]
async fn gateway_talks_to_the_control_plane_over_mutual_tls() {
    let pki = pki();
    let config = ControlPlaneConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/control-plane.toml"),
    )
    .unwrap();
    let svc = in_memory(config, SystemClock);
    let public = svc.signer().public_key_hex();
    let (routes, tel) = app(svc.clone());
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = tcp.local_addr().unwrap().port();
    let tls_config = server_config(&ServerTlsConfig {
        cert: pki.path("server.pem"),
        key: pki.path("server.key"),
        client_ca: Some(pki.path("ca.pem")),
    })
    .unwrap();
    // Only HTTP/1.1: axum here can't speak h2, so HTTP/2 clients must not negotiate it.
    assert_eq!(tls_config.alpn_protocols, vec![b"http/1.1".to_vec()]);
    let listener = TlsListener::new(tcp, tls_config).unwrap();
    tokio::spawn(async move { axum::serve(listener, routes).await });
    let url = format!("https://127.0.0.1:{port}");

    // Snapshots over mutual TLS.
    let good = source(&url, tls(&pki, Some("ca.pem"), true), public.clone());
    let gw = gateway(good.clone());
    let client = SnapshotClient::new(good.clone()).unwrap();
    assert!(client.poll_once(&gw, false).await.unwrap().is_some());

    // Heartbeats too.
    HeartbeatClient::new(good.clone(), "gw-1".into())
        .unwrap()
        .beat(&gw)
        .await
        .unwrap();
    let west = svc
        .region_statuses()
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.region == "eu-west")
        .unwrap();
    assert_eq!(west.gateways, 1);

    // And usage export.
    let sink = HttpSink::start(UsageExportConfig {
        control_plane_url: url.clone(),
        token: "region-token-eu-west-dev".into(),
        batch_size: 10,
        flush_interval_ms: 20,
        buffer: 100,
        tls: tls(&pki, Some("ca.pem"), true),
    })
    .unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    sink.emit(UsageRecord {
        request_id: uuid::Uuid::new_v4(),
        received_at_ms: now_ms,
        tenant: "acme".into(),
        reservation: "pt-x".into(),
        deployment: "d".into(),
        class: Some(TrafficClass::Provisioned),
        session_id: None,
        tokens: TokenBreakdown::default(),
        kv_token_seconds: 0.0,
        wu_estimated: 1.0,
        wu_actual: 1.0,
        timings: Timings {
            queue_ms: 0.0,
            ttft_ms: Some(1.0),
            total_ms: 1.0,
            tpot_ms: None,
        },
        in_shape: true,
        outcome: Outcome::Ok,
        profile: "p".into(),
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let got = tel
            .store
            .range("acme", "pt-x", 0, u64::MAX / 2)
            .await
            .unwrap();
        if got.len() == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "usage didn't arrive over TLS");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Refused: no client certificate, an untrusted server certificate, and plain HTTP.
    for bad in [
        source(&url, tls(&pki, Some("ca.pem"), false), public.clone()),
        source(&url, tls(&pki, None, true), public.clone()),
        source(&url, tls(&pki, Some("rogue.pem"), true), public.clone()),
        source(
            &format!("http://127.0.0.1:{port}"),
            tls(&pki, Some("ca.pem"), true),
            public.clone(),
        ),
    ] {
        let gw = gateway(bad.clone());
        let err = SnapshotClient::new(bad)
            .unwrap()
            .poll_once(&gw, false)
            .await
            .unwrap_err();
        assert!(matches!(err, SyncError::Http(_)), "{err}");
        assert_eq!(gw.entitlements().version, 0);
    }
    let _ = std::fs::remove_dir_all(&pki.dir);
}

#[tokio::test]
async fn plain_http_to_a_remote_control_plane_is_refused() {
    let remote = source(
        "http://cp.pt.example.com",
        ControlPlaneTls::default(),
        "b51da0aa7183df2b9a9ac31f26e8f856fd8b818d1118cdb87c9253afe13bb164".into(),
    );
    assert!(matches!(
        SnapshotClient::new(remote.clone()),
        Err(SyncError::Transport(_))
    ));
    assert!(HeartbeatClient::new(remote.clone(), "gw".into()).is_err());
    assert!(HttpSink::start(UsageExportConfig {
        control_plane_url: "http://cp.pt.example.com".into(),
        token: "t".into(),
        batch_size: 10,
        flush_interval_ms: 20,
        buffer: 100,
        tls: ControlPlaneTls::default(),
    })
    .is_err());
    // The gateway's configuration check catches it at startup.
    let mut config = GatewayConfig::load(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/gateway-eu-west.toml"),
    )
    .unwrap();
    config.entitlements.as_mut().unwrap().control_plane_url = "http://cp.pt.example.com".into();
    let err = config.validate().unwrap_err().to_string();
    assert!(err.contains("isn't reached over TLS"), "{err}");
    // Unless explicitly allowed.
    config
        .entitlements
        .as_mut()
        .unwrap()
        .tls
        .allow_insecure_transport = true;
    assert!(config.validate().is_ok());
}
