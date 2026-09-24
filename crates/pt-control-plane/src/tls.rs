//! HTTPS for the control plane (ADR-022).
//!
//! The control plane terminates TLS itself with rustls (ring provider). With `client_ca`
//! set, every connection must present a certificate that CA signed (mutual TLS). Region
//! tokens stay the identity; the client certificate is defence in depth.
//!
//! [`TlsListener`] plugs into `axum::serve`. Handshakes run in their own tasks with a
//! timeout, so a slow or hostile client can't hold up other connections.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

/// `[server.tls]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTlsConfig {
    /// PEM certificate chain, leaf first.
    pub cert: String,
    /// PEM private key (PKCS#8, PKCS#1, or SEC1).
    pub key: String,
    /// PEM CA for client certificates. When set, clients without a certificate it signed are
    /// refused during the handshake.
    #[serde(default)]
    pub client_ca: Option<String>,
}

/// A handshake that takes longer than this is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Load certificates and build the rustls server configuration.
pub fn server_config(cfg: &ServerTlsConfig) -> Result<Arc<ServerConfig>, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cfg.cert)
        .map_err(|e| format!("reading {}: {e}", cfg.cert))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parsing {}: {e}", cfg.cert))?;
    if certs.is_empty() {
        return Err(format!("{} has no certificates", cfg.cert));
    }
    let key =
        PrivateKeyDer::from_pem_file(&cfg.key).map_err(|e| format!("reading {}: {e}", cfg.key))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    let builder = match &cfg.client_ca {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            for cert in
                CertificateDer::pem_file_iter(ca).map_err(|e| format!("reading {ca}: {e}"))?
            {
                roots
                    .add(cert.map_err(|e| format!("parsing {ca}: {e}"))?)
                    .map_err(|e| format!("{ca}: {e}"))?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .map_err(|e| format!("client CA {ca}: {e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| format!("certificate and key: {e}"))?;
    // axum is built with HTTP/1 only: advertising h2 would make HTTP/2 clients (curl,
    // browsers) negotiate a protocol the server can't speak.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// A TCP listener that hands `axum::serve` only connections whose TLS handshake succeeded.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    /// Start accepting on `tcp`. Must be called inside a Tokio runtime.
    pub fn new(tcp: TcpListener, config: Arc<ServerConfig>) -> std::io::Result<Self> {
        let local = tcp.local_addr()?;
        let acceptor = TlsAcceptor::from(config);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let (stream, addr) = match tcp.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls)) => {
                            let _ = tx.send((tls, addr)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%addr, error = %e, "TLS handshake failed"),
                        Err(_) => tracing::debug!(%addr, "TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // The accept loop never ends, so this can't happen; wait forever if it does.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local)
    }
}
