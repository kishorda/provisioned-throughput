//! HTTPS to the control plane, for gateways and the capacity controller (ADR-022).
//!
//! The same rules as for the databases (ADR-021): `https://` with a private CA and an
//! optional client certificate. A plain `http://` URL is only accepted for a loopback host,
//! or when `allow_insecure_transport` says so explicitly.

use serde::Deserialize;

/// How to reach the control plane. Every field is optional; the defaults suit a public CA
/// over `https://`, or a local development control plane over `http://127.0.0.1`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlPlaneTls {
    /// PEM file of the CA that signed the control plane's certificate (a private CA).
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// PEM files for mutual TLS, when the control plane sets `client_ca`.
    #[serde(default)]
    pub client_cert: Option<String>,
    #[serde(default)]
    pub client_key: Option<String>,
    /// Allow `http://` to a non-loopback control plane. Off by default.
    #[serde(default)]
    pub allow_insecure_transport: bool,
}

/// Whether `url` protects the connection well enough: `https`, a loopback host, or
/// `allow_insecure`.
pub fn check_transport(url: &str, allow_insecure: bool) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("invalid URL {url}: {e}"))?;
    let loopback = parsed.host_str().is_some_and(|h| {
        let h = h.trim_start_matches('[').trim_end_matches(']');
        h.eq_ignore_ascii_case("localhost")
            || h.parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if parsed.scheme() == "https" || loopback || allow_insecure {
        Ok(())
    } else {
        Err(format!(
            "the control plane at {url} isn't reached over TLS. Use an https:// URL (with [control_plane_tls] ca_cert for a private CA), or set allow_insecure_transport = true"
        ))
    }
}

impl ControlPlaneTls {
    /// Check `url` against the transport policy.
    pub fn check(&self, url: &str) -> Result<(), String> {
        check_transport(url, self.allow_insecure_transport)
    }

    /// A reqwest client builder using rustls, trusting `ca_cert` as well as public roots,
    /// and presenting the client certificate if one is set.
    pub fn client_builder(&self) -> Result<reqwest::ClientBuilder, String> {
        let read = |path: &str| std::fs::read(path).map_err(|e| format!("reading {path}: {e}"));
        let mut builder = reqwest::Client::builder().use_rustls_tls();
        if let Some(ca) = &self.ca_cert {
            let cert =
                reqwest::Certificate::from_pem(&read(ca)?).map_err(|e| format!("{ca}: {e}"))?;
            builder = builder.add_root_certificate(cert);
        }
        match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) => {
                let mut pem = read(cert)?;
                pem.push(b'\n');
                pem.extend(read(key)?);
                let identity = reqwest::Identity::from_pem(&pem)
                    .map_err(|e| format!("client certificate: {e}"))?;
                builder = builder.identity(identity);
            }
            (None, None) => {}
            _ => return Err("set both client_cert and client_key, or neither".into()),
        }
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_policy() {
        let ok = |u: &str| check_transport(u, false).is_ok();
        assert!(ok("http://127.0.0.1:8090"));
        assert!(ok("http://localhost:8090"));
        assert!(ok("https://cp.pt.example.com"));
        assert!(!ok("http://cp.pt.example.com"));
        assert!(!ok("http://10.1.2.3:8090"));
        assert!(check_transport("http://10.1.2.3:8090", true).is_ok());
        let half = ControlPlaneTls {
            client_cert: Some("/x.pem".into()),
            ..Default::default()
        };
        assert!(half.client_builder().is_err());
    }
}
