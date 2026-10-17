//! TLS client configuration for the Cast control channel (TCP 8009).
//!
//! Cast receivers present self-signed device certificates, and some (for example
//! Samsung TVs with built-in Cast) present **X.509 v1** certificates. Reference
//! senders (Chrome, pychromecast) do not validate the TLS certificate chain: a
//! receiver's authenticity comes from the Cast device-auth challenge on
//! `urn:x-cast:com.google.cast.tp.deviceauth`, which neither this bridge nor
//! `rust_cast` performs.
//!
//! `rust_cast::NoCertificateVerification` skips the chain but still verifies the
//! handshake signature through rustls' webpki helpers, which parse the whole
//! certificate and reject v1 with `UnsupportedCertVersion`. [`CastServerCertVerifier`]
//! keeps that signature check while accepting v1: it reads the certificate's
//! `SubjectPublicKeyInfo` with `x509-parser` (version-agnostic) and verifies the
//! handshake signature against that raw key with the rustls crypto provider's
//! algorithms. The session keys are therefore bound to the key the device presented;
//! the identity behind that key is not authenticated (same posture as reference
//! senders without device auth).

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{
  CertificateDer, ServerName, SignatureVerificationAlgorithm, SubjectPublicKeyInfoDer, UnixTime,
};
use rustls::{CertificateError, ClientConfig, DigitallySignedStruct, OtherError, PeerMisbehaved, SignatureScheme};

/// Build the rustls client config for Cast control connections.
///
/// Uses the aws-lc-rs provider explicitly (no process-default lookup) with TLS 1.2
/// and 1.3, no client auth, and [`CastServerCertVerifier`].
///
/// # Errors
///
/// Returns a rustls error if the provider rejects the default protocol versions.
pub(crate) fn client_config() -> Result<ClientConfig, rustls::Error> {
  let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
  let verifier = Arc::new(CastServerCertVerifier::new(provider.signature_verification_algorithms));
  Ok(
    ClientConfig::builder_with_provider(provider)
      .with_safe_default_protocol_versions()?
      .dangerous()
      .with_custom_certificate_verifier(verifier)
      .with_no_client_auth(),
  )
}

/// Cast server certificate verifier: any certificate (including X.509 v1), but the
/// handshake signature must verify against the presented certificate's public key.
#[derive(Debug)]
pub(crate) struct CastServerCertVerifier {
  /// Signature algorithms of the crypto provider, keyed by TLS signature scheme.
  algorithms: WebPkiSupportedAlgorithms,
}

impl CastServerCertVerifier {
  /// Verifier over the given provider signature algorithms.
  pub(crate) const fn new(algorithms: WebPkiSupportedAlgorithms) -> Self {
    Self { algorithms }
  }

  /// TLS 1.2 handshake signature over `message` with the public key in `cert`.
  ///
  /// Mirrors rustls' `verify_tls12_signature`, but takes the key from the SPKI instead
  /// of a webpki-parsed (v3-only) certificate. TLS 1.2 schemes do not pin an ECDSA
  /// curve, so every mapped algorithm is tried until one fits the key.
  fn verify_tls12(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    let candidates = self.algorithms_for(dss.scheme)?;
    let spki = subject_public_key_info(cert)?;
    let key = webpki::RawPublicKeyEntity::try_from(&spki).map_err(webpki_error)?;

    let mut mismatch = None;
    for alg in candidates {
      match key.verify_signature(*alg, message, dss.signature()) {
        Ok(()) => return Ok(HandshakeSignatureValid::assertion()),
        // Algorithm does not fit this key type / curve; try the next candidate.
        Err(err @ webpki::Error::UnsupportedSignatureAlgorithmForPublicKeyContext(_)) => mismatch = Some(err),
        Err(err) => return Err(webpki_error(err)),
      }
    }
    Err(mismatch.map_or_else(|| PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme.into(), webpki_error))
  }

  /// Provider algorithms for a TLS signature scheme we advertised.
  fn algorithms_for(
    &self,
    scheme: SignatureScheme,
  ) -> Result<&'static [&'static dyn SignatureVerificationAlgorithm], rustls::Error> {
    self
      .algorithms
      .mapping
      .iter()
      .find(|(candidate, _)| *candidate == scheme)
      .map(|(_, algorithms)| *algorithms)
      .ok_or_else(|| PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme.into())
  }
}

impl ServerCertVerifier for CastServerCertVerifier {
  /// Accept any server certificate: Cast device certificates are self-signed (some
  /// X.509 v1) and are not chain-validated by reference senders either.
  fn verify_server_cert(
    &self,
    _end_entity: &CertificateDer<'_>,
    _intermediates: &[CertificateDer<'_>],
    _server_name: &ServerName<'_>,
    _ocsp_response: &[u8],
    _now: UnixTime,
  ) -> Result<ServerCertVerified, rustls::Error> {
    Ok(ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    self.verify_tls12(message, cert, dss)
  }

  /// Same key source as TLS 1.2; rustls' raw-key helper applies the TLS 1.3 rules
  /// (scheme must be TLS 1.3-capable; first mapped algorithm only).
  fn verify_tls13_signature(
    &self,
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    let spki = subject_public_key_info(cert)?;
    rustls::crypto::verify_tls13_signature_with_raw_key(message, &spki, dss, &self.algorithms)
  }

  fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
    self.algorithms.supported_schemes()
  }
}

/// DER `SubjectPublicKeyInfo` of `cert`, borrowed from the certificate bytes.
///
/// `x509-parser` accepts v1, v2, and v3 certificates.
fn subject_public_key_info<'a>(cert: &'a CertificateDer<'_>) -> Result<SubjectPublicKeyInfoDer<'a>, rustls::Error> {
  let (_, parsed) = x509_parser::parse_x509_certificate(cert.as_ref()).map_err(|err| {
    tracing::debug!(error = %err, "Cast device certificate is not parseable X.509");
    rustls::Error::InvalidCertificate(CertificateError::BadEncoding)
  })?;
  Ok(SubjectPublicKeyInfoDer::from(parsed.tbs_certificate.subject_pki.raw))
}

/// Map webpki signature errors onto the rustls errors its own verifier would use.
#[expect(
  clippy::wildcard_enum_match_arm,
  reason = "webpki::Error is non_exhaustive; only signature/encoding variants have rustls equivalents here"
)]
fn webpki_error(err: webpki::Error) -> rustls::Error {
  match err {
    webpki::Error::BadDer | webpki::Error::TrailingData(_) => CertificateError::BadEncoding.into(),
    webpki::Error::InvalidSignatureForPublicKey => CertificateError::BadSignature.into(),
    webpki::Error::UnsupportedSignatureAlgorithmForPublicKeyContext(context) => {
      CertificateError::UnsupportedSignatureAlgorithmForPublicKeyContext {
        signature_algorithm_id: context.signature_algorithm_id,
        public_key_algorithm_id: context.public_key_algorithm_id,
      }
      .into()
    },
    other => CertificateError::Other(OtherError(Arc::new(other))).into(),
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
  use rustls::server::{ClientHello, ResolvesServerCert};
  use rustls::sign::CertifiedKey;
  use rustls::{CertificateError, ClientConfig, ClientConnection, Connection, ServerConfig, ServerConnection};

  use super::client_config;

  /// Self-signed X.509 **v1** RSA-2048 certificate (no version field, no extensions),
  /// the shape some Cast TVs present. Test-only key material.
  const V1_RSA_CERT: &[u8] = include_bytes!("testdata/v1-self-signed-rsa.der");
  const V1_RSA_KEY: &[u8] = include_bytes!("testdata/v1-self-signed-rsa.test-key.der");
  /// Self-signed X.509 v3 ECDSA P-256 certificate. Test-only key material.
  const V3_P256_CERT: &[u8] = include_bytes!("testdata/v3-self-signed-p256.der");
  const V3_P256_KEY: &[u8] = include_bytes!("testdata/v3-self-signed-p256.test-key.der");
  /// Unrelated RSA-2048 key (test-only) for wrong-signer handshakes.
  const OTHER_RSA_KEY: &[u8] = include_bytes!("testdata/other-rsa.test-key.der");

  #[derive(Debug)]
  struct FixedCert(Arc<CertifiedKey>);

  impl ResolvesServerCert for FixedCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
      Some(Arc::clone(&self.0))
    }
  }

  /// Server presenting `cert` and signing the handshake with `key` (no cert/key match
  /// check, which would itself reject v1 certificates).
  fn server_config(cert: &[u8], key: &[u8], version: &'static rustls::SupportedProtocolVersion) -> Arc<ServerConfig> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.to_vec()));
    let signing_key = provider.key_provider.load_private_key(key_der).unwrap();
    let certified = CertifiedKey::new(vec![CertificateDer::from(cert.to_vec())], signing_key);
    let config = ServerConfig::builder_with_provider(provider)
      .with_protocol_versions(&[version])
      .unwrap()
      .with_no_client_auth()
      .with_cert_resolver(Arc::new(FixedCert(Arc::new(certified))));
    Arc::new(config)
  }

  /// Move pending TLS records from `from` to `to`; returns the error `to` raised, if any.
  fn pump(from: &mut Connection, to: &mut Connection) -> Option<rustls::Error> {
    let mut wire = Vec::new();
    while from.wants_write() {
      assert!(from.write_tls(&mut wire).unwrap() > 0);
    }
    let mut cursor = wire.as_slice();
    while !cursor.is_empty() {
      assert!(to.read_tls(&mut cursor).unwrap() > 0);
      if let Err(err) = to.process_new_packets() {
        return Some(err);
      }
    }
    None
  }

  /// In-memory handshake; returns the client-side error, if any.
  fn handshake(client_cfg: Arc<ClientConfig>, server_cfg: Arc<ServerConfig>) -> Option<rustls::Error> {
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let mut client = Connection::Client(ClientConnection::new(client_cfg, name).unwrap());
    let mut server = Connection::Server(ServerConnection::new(server_cfg).unwrap());
    for _ in 0..8 {
      if !client.is_handshaking() && !server.is_handshaking() {
        return None;
      }
      if let Some(err) = pump(&mut client, &mut server) {
        panic!("server side of the handshake failed: {err}");
      }
      if let Some(err) = pump(&mut server, &mut client) {
        return Some(err);
      }
    }
    panic!("handshake did not complete");
  }

  fn cast_client() -> Arc<ClientConfig> {
    Arc::new(client_config().unwrap())
  }

  const VERSIONS: [&rustls::SupportedProtocolVersion; 2] = [&rustls::version::TLS12, &rustls::version::TLS13];

  #[test]
  fn x509_v1_device_cert_completes_handshake() {
    for version in VERSIONS {
      let result = handshake(cast_client(), server_config(V1_RSA_CERT, V1_RSA_KEY, version));
      assert_eq!(result, None, "{version:?}");
    }
  }

  #[test]
  fn x509_v1_fixture_reproduces_webpki_rejection() {
    // The stock rust_cast verifier fails on this fixture the way the TV failed in the field.
    let stock = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
      .with_safe_default_protocol_versions()
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(rust_cast::NoCertificateVerification))
      .with_no_client_auth();
    let err = handshake(Arc::new(stock), server_config(V1_RSA_CERT, V1_RSA_KEY, &rustls::version::TLS12))
      .expect("stock verifier must reject the v1 fixture");
    assert!(err.to_string().contains("UnsupportedCertVersion"), "{err}");
  }

  #[test]
  fn x509_v3_device_cert_completes_handshake() {
    for version in VERSIONS {
      let result = handshake(cast_client(), server_config(V3_P256_CERT, V3_P256_KEY, version));
      assert_eq!(result, None, "{version:?}");
    }
  }

  #[test]
  fn v1_cert_with_handshake_signed_by_another_key_is_rejected() {
    // Same key type as the presented v1 cert, wrong private key: the signature check
    // against the presented certificate's key must fail, not be skipped.
    for version in VERSIONS {
      let err =
        handshake(cast_client(), server_config(V1_RSA_CERT, OTHER_RSA_KEY, version)).expect("wrong signer accepted");
      assert_eq!(
        err,
        rustls::Error::InvalidCertificate(CertificateError::BadSignature),
        "{version:?}"
      );
    }
  }

  #[test]
  fn v1_rsa_cert_with_ecdsa_handshake_signature_is_rejected() {
    for version in VERSIONS {
      let err =
        handshake(cast_client(), server_config(V1_RSA_CERT, V3_P256_KEY, version)).expect("key mismatch accepted");
      assert!(
        matches!(
          err,
          rustls::Error::InvalidCertificate(CertificateError::UnsupportedSignatureAlgorithmForPublicKeyContext { .. })
        ),
        "{version:?}: {err}"
      );
    }
  }
}
