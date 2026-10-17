//! AirPlay 2 encrypted event and remote control channels.
//!
//! After initial SETUP, the client connects to the event TCP port.
//! All traffic is encrypted with ChaCha20-Poly1305 using HKDF-derived keys.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::crypto::chacha_transport::EncryptedChannel;
use crate::error::RemoteError;
use crate::raop::remote::{SenderVersion, encode_command_request};

/// Cap on ciphertext accumulated before a full AEAD frame can be decrypted.
/// Prevents a stuck/malicious peer from growing `encrypted_buf` without bound.
const MAX_ENCRYPTED_BUF: usize = 1024 * 1024;

/// Receiver-to-sender handle for one connection's event channel.
///
/// Holding it keeps the outbound mpsc open for the connection's lifetime. The SETUP-time
/// `updateInfo` goes straight through the raw channel with CSeq 0; [`Self::send_command`]
/// numbers later commands from 1.
#[derive(Clone)]
pub(crate) struct EventSender {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    next_cseq: Arc<AtomicU32>,
    /// Client version of the sender that opened the channel (`User-Agent`), if known.
    sender_version: Option<SenderVersion>,
}

impl EventSender {
    /// Wrap the outbound channel of an event-channel task.
    pub(crate) fn new(tx: mpsc::UnboundedSender<Vec<u8>>, sender_version: Option<SenderVersion>) -> Self {
        Self {
            tx,
            next_cseq: Arc::new(AtomicU32::new(1)),
            sender_version,
        }
    }

    /// Sender client version, if its `User-Agent` was seen before SETUP.
    pub(crate) const fn sender_version(&self) -> Option<SenderVersion> {
        self.sender_version
    }

    /// Queue `POST /command` with `body` as a binary plist.
    pub(crate) fn send_command(&self, body: &plist::Dictionary) -> Result<(), RemoteError> {
        let cseq = self.next_cseq.fetch_add(1, Ordering::Relaxed);
        let message = encode_command_request(cseq, body)?;
        self.tx.send(message).map_err(|_closed| RemoteError::ChannelClosed)
    }
}

/// Async event channel that accepts one encrypted TCP connection.
pub(crate) struct EventChannel;

impl EventChannel {
    /// Handle a connected event channel stream (public for use from handlers).
    pub(crate) async fn handle_stream(
        stream: TcpStream,
        channel: EncryptedChannel,
        cmd_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        Self::handle(stream, channel, cmd_rx).await;
    }

    async fn handle(
        mut stream: TcpStream,
        mut channel: EncryptedChannel,
        mut cmd_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let mut buf = vec![0u8; 4096];
        let mut encrypted_buf = Vec::new();
        loop {
            tokio::select! {
                result = stream.read(&mut buf) => {
                    match result {
                        Ok(0) => { debug!("Event channel closed by client"); break; }
                        Ok(n) => {
                            encrypted_buf.extend_from_slice(&buf[..n]);
                            if encrypted_buf.len() > MAX_ENCRYPTED_BUF {
                                warn!(
                                    len = encrypted_buf.len(),
                                    "Event channel encrypted buffer exceeded 1 MB; disconnecting"
                                );
                                break;
                            }
                            debug!(n, "Event channel data received");
                            match channel.decrypt_ctx.decrypt(&encrypted_buf) {
                                Ok((plain, consumed)) => {
                                    if consumed > 0 { encrypted_buf.drain(..consumed); }
                                    if !plain.is_empty() {
                                        // Sender replies to our POST /command (e.g. `RTSP/1.0 200 OK`).
                                        let first_line = plain.split(|b| *b == b'\r').next().unwrap_or_default();
                                        debug!(
                                            len = plain.len(),
                                            reply = %String::from_utf8_lossy(first_line),
                                            "Event channel message received"
                                        );
                                    }
                                }
                                Err(e) => {
                                    // Do not keep growing on permanent decrypt failure.
                                    warn!("Event channel decrypt error: {e}; disconnecting");
                                    break;
                                }
                            }
                        }
                        Err(e) => { warn!("Event channel read error: {e}"); break; }
                    }
                }
                maybe_cmd = cmd_rx.recv() => {
                    let Some(data) = maybe_cmd else {
                        debug!("Event channel command sender closed");
                        break;
                    };
                    debug!(len = data.len(), "Sending on event channel");
                    let encrypted = match channel.encrypt_ctx.encrypt(&data) {
                        Ok(e) => e,
                        Err(e) => { warn!("Event channel encrypt error: {e}"); break; }
                    };
                    if let Err(e) = stream.write_all(&encrypted).await {
                        warn!("Event channel write error: {e}"); break;
                    }
                }
            }
        }
    }
}
