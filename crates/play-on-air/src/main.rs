//! PlayOnAir binary entrypoint.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

use play_on_air::app::App;
use play_on_air::config::Config;

/// Chromecast devices as AirPlay 2 speakers on the local network.
#[derive(Debug, Parser)]
#[command(name = "play-on-air", version, about, long_about = None)]
struct Cli {
  /// Optional path to a TOML config (rename / hide only).
  ///
  /// If omitted, uses `$PLAY_ON_AIR_CONFIG` or `./play-on-air.toml`.
  /// A missing file is fine — product defaults apply.
  #[arg(long, value_name = "PATH")]
  config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
  if let Err(err) = run().await {
    // Last-resort process failure; tracing may not be up yet.
    tracing::error!(error = %err, "PlayOnAir exited with error");
    return ExitCode::FAILURE;
  }
  ExitCode::SUCCESS
}

async fn run() -> play_on_air::Result<()> {
  init_tracing();

  let cli = Cli::parse();
  let config = Config::load_optional(cli.config.as_deref())?;
  tracing::info!(devices = config.devices.len(), "PlayOnAir starting (optional config loaded)");

  let (shutdown_tx, shutdown_rx) = watch::channel(false);
  // Keep JoinHandle so the signal task is not dropped before the app exits.
  let signal_task = spawn_signal_handler(shutdown_tx);

  let app = App::new(config);
  app.run(shutdown_rx).await?;
  signal_task.abort();
  Ok(())
}

fn init_tracing() {
  let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
  let subscriber = tracing_subscriber::fmt().with_env_filter(filter).with_target(true).finish();
  // Bridge `log` (used by rust_cast / mdns-sd) into tracing.
  if let Err(err) = tracing_log::LogTracer::init() {
    tracing::debug!(error = %err, "log tracer already initialized");
  }
  if let Err(err) = tracing::subscriber::set_global_default(subscriber) {
    tracing::debug!(error = %err, "tracing subscriber already set");
  }
}

fn spawn_signal_handler(shutdown_tx: watch::Sender<bool>) -> tokio::task::JoinHandle<()> {
  tokio::spawn(async move {
    wait_for_first_shutdown_signal().await;
    tracing::info!("graceful shutdown requested (second signal forces exit)");
    let _sent = shutdown_tx.send(true);

    // Re-arm: a second SIGINT/SIGTERM exits immediately.
    wait_for_first_shutdown_signal().await;
    tracing::error!("second shutdown signal; exiting immediately");
    // Forced exit on second signal; graceful shutdown already in progress.
    #[expect(
      clippy::exit,
      reason = "second SIGINT/SIGTERM must force-exit per lifecycle contract"
    )]
    {
      std::process::exit(1);
    }
  })
}

/// Wait for one SIGINT or SIGTERM (or Ctrl-C on non-unix).
async fn wait_for_first_shutdown_signal() {
  let ctrl_c = tokio::signal::ctrl_c();

  #[cfg(unix)]
  {
    let mut sigterm = {
      use tokio::signal::unix::{SignalKind, signal};
      signal(SignalKind::terminate()).ok()
    };

    tokio::select! {
      result = ctrl_c => {
        if let Err(err) = result {
          tracing::warn!(error = %err, "SIGINT handler error");
        }
        tracing::info!("received SIGINT");
      }
      () = async {
        if let Some(ref mut sig) = sigterm {
          let _term = sig.recv().await;
          tracing::info!("received SIGTERM");
        } else {
          std::future::pending::<()>().await;
        }
      } => {}
    }
  }

  #[cfg(not(unix))]
  {
    if let Err(err) = ctrl_c.await {
      tracing::warn!(error = %err, "SIGINT handler error");
    }
    tracing::info!("received interrupt signal");
  }
}
