//! Process-wide tracing capture: WARN/ERROR events for assertions, optional `RUST_LOG` output.

use std::fmt::Write as _;
use std::sync::LazyLock;

use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

static WARNINGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static INSTALL: LazyLock<()> = LazyLock::new(|| {
  let output = tracing_subscriber::fmt::layer()
    .with_test_writer()
    .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("off")));
  tracing_subscriber::registry()
    .with(output)
    .with(WarnCapture)
    .try_init()
    .expect("install tracing subscriber");
});

/// Install the global subscriber once per test process.
pub(crate) fn install() {
  let () = *LazyLock::force(&INSTALL);
}

/// Drop everything captured so far (start of a scenario).
pub(crate) fn clear() {
  WARNINGS.lock().clear();
}

/// WARN/ERROR events captured since the last [`clear`], formatted `LEVEL target: message k=v…`.
pub(crate) fn warnings() -> Vec<String> {
  WARNINGS.lock().clone()
}

struct WarnCapture;

impl<S: Subscriber> Layer<S> for WarnCapture {
  fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
    let level = *event.metadata().level();
    if level > Level::WARN {
      return;
    }
    let mut line = format!("{level} {}:", event.metadata().target());
    event.record(&mut FieldWriter(&mut line));
    WARNINGS.lock().push(line);
  }
}

struct FieldWriter<'a>(&'a mut String);

impl Visit for FieldWriter<'_> {
  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    if field.name() == "message" {
      let _ok = write!(self.0, " {value:?}");
    } else {
      let _ok = write!(self.0, " {}={value:?}", field.name());
    }
  }
}
