//! Progress capture.
//!
//! SurrealKit's modules report progress through the [`log`] facade. The CLI turns
//! that back into console output; an MCP server needs the same lines as *data*,
//! per tool call, without them reaching stdout -- which on the stdio transport is
//! the JSON-RPC channel, where one stray byte corrupts the protocol.
//!
//! `log::set_logger` can only be called once per process, so there cannot be one
//! logger for the CLI and another for the server. Instead there is a single
//! [`CaptureLogger`] that *dispatches*: to a task-local [`ProgressSink`] when one
//! is active, and to a fallback logger otherwise. A CLI run never enters a
//! capture scope, so it behaves exactly as it did before.

use std::future::Future;
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use serde::Serialize;

/// One captured log record.
#[derive(Debug, Clone, Serialize)]
pub struct ProgressLine {
	/// Lowercase level name: `error`, `warn`, `info`, `debug` or `trace`.
	pub level: &'static str,
	/// The formatted message, exactly as the CLI would have printed it.
	pub message: String,
}

/// A bounded buffer collecting the progress of one unit of work.
#[derive(Debug)]
pub struct ProgressSink {
	inner: Mutex<SinkInner>,
	max_lines: usize,
	max_bytes: usize,
}

#[derive(Debug, Default)]
struct SinkInner {
	lines: Vec<ProgressLine>,
	bytes: usize,
	dropped: usize,
}

impl ProgressSink {
	/// A sink with the default limits: 4096 lines or 512 KiB, whichever comes
	/// first.
	///
	/// Bounded on purpose. A sync over a large schema legitimately emits
	/// thousands of lines, but an unbounded buffer would let a runaway loop
	/// exhaust memory -- and a tool result has to fit in a model's context
	/// window regardless.
	pub fn new() -> Arc<Self> {
		Self::with_limits(4096, 512 * 1024)
	}

	/// A sink with explicit limits.
	pub fn with_limits(max_lines: usize, max_bytes: usize) -> Arc<Self> {
		Arc::new(Self {
			inner: Mutex::new(SinkInner::default()),
			max_lines,
			max_bytes,
		})
	}

	fn push(&self, level: &'static str, message: String) {
		// A poisoned sink must never take down the work it was observing.
		let Ok(mut inner) = self.inner.lock() else {
			return;
		};
		if inner.lines.len() >= self.max_lines || inner.bytes + message.len() > self.max_bytes {
			inner.dropped += 1;
			return;
		}
		inner.bytes += message.len();
		inner.lines.push(ProgressLine {
			level,
			message,
		});
	}

	/// Take everything captured so far, leaving the sink empty.
	///
	/// Returns the lines and the number of records dropped because a limit was
	/// reached, so a caller can say so rather than silently truncating.
	pub fn drain(&self) -> (Vec<ProgressLine>, usize) {
		let Ok(mut inner) = self.inner.lock() else {
			return (Vec::new(), 0);
		};
		let dropped = std::mem::take(&mut inner.dropped);
		inner.bytes = 0;
		(std::mem::take(&mut inner.lines), dropped)
	}

	/// The captured lines rendered as the CLI would have printed them.
	pub fn render(&self) -> String {
		let Ok(inner) = self.inner.lock() else {
			return String::new();
		};
		let mut out = String::new();
		for line in &inner.lines {
			out.push_str(&line.message);
			out.push('\n');
		}
		if inner.dropped > 0 {
			out.push_str(&format!("... and {} more line(s), elided\n", inner.dropped));
		}
		out
	}
}

tokio::task_local! {
	static ACTIVE: Arc<ProgressSink>;
}

/// The sink this task is currently capturing into, if any.
fn active() -> Option<Arc<ProgressSink>> {
	ACTIVE.try_with(Arc::clone).ok()
}

/// Run `fut` with `sink` as this task's capture target.
pub async fn capture<F: Future>(sink: Arc<ProgressSink>, fut: F) -> F::Output {
	ACTIVE.scope(sink, fut).await
}

/// Re-enter the caller's capture scope inside a task started with
/// [`tokio::spawn`] or [`tokio::task::JoinSet::spawn`].
///
/// Task-locals are **not** inherited across a spawn, so library code that spawns
/// must wrap the spawned future in this or its progress is silently dropped.
/// Today `tester::runner`'s parallel suite runner is the only such place.
pub fn propagate<F: Future>(fut: F) -> impl Future<Output = F::Output> {
	let sink = active();
	async move {
		match sink {
			Some(sink) => ACTIVE.scope(sink, fut).await,
			None => fut.await,
		}
	}
}

fn level_str(level: log::Level) -> &'static str {
	match level {
		log::Level::Error => "error",
		log::Level::Warn => "warn",
		log::Level::Info => "info",
		log::Level::Debug => "debug",
		log::Level::Trace => "trace",
	}
}

/// The process logger: routes to the active [`ProgressSink`], else to `fallback`.
///
/// Install it once, from whoever owns the process -- the CLI in `main`, or an
/// embedding application via [`CaptureLogger::install`].
pub struct CaptureLogger {
	fallback: Box<dyn log::Log>,
	level: log::LevelFilter,
	tee: bool,
}

impl log::Log for CaptureLogger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		// Only SurrealKit's own output; dependencies stay quiet.
		metadata.level() <= self.level && metadata.target().starts_with("surrealkit")
	}

	fn log(&self, record: &log::Record<'_>) {
		if !self.enabled(record.metadata()) {
			return;
		}
		match active() {
			Some(sink) => {
				sink.push(level_str(record.level()), record.args().to_string());
				if self.tee {
					// `-v`: mirror captured progress to the fallback too, so a
					// server operator can watch a long call from the console.
					self.fallback.log(record);
				}
			}
			None => self.fallback.log(record),
		}
	}

	fn flush(&self) {
		self.fallback.flush();
	}
}

impl CaptureLogger {
	/// Install as the process logger.
	///
	/// Fails when another logger is already installed -- typically because an
	/// embedding application called `env_logger::init` first. That is not fatal:
	/// capture simply yields nothing, and structured results are unaffected.
	pub fn install(
		fallback: Box<dyn log::Log>,
		level: log::LevelFilter,
		tee: bool,
	) -> Result<(), log::SetLoggerError> {
		let logger = Box::leak(Box::new(Self {
			fallback,
			level,
			tee,
		}));
		log::set_logger(logger)?;
		log::set_max_level(level);
		Ok(())
	}
}

/// A logger that writes every level to **stderr**.
///
/// The stdio MCP transport owns stdout, so nothing may be written there outside
/// JSON-RPC framing. Uses `io::stderr()` rather than `eprintln!` so it compiles
/// under the crate's `print_stderr` denial.
pub struct StderrLogger;

impl log::Log for StderrLogger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		metadata.target().starts_with("surrealkit")
	}

	fn log(&self, record: &log::Record<'_>) {
		if !self.enabled(record.metadata()) {
			return;
		}
		let mut err = std::io::stderr().lock();
		let _ = writeln!(err, "{}", record.args());
	}

	fn flush(&self) {
		let _ = std::io::stderr().flush();
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;

	/// A fallback that counts what it was handed, so tests can prove a record
	/// went to the console path rather than the capture path.
	struct Counting(Arc<AtomicUsize>);

	impl log::Log for Counting {
		fn enabled(&self, _: &log::Metadata<'_>) -> bool {
			true
		}
		fn log(&self, _: &log::Record<'_>) {
			self.0.fetch_add(1, Ordering::SeqCst);
		}
		fn flush(&self) {}
	}

	fn line(sink: &ProgressSink, msg: &str) {
		sink.push("info", msg.to_string());
	}

	#[tokio::test]
	async fn capture_collects_lines_in_order() {
		let sink = ProgressSink::new();
		capture(Arc::clone(&sink), async {
			let active = active().expect("a sink must be active inside capture");
			line(&active, "first");
			line(&active, "second");
		})
		.await;
		let (lines, dropped) = sink.drain();
		assert_eq!(dropped, 0);
		assert_eq!(
			lines.iter().map(|l| l.message.as_str()).collect::<Vec<_>>(),
			vec!["first", "second"],
			"emission order must be preserved"
		);
	}

	#[tokio::test]
	async fn no_sink_is_active_outside_capture() {
		assert!(active().is_none());
	}

	#[tokio::test]
	async fn concurrent_captures_do_not_interleave() {
		// The property that makes per-call capture correct under a stateless
		// server handling concurrent tool calls.
		let a = ProgressSink::new();
		let b = ProgressSink::new();
		let (ra, rb) = tokio::join!(
			capture(Arc::clone(&a), async {
				for i in 0..50 {
					line(&active().expect("sink a"), &format!("a{i}"));
					tokio::task::yield_now().await;
				}
			}),
			capture(Arc::clone(&b), async {
				for i in 0..50 {
					line(&active().expect("sink b"), &format!("b{i}"));
					tokio::task::yield_now().await;
				}
			}),
		);
		let ((), ()) = (ra, rb);
		let (la, _) = a.drain();
		let (lb, _) = b.drain();
		assert_eq!(la.len(), 50);
		assert_eq!(lb.len(), 50);
		assert!(la.iter().all(|l| l.message.starts_with('a')), "sink a captured foreign lines");
		assert!(lb.iter().all(|l| l.message.starts_with('b')), "sink b captured foreign lines");
	}

	#[tokio::test]
	async fn a_spawned_task_loses_the_sink_without_propagate() {
		// The regression lock for `tester::runner`'s JoinSet: task-locals are not
		// inherited across a spawn, so anything that spawns must call `propagate`.
		let sink = ProgressSink::new();
		capture(Arc::clone(&sink), async {
			tokio::spawn(async {
				assert!(active().is_none(), "a bare spawn must not see the caller's sink");
			})
			.await
			.expect("join");
		})
		.await;
		assert_eq!(sink.drain().0.len(), 0);
	}

	#[tokio::test]
	async fn propagate_carries_the_sink_into_a_spawned_task() {
		let sink = ProgressSink::new();
		capture(Arc::clone(&sink), async {
			tokio::spawn(propagate(async {
				line(&active().expect("propagated sink"), "from a spawned task");
			}))
			.await
			.expect("join");
		})
		.await;
		let (lines, _) = sink.drain();
		assert_eq!(lines.len(), 1);
		assert_eq!(lines[0].message, "from a spawned task");
	}

	#[tokio::test]
	async fn the_line_limit_elides_and_counts() {
		let sink = ProgressSink::with_limits(3, 1024);
		capture(Arc::clone(&sink), async {
			let active = active().expect("sink");
			for i in 0..10 {
				line(&active, &format!("{i}"));
			}
		})
		.await;
		let (lines, dropped) = sink.drain();
		assert_eq!(lines.len(), 3);
		assert_eq!(dropped, 7, "elided records must be counted, not silently lost");
	}

	#[tokio::test]
	async fn the_byte_limit_elides_too() {
		let sink = ProgressSink::with_limits(1000, 10);
		capture(Arc::clone(&sink), async {
			let active = active().expect("sink");
			line(&active, "12345");
			line(&active, "67890123456");
		})
		.await;
		let (lines, dropped) = sink.drain();
		assert_eq!(lines.len(), 1);
		assert_eq!(dropped, 1);
	}

	#[tokio::test]
	async fn render_reports_elision() {
		let sink = ProgressSink::with_limits(1, 1024);
		sink.push("info", "kept".to_string());
		sink.push("info", "dropped".to_string());
		let rendered = sink.render();
		assert!(rendered.contains("kept"));
		assert!(rendered.contains("1 more line(s), elided"), "got: {rendered}");
	}

	#[test]
	fn the_fallback_receives_records_when_no_sink_is_active() {
		let count = Arc::new(AtomicUsize::new(0));
		let logger = CaptureLogger {
			fallback: Box::new(Counting(Arc::clone(&count))),
			level: log::LevelFilter::Info,
			tee: false,
		};
		let record = log::Record::builder()
			.args(format_args!("hello"))
			.level(log::Level::Info)
			.target("surrealkit::sync")
			.build();
		log::Log::log(&logger, &record);
		assert_eq!(count.load(Ordering::SeqCst), 1);
	}

	#[test]
	fn dependency_records_are_ignored() {
		let count = Arc::new(AtomicUsize::new(0));
		let logger = CaptureLogger {
			fallback: Box::new(Counting(Arc::clone(&count))),
			level: log::LevelFilter::Info,
			tee: false,
		};
		let record = log::Record::builder()
			.args(format_args!("noise"))
			.level(log::Level::Info)
			.target("surrealdb::kvs")
			.build();
		log::Log::log(&logger, &record);
		assert_eq!(count.load(Ordering::SeqCst), 0, "only surrealkit's own output is logged");
	}
}
