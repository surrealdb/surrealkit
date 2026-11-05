//! Watching files for changes: the part of a watch loop that does not depend on what it
//! rebuilds.
//!
//! A [`Watcher`] watches a set of [`Root`]s, keeps the changes a caller's filter accepts, and
//! hands them out a burst at a time: the changes in the [`DEBOUNCE`] window after the first
//! one are one [`Batch`], so a save that touches several files, or an editor that writes a
//! temporary file and renames it, is one rebuild. [`Watcher::next`] also ends on Ctrl-C, and a
//! caller wraps its own work in [`Watcher::interruptible`] to make that work end on Ctrl-C too.
//!
//! The paths a batch holds are absolute and resolved ([`resolve`]): the form the platform's
//! events carry (on macOS `/private/var/...` for a `/var/...` directory). A caller compares
//! them with paths it resolved the same way.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// How long to keep collecting changes after the first, so one save is one batch.
pub const DEBOUNCE: Duration = Duration::from_millis(200);

/// Something to watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Root {
	/// One file, which need not exist yet. Its directory is watched, so an editor that replaces
	/// the file by renaming another over it is seen.
	File(PathBuf),
	/// A directory and everything below it, or a file. A path that does not exist yet is watched
	/// from its nearest existing ancestor, and changes elsewhere in that ancestor are ignored.
	Tree(PathBuf),
}

/// The changes of one burst.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
	/// The paths that changed and passed the filter, sorted, each once.
	pub paths: Vec<PathBuf>,
	/// What the platform's watcher reported as having gone wrong, in order.
	pub errors: Vec<String>,
}

impl Batch {
	fn is_empty(&self) -> bool {
		self.paths.is_empty() && self.errors.is_empty()
	}
}

/// What [`Watcher::next`] ended with.
#[derive(Debug)]
pub enum Next {
	Changed(Batch),
	/// Ctrl-C.
	Interrupted,
}

/// Where a change counts: under a root, or exactly a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Accept {
	path: PathBuf,
	exact: bool,
}

type Filter = Box<dyn Fn(&Path) -> bool + Send>;
type Events = UnboundedReceiver<notify::Result<Event>>;

/// A running watch. Dropping it stops watching.
pub struct Watcher {
	events: Events,
	sender: UnboundedSender<notify::Result<Event>>,
	inner: Option<RecommendedWatcher>,
	accept: Vec<Accept>,
	filter: Filter,
	interrupt: Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>,
}

impl Watcher {
	/// Start watching `roots`, keeping the changes `filter` accepts. `filter` sees the resolved
	/// path of each change under a root.
	///
	/// # Errors
	///
	/// Fails when the platform's watcher cannot start or cannot watch a root.
	pub fn start(roots: &[Root], filter: impl Fn(&Path) -> bool + Send + 'static) -> Result<Self> {
		let (sender, events) = unbounded_channel();
		let mut watcher = Self {
			events,
			sender,
			inner: None,
			accept: Vec::new(),
			filter: Box::new(|_| false),
			interrupt: Box::pin(tokio::signal::ctrl_c()),
		};
		watcher.retarget(roots, filter)?;
		Ok(watcher)
	}

	/// Watch `roots` instead of what was watched, with a new `filter`. Ctrl-C stays handled.
	///
	/// # Errors
	///
	/// As [`Watcher::start`]. The previous roots are still watched when it fails.
	pub fn retarget(
		&mut self,
		roots: &[Root],
		filter: impl Fn(&Path) -> bool + Send + 'static,
	) -> Result<()> {
		let (targets, accept) = plan(roots);
		let sender = self.sender.clone();
		let mut inner = notify::recommended_watcher(move |event| {
			let _ = sender.send(event);
		})
		.context("starting the file watcher")?;
		for target in &targets {
			inner
				.watch(&target.path, target.mode)
				.with_context(|| format!("watching {}", target.path.display()))?;
		}
		self.inner = Some(inner);
		self.accept = accept;
		self.filter = Box::new(filter);
		Ok(())
	}

	/// Wait for the next batch of changes, or Ctrl-C.
	///
	/// # Errors
	///
	/// Fails when the platform's watcher stopped, or Ctrl-C cannot be handled.
	pub async fn next(&mut self) -> Result<Next> {
		let Self {
			events,
			accept,
			filter,
			interrupt,
			..
		} = self;
		tokio::select! {
			biased;
			signal = interrupt => {
				signal.context("handling Ctrl-C")?;
				Ok(Next::Interrupted)
			}
			batch = collect(events, accept, filter.as_ref(), DEBOUNCE) => match batch {
				Some(batch) => Ok(Next::Changed(batch)),
				None => bail!("the file watcher stopped"),
			},
		}
	}

	/// Run `work`, giving up on Ctrl-C: `None` when it was interrupted.
	pub async fn interruptible<T>(&mut self, work: impl Future<Output = T>) -> Option<T> {
		tokio::select! {
			biased;
			_ = &mut self.interrupt => None,
			out = work => Some(out),
		}
	}
}

/// The next burst from `events`, or `None` when the channel is closed with nothing collected.
/// The first change that passes starts a window of `window`, and everything in it is one batch.
async fn collect(
	events: &mut Events,
	accept: &[Accept],
	filter: &(dyn Fn(&Path) -> bool + Send),
	window: Duration,
) -> Option<Batch> {
	let mut batch = Batch::default();
	while batch.is_empty() {
		absorb(&mut batch, events.recv().await?, accept, filter);
	}
	let closing = tokio::time::sleep(window);
	tokio::pin!(closing);
	loop {
		tokio::select! {
			_ = &mut closing => break,
			event = events.recv() => match event {
				Some(event) => absorb(&mut batch, event, accept, filter),
				None => break,
			},
		}
	}
	batch.paths.sort();
	batch.paths.dedup();
	Some(batch)
}

/// Add what `event` says changed to `batch`: the paths under a root that `filter` accepts. A
/// read is not a change.
fn absorb(
	batch: &mut Batch,
	event: notify::Result<Event>,
	accept: &[Accept],
	filter: &(dyn Fn(&Path) -> bool + Send),
) {
	match event {
		Ok(event) if matches!(event.kind, EventKind::Access(_)) => {}
		Ok(event) => batch.paths.extend(
			event.paths.into_iter().filter(|path| under(accept, path) && filter(path.as_path())),
		),
		Err(error) => batch.errors.push(error.to_string()),
	}
}

fn under(accept: &[Accept], path: &Path) -> bool {
	accept.iter().any(|a| {
		if a.exact {
			path == a.path
		} else {
			path.starts_with(&a.path)
		}
	})
}

/// One directory handed to the platform's watcher.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
	path: PathBuf,
	mode: RecursiveMode,
}

/// The directories to watch for `roots`, and where a change in them counts. A target below
/// another recursive one is left out, because the outer one sees it.
fn plan(roots: &[Root]) -> (Vec<Target>, Vec<Accept>) {
	let mut wanted: Vec<Target> = Vec::new();
	let mut accept = Vec::new();
	for root in roots {
		let (path, file) = match root {
			Root::File(path) => (resolve(path), true),
			Root::Tree(path) => (resolve(path), false),
		};
		let target = if path.is_dir() && !file {
			Target {
				path: path.clone(),
				mode: RecursiveMode::Recursive,
			}
		} else if path.exists() || file {
			// A file, or a file root that may not exist yet: its directory, when there is one.
			match path.parent().filter(|dir| dir.is_dir()) {
				Some(dir) => Target {
					path: dir.to_path_buf(),
					mode: RecursiveMode::NonRecursive,
				},
				None => Target {
					path: nearest_existing(&path),
					mode: RecursiveMode::Recursive,
				},
			}
		} else {
			Target {
				path: nearest_existing(&path),
				mode: RecursiveMode::Recursive,
			}
		};
		accept.push(Accept {
			path,
			exact: file,
		});
		match wanted.iter_mut().find(|t| t.path == target.path) {
			Some(same) if target.mode == RecursiveMode::Recursive => same.mode = target.mode,
			Some(_) => {}
			None => wanted.push(target),
		}
	}
	let recursive: Vec<PathBuf> = wanted
		.iter()
		.filter(|t| t.mode == RecursiveMode::Recursive)
		.map(|t| t.path.clone())
		.collect();
	wanted.retain(|t| !recursive.iter().any(|outer| t.path.starts_with(outer) && t.path != *outer));
	wanted.sort_by(|a, b| a.path.cmp(&b.path));
	(wanted, accept)
}

fn nearest_existing(path: &Path) -> PathBuf {
	path.ancestors().find(|p| p.exists()).map_or_else(|| path.to_path_buf(), Path::to_path_buf)
}

/// `path` as the platform's events spell it: absolute, without `.` and `..`, and with symbolic
/// links resolved as far as the path exists. The part that does not exist yet is kept as it is.
pub fn resolve(path: &Path) -> PathBuf {
	let absolute = if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
	};
	let mut clean = PathBuf::new();
	for component in absolute.components() {
		match component {
			std::path::Component::CurDir => {}
			std::path::Component::ParentDir => {
				clean.pop();
			}
			other => clean.push(other.as_os_str()),
		}
	}
	let mut missing = Vec::new();
	let mut base = clean.as_path();
	loop {
		if let Ok(mut resolved) = base.canonicalize() {
			resolved.extend(missing.iter().rev());
			return resolved;
		}
		match (base.file_name(), base.parent()) {
			(Some(name), Some(parent)) => {
				missing.push(name.to_os_string());
				base = parent;
			}
			_ => return clean,
		}
	}
}

#[cfg(test)]
mod tests {
	use notify::event::{AccessKind, CreateKind, ModifyKind};

	use super::*;

	const WINDOW: Duration = Duration::from_millis(80);

	fn changed(path: &str) -> notify::Result<Event> {
		Ok(Event::new(EventKind::Modify(ModifyKind::Any)).add_path(PathBuf::from(path)))
	}

	fn everywhere() -> Vec<Accept> {
		vec![Accept {
			path: PathBuf::from("/p"),
			exact: false,
		}]
	}

	fn channel() -> (UnboundedSender<notify::Result<Event>>, Events) {
		unbounded_channel()
	}

	#[tokio::test]
	async fn a_burst_is_one_batch_with_each_path_once() {
		let (send, mut events) = channel();
		for path in ["/p/b", "/p/a", "/p/b", "/p/c"] {
			send.send(changed(path)).unwrap();
		}
		let batch = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert_eq!(batch.paths, [PathBuf::from("/p/a"), "/p/b".into(), "/p/c".into()]);
		assert!(batch.errors.is_empty());
	}

	#[tokio::test]
	async fn changes_after_the_window_are_the_next_batch() {
		let (send, mut events) = channel();
		send.send(changed("/p/a")).unwrap();
		let later = send.clone();
		tokio::spawn(async move {
			tokio::time::sleep(WINDOW * 3).await;
			later.send(changed("/p/b")).unwrap();
		});
		let first = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert_eq!(first.paths, [PathBuf::from("/p/a")]);
		let second = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert_eq!(second.paths, [PathBuf::from("/p/b")]);
	}

	#[tokio::test]
	async fn the_window_runs_from_the_first_change_and_is_not_extended() {
		let (send, mut events) = channel();
		send.send(changed("/p/a")).unwrap();
		let steady = send.clone();
		// A change every 30 ms keeps arriving; the batch still closes after one window.
		tokio::spawn(async move {
			for n in 0..20 {
				tokio::time::sleep(Duration::from_millis(30)).await;
				if steady.send(changed(&format!("/p/{n}"))).is_err() {
					break;
				}
			}
		});
		let start = std::time::Instant::now();
		let batch = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert!(start.elapsed() < WINDOW * 3, "{:?}", start.elapsed());
		assert!(batch.paths.len() < 20, "{:?}", batch.paths);
	}

	#[tokio::test]
	async fn what_the_filter_or_the_roots_exclude_does_not_start_a_window() {
		let (send, mut events) = channel();
		send.send(changed("/elsewhere/a")).unwrap();
		send.send(changed("/p/skip.tmp")).unwrap();
		send.send(Ok(Event::new(EventKind::Access(AccessKind::Any)).add_path("/p/read".into())))
			.unwrap();
		let late = send.clone();
		tokio::spawn(async move {
			tokio::time::sleep(WINDOW).await;
			late.send(changed("/p/keep")).unwrap();
		});
		let batch = collect(
			&mut events,
			&everywhere(),
			&|path: &Path| path.extension().is_none_or(|e| e != "tmp"),
			WINDOW,
		)
		.await
		.unwrap();
		assert_eq!(batch.paths, [PathBuf::from("/p/keep")]);
	}

	#[tokio::test]
	async fn creations_count_and_exact_roots_match_only_that_file() {
		let accept = [Accept {
			path: PathBuf::from("/p/.env"),
			exact: true,
		}];
		let (send, mut events) = channel();
		send.send(Ok(Event::new(EventKind::Create(CreateKind::File)).add_path("/p/.env".into())))
			.unwrap();
		send.send(changed("/p/.env.bak")).unwrap();
		send.send(changed("/p/.env/inner")).unwrap();
		let batch = collect(&mut events, &accept, &|_| true, WINDOW).await.unwrap();
		assert_eq!(batch.paths, [PathBuf::from("/p/.env")]);
	}

	#[tokio::test]
	async fn a_watcher_error_is_reported_with_the_batch_it_arrived_in() {
		let (send, mut events) = channel();
		send.send(Err(notify::Error::generic("the queue overflowed"))).unwrap();
		send.send(changed("/p/a")).unwrap();
		let batch = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert_eq!(batch.paths, [PathBuf::from("/p/a")]);
		assert_eq!(batch.errors, ["the queue overflowed"]);
	}

	#[tokio::test]
	async fn an_error_alone_is_a_batch() {
		let (send, mut events) = channel();
		send.send(Err(notify::Error::generic("gone"))).unwrap();
		let batch = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert!(batch.paths.is_empty());
		assert_eq!(batch.errors, ["gone"]);
	}

	#[tokio::test]
	async fn a_closed_channel_ends_the_stream_after_the_batch_it_held() {
		let (send, mut events) = channel();
		send.send(changed("/p/a")).unwrap();
		drop(send);
		let batch = collect(&mut events, &everywhere(), &|_| true, WINDOW).await.unwrap();
		assert_eq!(batch.paths, [PathBuf::from("/p/a")]);
		assert!(collect(&mut events, &everywhere(), &|_| true, WINDOW).await.is_none());
	}

	#[test]
	fn a_missing_root_is_watched_from_its_nearest_existing_ancestor() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = resolve(dir.path());
		let missing = dir.path().join("database/queries");
		let (targets, accept) = plan(&[Root::Tree(missing)]);
		assert_eq!(
			targets,
			[Target {
				path: root.clone(),
				mode: RecursiveMode::Recursive
			}]
		);
		assert_eq!(
			accept,
			[Accept {
				path: root.join("database/queries"),
				exact: false
			}]
		);
	}

	#[test]
	fn a_file_is_watched_through_its_directory_and_a_nested_target_is_left_out() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = resolve(dir.path());
		std::fs::create_dir_all(root.join("src/lib")).unwrap();
		std::fs::write(root.join("surrealkit.toml"), "").unwrap();
		let (targets, accept) = plan(&[
			Root::File(root.join("surrealkit.toml")),
			Root::File(root.join(".env")),
			Root::Tree(root.join("src")),
			Root::Tree(root.join("src/lib")),
		]);
		assert_eq!(
			targets,
			[
				Target {
					path: root.clone(),
					mode: RecursiveMode::NonRecursive
				},
				Target {
					path: root.join("src"),
					mode: RecursiveMode::Recursive
				},
			]
		);
		assert!(accept.contains(&Accept {
			path: root.join(".env"),
			exact: true
		}));
	}

	#[test]
	fn resolve_removes_dots_and_keeps_what_does_not_exist() {
		let dir = tempfile::TempDir::new().unwrap();
		let root = resolve(dir.path());
		std::fs::create_dir(root.join("a")).unwrap();
		assert_eq!(resolve(&root.join("a/./../a/b/c")), root.join("a/b/c"));
	}
}
