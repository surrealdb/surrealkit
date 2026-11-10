import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/** A throwaway Vite project wired to a fake `surrealkit` executable. */
export interface Fixture {
	/** Vite root. */
	root: string;
	/** Absolute path to the fake `surrealkit` binary. */
	binary: string;
	/** Env the plugin must forward to the fake binary. */
	env: Record<string, string>;
	/** One entry per `surrealkit` invocation, each the joined argv. */
	invocations(): string[];
	/** Make the next invocations exit non-zero. */
	failWith(code: number): void;
	/** Make the next invocations succeed again. */
	succeed(): void;
	/** What the next `compile --watch-paths` prints; defaults to empty lists. */
	watchPaths(paths: Partial<WatchPathsDocument>): void;
	/** Make the next `compile --watch-paths` print `text` verbatim. */
	watchPathsRaw(text: string): void;
	/** Make the next `compile --watch-paths` exit non-zero. */
	failWatchPaths(code: number): void;
	/** One entry per `compile --watch-paths` call, each the joined argv. */
	watchPathsCalls(): string[];
	/** Make the next invocations sleep this many seconds first. */
	sleepFor(seconds: number): void;
	/** What the next successful invocation publishes to the generated file. */
	publish(contents: string): void;
	/** Path of the generated file, relative to the root. */
	readonly generatedFile: string;
	/** Lifecycle events of the fake binary: done, failed, terminated. */
	events(): string[];
	write(relativePath: string, contents: string): void;
	remove(relativePath: string): void;
	cleanup(): void;
}

/** The `compile --watch-paths` document. Paths are relative to the cwd. */
export interface WatchPathsDocument {
	version: number;
	schema: string[];
	config: string[];
	queryFiles: string[];
	querySources: string[];
	queryExtensions: string[];
	queryIgnoredDirectories: string[];
	outputs: string[];
}

const EMPTY_WATCH_PATHS: WatchPathsDocument = {
	version: 1,
	schema: [],
	config: [],
	queryFiles: [],
	querySources: [],
	queryExtensions: [],
	queryIgnoredDirectories: [],
	outputs: [],
};

// `compile --watch-paths` prints a configured document (or exits non-zero)
// and is logged apart from the generations, so invocation counts stay simple.
// Otherwise it logs its argv, then optionally sleeps (interruptibly, so SIGTERM lands at
// once), fails, or "publishes" a generated file; lifecycle events go to a
// separate log so invocation counts stay simple. The trap comes first: tests
// signal the process as soon as its invocation is logged.
const FAKE_BINARY = `#!/bin/sh
trap 'printf "terminated\\n" >> "$SURREALKIT_TEST_EVENTS"; exit 143' TERM
case " $* " in
*" --watch-paths "*)
	printf '%s\\n' "$*" >> "$SURREALKIT_TEST_WATCH_LOG"
	if [ -f "$SURREALKIT_TEST_WATCH_EXIT_FILE" ]; then
		echo "fake surrealkit: watch-paths failing on purpose" >&2
		exit "$(cat "$SURREALKIT_TEST_WATCH_EXIT_FILE")"
	fi
	cat "$SURREALKIT_TEST_WATCH_FILE"
	exit 0
	;;
esac
echo "fake surrealkit: $*"
printf '%s\\n' "$*" >> "$SURREALKIT_TEST_LOG"
if [ -f "$SURREALKIT_TEST_SLEEP_FILE" ]; then
	sleep "$(cat "$SURREALKIT_TEST_SLEEP_FILE")" &
	wait $!
fi
if [ -f "$SURREALKIT_TEST_EXIT_FILE" ]; then
	echo "fake surrealkit: failing on purpose" >&2
	printf 'failed\\n' >> "$SURREALKIT_TEST_EVENTS"
	exit "$(cat "$SURREALKIT_TEST_EXIT_FILE")"
fi
if [ -f "$SURREALKIT_TEST_OUTPUT_SOURCE" ]; then
	mkdir -p "$(dirname "$SURREALKIT_TEST_OUTPUT")"
	cmp -s "$SURREALKIT_TEST_OUTPUT_SOURCE" "$SURREALKIT_TEST_OUTPUT" || cp "$SURREALKIT_TEST_OUTPUT_SOURCE" "$SURREALKIT_TEST_OUTPUT"
fi
printf 'done\\n' >> "$SURREALKIT_TEST_EVENTS"
exit 0
`;

export function createFixture(): Fixture {
	// realpath so that macOS' /var -> /private/var symlink does not make the
	// watcher report paths the plugin's root-relative matcher cannot match.
	const base = fs.mkdtempSync(
		path.join(fs.realpathSync(os.tmpdir()), 'surrealkit-vite-'),
	);
	const root = path.join(base, 'project');
	fs.mkdirSync(root);

	// The harness's own files stay outside the Vite root. Inside it, the watcher
	// reports the binary's log writes as updates arriving during a generation,
	// which the plugin holds back and turns into a reload.
	const binary = path.join(base, 'fake-surrealkit');
	const log = path.join(base, 'invocations.log');
	const events = path.join(base, 'events.log');
	const exitFile = path.join(base, 'exit-code');
	const sleepFile = path.join(base, 'sleep-seconds');
	const outputSource = path.join(base, 'next-output');
	const watchLog = path.join(base, 'watch-paths.log');
	const watchFile = path.join(base, 'watch-paths.json');
	const watchExitFile = path.join(base, 'watch-paths-exit-code');
	const generatedFile = 'src/generated/schema.ts';

	fs.writeFileSync(binary, FAKE_BINARY, { mode: 0o755 });
	fs.writeFileSync(log, '');
	fs.writeFileSync(events, '');
	fs.writeFileSync(watchLog, '');
	fs.writeFileSync(watchFile, JSON.stringify(EMPTY_WATCH_PATHS));

	const lines = (file: string) =>
		fs
			.readFileSync(file, 'utf8')
			.split('\n')
			.filter((line) => line.length > 0);

	const write = (relativePath: string, contents: string): void => {
		const target = path.join(root, relativePath);
		fs.mkdirSync(path.dirname(target), { recursive: true });
		fs.writeFileSync(target, contents);
	};

	write(
		'index.html',
		'<!doctype html><script type="module" src="/src/main.js"></script>',
	);
	write('src/main.js', 'export const app = "surrealkit";\n');
	write('database/schema/user.surql', 'DEFINE TABLE user SCHEMAFULL;\n');
	write('database/modules/billing/schema/plan.surql', 'DEFINE TABLE plan;\n');

	return {
		root,
		binary,
		env: {
			SURREALKIT_TEST_LOG: log,
			SURREALKIT_TEST_EVENTS: events,
			SURREALKIT_TEST_EXIT_FILE: exitFile,
			SURREALKIT_TEST_SLEEP_FILE: sleepFile,
			SURREALKIT_TEST_OUTPUT_SOURCE: outputSource,
			SURREALKIT_TEST_OUTPUT: path.join(root, generatedFile),
			SURREALKIT_TEST_WATCH_LOG: watchLog,
			SURREALKIT_TEST_WATCH_FILE: watchFile,
			SURREALKIT_TEST_WATCH_EXIT_FILE: watchExitFile,
		},
		invocations: () => lines(log),
		events: () => lines(events),
		watchPaths: (paths) => {
			fs.writeFileSync(
				watchFile,
				JSON.stringify({ ...EMPTY_WATCH_PATHS, ...paths }),
			);
		},
		watchPathsRaw: (text: string) => {
			fs.writeFileSync(watchFile, text);
		},
		failWatchPaths: (code: number) => {
			fs.writeFileSync(watchExitFile, String(code));
		},
		watchPathsCalls: () => lines(watchLog),
		failWith: (code: number) => {
			fs.writeFileSync(exitFile, String(code));
		},
		succeed: () => {
			fs.rmSync(exitFile, { force: true });
		},
		sleepFor: (seconds: number) => {
			if (seconds <= 0) fs.rmSync(sleepFile, { force: true });
			else fs.writeFileSync(sleepFile, String(seconds));
		},
		publish: (contents: string) => {
			fs.writeFileSync(outputSource, contents);
		},
		generatedFile,
		write,
		remove: (relativePath: string) => {
			fs.rmSync(path.join(root, relativePath), { force: true });
		},
		cleanup: () => {
			fs.rmSync(base, { recursive: true, force: true });
		},
	};
}

/** Poll until `predicate` holds, or throw once `timeout` elapses. */
export async function waitFor(
	predicate: () => boolean,
	message: string,
	timeout = 8000,
): Promise<void> {
	const deadline = Date.now() + timeout;

	while (Date.now() < deadline) {
		if (predicate()) return;
		await sleep(25);
	}

	throw new Error(`timed out waiting for ${message}`);
}

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Give the watcher a moment to finish its initial scan. Without this, writes
 * that land mid-scan are silently folded into the initial listing.
 */
export async function settleWatcher(): Promise<void> {
	await sleep(300);
}
