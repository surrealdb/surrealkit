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

// Logs its argv, then optionally sleeps (interruptibly, so SIGTERM lands at
// once), fails, or "publishes" a generated file; lifecycle events go to a
// separate log so invocation counts stay simple.
const FAKE_BINARY = `#!/bin/sh
echo "fake surrealkit: $*"
printf '%s\\n' "$*" >> "$SURREALKIT_TEST_LOG"
trap 'printf "terminated\\n" >> "$SURREALKIT_TEST_EVENTS"; exit 143' TERM
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
	const root = fs.mkdtempSync(
		path.join(fs.realpathSync(os.tmpdir()), 'surrealkit-vite-'),
	);

	const binary = path.join(root, 'fake-surrealkit');
	const log = path.join(root, 'invocations.log');
	const events = path.join(root, 'events.log');
	const exitFile = path.join(root, 'exit-code');
	const sleepFile = path.join(root, 'sleep-seconds');
	const outputSource = path.join(root, 'next-output');
	const generatedFile = 'src/generated/schema.ts';

	fs.writeFileSync(binary, FAKE_BINARY, { mode: 0o755 });
	fs.writeFileSync(log, '');
	fs.writeFileSync(events, '');

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
		},
		invocations: () => lines(log),
		events: () => lines(events),
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
			fs.rmSync(root, { recursive: true, force: true });
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
