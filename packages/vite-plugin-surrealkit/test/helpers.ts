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
	write(relativePath: string, contents: string): void;
	remove(relativePath: string): void;
	cleanup(): void;
}

const FAKE_BINARY = `#!/bin/sh
echo "fake surrealkit: $*"
printf '%s\\n' "$*" >> "$SURREALKIT_TEST_LOG"
if [ -f "$SURREALKIT_TEST_EXIT_FILE" ]; then
	echo "fake surrealkit: failing on purpose" >&2
	exit "$(cat "$SURREALKIT_TEST_EXIT_FILE")"
fi
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
	const exitFile = path.join(root, 'exit-code');

	fs.writeFileSync(binary, FAKE_BINARY, { mode: 0o755 });
	fs.writeFileSync(log, '');

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
		env: { SURREALKIT_TEST_LOG: log, SURREALKIT_TEST_EXIT_FILE: exitFile },
		invocations: () =>
			fs
				.readFileSync(log, 'utf8')
				.split('\n')
				.filter((line) => line.length > 0),
		failWith: (code: number) => {
			fs.writeFileSync(exitFile, String(code));
		},
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
