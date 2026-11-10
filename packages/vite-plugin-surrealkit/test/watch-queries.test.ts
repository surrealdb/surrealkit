import { createServer, type ViteDevServer } from 'vite';
import { afterEach, describe, expect, it } from 'vitest';

import {
	type SurrealkitPluginOptions,
	surrealkitPlugin,
} from '../src/index.js';
import {
	createFixture,
	type Fixture,
	settleWatcher,
	sleep,
	waitFor,
} from './helpers.js';

let fixture: Fixture | undefined;
let server: ViteDevServer | undefined;

afterEach(async () => {
	await server?.close();
	server = undefined;
	fixture?.cleanup();
	fixture = undefined;
});

const EXTENSIONS = ['ts', 'svelte', 'vue'];
const IGNORED = ['node_modules', 'dist'];

/** Starts a generate-mode dev server; returns the warnings it logged. */
async function startServer(
	options: SurrealkitPluginOptions = {},
): Promise<string[]> {
	if (!fixture) throw new Error('fixture not created');

	const warnings: string[] = [];
	server = await createServer({
		configFile: false,
		root: fixture.root,
		logLevel: 'info',
		appType: 'spa',
		customLogger: {
			error: () => undefined,
			info: () => undefined,
			warn: (message: string) => {
				warnings.push(message);
			},
			warnOnce: () => undefined,
			clearScreen: () => undefined,
			hasErrorLogged: () => false,
			hasWarned: false,
		},
		plugins: [
			surrealkitPlugin({
				mode: 'generate',
				binary: fixture.binary,
				env: fixture.env,
				debounceMs: 20,
				...options,
			}),
		],
	});

	return warnings;
}

/** A project with host sources, a query directory and a generated output. */
function createQueryFixture(): Fixture {
	const created = createFixture();
	created.write('src/routes/page.svelte', '<script>1</script>\n');
	created.write('src/lib/user.ts', 'export const user = 1;\n');
	created.write('database/queries/get_user.surql', 'SELECT * FROM user;\n');
	created.write('surrealkit.toml', '[project]\n');
	created.watchPaths({
		config: ['surrealkit.toml'],
		queryFiles: ['database/queries'],
		querySources: ['src'],
		queryExtensions: EXTENSIONS,
		queryIgnoredDirectories: IGNORED,
		outputs: ['src/generated/queries.ts', 'src/generated/surreal'],
	});
	return created;
}

const compiles = (f: Fixture) => f.invocations().length;

/** Waits until `count` compile runs have finished (the startup run is one). */
const waitForCompiles = (f: Fixture, count: number, what: string) =>
	waitFor(() => compiles(f) >= count, what);

describe('query source watching (generate mode)', () => {
	it('asks the binary once for the watch paths, with the schema args', async () => {
		fixture = createQueryFixture();
		await startServer({ schemas: ['core'], generateArgs: ['--strict'] });

		expect(fixture.watchPathsCalls()).toEqual([
			'compile --watch-paths --schema core',
		]);
		expect(fixture.invocations()).toEqual([
			'compile --schema core --strict',
		]);
	});

	it('compiles once per edited host file under a query source', async () => {
		fixture = createQueryFixture();
		await startServer();
		await settleWatcher();

		fixture.write('src/routes/page.svelte', '<script>2</script>\n');
		await waitForCompiles(fixture, 2, 'the .svelte edit to compile');
		fixture.write('src/lib/user.ts', 'export const user = 2;\n');
		await waitForCompiles(fixture, 3, 'the .ts edit to compile');

		await sleep(300);
		expect(fixture.invocations()).toEqual([
			'compile',
			'compile',
			'compile',
		]);
	});

	it('debounces a burst of edits into one compile', async () => {
		fixture = createQueryFixture();
		await startServer({ debounceMs: 200 });
		await settleWatcher();

		fixture.write('src/lib/user.ts', 'export const user = 2;\n');
		fixture.write('src/lib/user.ts', 'export const user = 3;\n');
		fixture.write('src/routes/page.svelte', '<script>2</script>\n');
		await waitForCompiles(fixture, 2, 'the burst to compile');

		await sleep(500);
		expect(compiles(fixture)).toBe(2);
	});

	it('ignores declaration files, other extensions, ignored and hidden directories, and files outside the roots', async () => {
		fixture = createQueryFixture();
		fixture.write('lib/elsewhere.ts', 'export {};\n');
		await startServer();
		await settleWatcher();

		fixture.write('src/types.d.ts', 'export {};\n');
		fixture.write('src/notes.md', '# notes\n');
		fixture.write('src/dist/bundle.ts', 'export {};\n');
		fixture.write('src/node_modules/dep/index.ts', 'export {};\n');
		fixture.write('src/.cache/hidden.ts', 'export {};\n');
		fixture.write('src/lib/.hidden.ts', 'export {};\n');
		fixture.write('lib/elsewhere.ts', 'export const x = 1;\n');
		await sleep(500);

		expect(fixture.invocations()).toEqual(['compile']);
	});

	it('compiles when a named query file under queryFiles changes', async () => {
		fixture = createQueryFixture();
		await startServer();
		await settleWatcher();

		fixture.write(
			'database/queries/get_user.surql',
			'SELECT id FROM user;\n',
		);
		await waitForCompiles(fixture, 2, 'the .surql edit to compile');
		expect(fixture.invocations()).toEqual(['compile', 'compile']);
	});

	it('never compiles for generated outputs or temp siblings', async () => {
		fixture = createQueryFixture();
		await startServer();
		await settleWatcher();

		fixture.write('src/generated/queries.ts', 'export const q = 1;\n');
		fixture.write('src/generated/surreal/user.ts', 'export {};\n');
		fixture.write('src/lib/user.surrealkit-tmp-42.ts', 'export {};\n');
		await sleep(500);

		expect(fixture.invocations()).toEqual(['compile']);
	});

	it('still compiles for schema files', async () => {
		fixture = createQueryFixture();
		await startServer();
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitForCompiles(fixture, 2, 'the schema edit to compile');
	});

	it('resolves the paths against the binary cwd', async () => {
		fixture = createFixture();
		fixture.write('sub/src/a.ts', 'export {};\n');
		fixture.write('src/b.ts', 'export {};\n');
		fixture.watchPaths({
			querySources: ['src'],
			queryExtensions: EXTENSIONS,
		});
		await startServer({ cwd: `${fixture.root}/sub` });
		await settleWatcher();

		fixture.write('src/b.ts', 'export const b = 1;\n');
		await sleep(400);
		expect(compiles(fixture)).toBe(1);

		fixture.write('sub/src/a.ts', 'export const a = 1;\n');
		await waitForCompiles(fixture, 2, 'the cwd-relative edit to compile');
	});

	it('re-reads the watch paths when a config file changes', async () => {
		fixture = createQueryFixture();
		fixture.write('web/page.ts', 'export {};\n');
		await startServer();
		await settleWatcher();
		expect(fixture.watchPathsCalls()).toHaveLength(1);

		fixture.watchPaths({
			config: ['surrealkit.toml'],
			querySources: ['web'],
			queryExtensions: EXTENSIONS,
			queryIgnoredDirectories: IGNORED,
		});
		fixture.write('surrealkit.toml', '[queries]\nsources = ["web"]\n');
		await waitFor(
			() => (fixture?.watchPathsCalls().length ?? 0) === 2,
			'the watch paths to be read again',
		);
		// The config edit itself compiles nothing.
		await sleep(300);
		expect(compiles(fixture)).toBe(1);

		fixture.write('web/page.ts', 'export const page = 1;\n');
		await waitForCompiles(fixture, 2, 'the new root to compile');

		// The old root no longer counts.
		fixture.write('src/lib/user.ts', 'export const user = 9;\n');
		await sleep(400);
		expect(compiles(fixture)).toBe(2);
	});

	it('keeps the previous paths when re-reading them fails', async () => {
		fixture = createQueryFixture();
		const warnings = await startServer();
		await settleWatcher();

		fixture.failWatchPaths(2);
		fixture.write('surrealkit.toml', '[broken\n');
		await waitFor(() => warnings.length === 1, 'the warning');
		expect(warnings[0]).toMatch(/keeping the previous query paths/);

		fixture.write('src/lib/user.ts', 'export const user = 2;\n');
		await waitForCompiles(fixture, 2, 'the edit to compile');
	});
});

describe('query source watching: fallback', () => {
	const unusable: Array<[string, (f: Fixture) => void, RegExp]> = [
		[
			'a non-zero exit',
			(f) => f.failWatchPaths(2),
			/failed \(exit code 2\)/,
		],
		[
			'output that is not JSON',
			(f) => f.watchPathsRaw('garbage'),
			/not JSON/,
		],
		[
			'another version',
			(f) =>
				f.watchPaths({
					version: 2,
					querySources: ['src'],
					queryExtensions: EXTENSIONS,
				}),
			/version 2; expected 1/,
		],
		[
			'a missing array',
			(f) => f.watchPathsRaw('{"version":1,"schema":[]}'),
			/`config` is not an array of strings/,
		],
	];

	it.each(
		unusable,
	)('warns once and keeps schema watching on %s', async (_name, arrange, cause) => {
		fixture = createQueryFixture();
		arrange(fixture);
		const warnings = await startServer();
		await settleWatcher();

		expect(warnings).toHaveLength(1);
		expect(warnings[0]).toMatch(/--watch-paths/);
		expect(warnings[0]).toMatch(cause);
		expect(warnings[0]).toMatch(/query sources are not watched/);

		// Query sources are not watched; schema files still are.
		fixture.write('src/lib/user.ts', 'export const user = 2;\n');
		await sleep(400);
		expect(compiles(fixture)).toBe(1);

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitForCompiles(fixture, 2, 'the schema edit to compile');
		expect(warnings).toHaveLength(1);
	});

	it('does not ask the binary when watchQueries is false', async () => {
		fixture = createQueryFixture();
		const warnings = await startServer({ watchQueries: false });
		await settleWatcher();

		expect(fixture.watchPathsCalls()).toEqual([]);

		fixture.write('src/lib/user.ts', 'export const user = 2;\n');
		fixture.write('database/queries/get_user.surql', 'SELECT 1;\n');
		await sleep(400);
		expect(compiles(fixture)).toBe(1);

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitForCompiles(fixture, 2, 'the schema edit to compile');
		expect(warnings).toEqual([]);
	});

	it('does not ask the binary in sync mode', async () => {
		fixture = createQueryFixture();
		server = await createServer({
			configFile: false,
			root: fixture.root,
			logLevel: 'silent',
			appType: 'spa',
			plugins: [
				surrealkitPlugin({
					binary: fixture.binary,
					env: fixture.env,
					debounceMs: 20,
				}),
			],
		});
		await waitFor(() => compiles(fixture as Fixture) >= 1, 'the sync');

		expect(fixture.watchPathsCalls()).toEqual([]);
	});
});

describe('watchQueries option', () => {
	it('is rejected in sync mode', () => {
		expect(() => surrealkitPlugin({ watchQueries: false })).toThrow(
			/watchQueries only apply to mode 'generate'/,
		);
		expect(() =>
			surrealkitPlugin({ mode: 'sync', watchQueries: true }),
		).toThrow(/watchQueries only apply to mode 'generate'/);
	});

	it('is accepted in generate mode', () => {
		expect(() =>
			surrealkitPlugin({ mode: 'generate', watchQueries: false }),
		).not.toThrow();
	});
});
