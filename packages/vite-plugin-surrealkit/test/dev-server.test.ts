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

/**
 * Boot a dev server on the fixture. No `listen()` - the watcher and every
 * plugin hook are already live, and skipping it keeps tests off real ports.
 */
async function startServer(
	options: SurrealkitPluginOptions = {},
	{ middlewareMode = false } = {},
): Promise<ViteDevServer> {
	if (!fixture) throw new Error('fixture not created');

	server = await createServer({
		configFile: false,
		root: fixture.root,
		logLevel: 'silent',
		server: middlewareMode ? { middlewareMode: true } : {},
		appType: middlewareMode ? 'custom' : 'spa',
		plugins: [
			surrealkitPlugin({
				binary: fixture.binary,
				env: fixture.env,
				debounceMs: 20,
				...options,
			}),
		],
	});

	return server;
}

describe('dev server', () => {
	it('syncs once on startup', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'the startup sync',
		);
		expect(fixture.invocations()).toEqual(['sync']);
	});

	// Regression test for Vite 8. The plugin used to hand raw globs to
	// `watcher.add()`; because Vite disables chokidar globbing those register
	// as non-existent literal paths, which on Vite 8 suppressed change events
	// for the real files next to them and killed schema watching entirely.
	it('re-syncs when a watched schema file changes', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');

		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync triggered by the change',
		);
	});

	it('re-syncs when a schema file is added', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('database/schema/post.surql', 'DEFINE TABLE post;\n');

		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync triggered by the new file',
		);
	});

	it('re-syncs when a schema file is removed', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.remove('database/schema/user.surql');

		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync triggered by the removal',
		);
	});

	it('watches named module schema directories by default', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write(
			'database/modules/billing/schema/plan.surql',
			'DEFINE TABLE plan SCHEMAFULL;\n',
		);

		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync for the module schema',
		);
	});

	it('ignores files that do not match the schema globs', async () => {
		fixture = createFixture();
		await startServer();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('src/main.js', 'export const app = "changed";\n');
		fixture.write('database/schema/notes.txt', 'not a schema\n');
		await sleep(500);

		expect(fixture.invocations()).toHaveLength(1);
	});

	it('honours custom schema globs', async () => {
		fixture = createFixture();
		await startServer({ schemaGlobs: ['db/**/*.surql'] });

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE ignored;\n');
		await sleep(400);
		expect(fixture.invocations()).toHaveLength(1);

		fixture.write('db/custom.surql', 'DEFINE TABLE custom;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync for the custom glob',
		);
	});

	it('coalesces rapid edits into a single sync', async () => {
		fixture = createFixture();
		await startServer({ debounceMs: 200 });

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		for (let i = 0; i < 5; i++) {
			fixture.write(
				'database/schema/user.surql',
				`DEFINE TABLE u${i};\n`,
			);
			await sleep(20);
		}

		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'the debounced sync',
		);
		await sleep(600);

		expect(fixture.invocations()).toHaveLength(2);
	});

	it('skips the startup sync when runOnStartup is false', async () => {
		fixture = createFixture();
		await startServer({ runOnStartup: false });
		await sleep(500);

		expect(fixture.invocations()).toHaveLength(0);
	});

	it('does nothing when include omits "serve"', async () => {
		fixture = createFixture();
		await startServer({ include: ['build'] });
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await sleep(500);

		expect(fixture.invocations()).toHaveLength(0);
	});

	it('sends a full reload after a schema-triggered sync', async () => {
		fixture = createFixture();
		const devServer = await startServer({ reloadOnSync: true });

		const payloads: unknown[] = [];
		const send = devServer.hot.send.bind(devServer.hot);
		devServer.hot.send = ((...args: unknown[]) => {
			payloads.push(args[0]);
			return (send as (...a: unknown[]) => unknown)(...args);
		}) as typeof devServer.hot.send;

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(
			() =>
				payloads.some(
					(p) => (p as { type?: string })?.type === 'full-reload',
				),
			'a full-reload payload',
		);
	});

	it('works in middleware mode, where httpServer is null', async () => {
		fixture = createFixture();
		const devServer = await startServer({}, { middlewareMode: true });
		expect(devServer.httpServer).toBeNull();

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a sync in middleware mode',
		);
	});

	it('stops syncing once the server is closed', async () => {
		fixture = createFixture();
		const devServer = await startServer({ debounceMs: 300 });

		await waitFor(
			() => fixture?.invocations().length === 1,
			'startup sync',
		);
		await settleWatcher();

		// Queue a debounced sync, then close before the timer fires.
		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await sleep(50);
		await devServer.close();
		server = undefined;

		const afterClose = fixture.invocations().length;
		fixture.write('database/schema/user.surql', 'DEFINE TABLE later;\n');
		await sleep(800);

		expect(fixture.invocations()).toHaveLength(afterClose);
	});
});
