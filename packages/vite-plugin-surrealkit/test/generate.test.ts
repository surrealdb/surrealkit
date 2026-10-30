import { build, createServer, type Plugin, type ViteDevServer } from 'vite';
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

async function startServer(
	options: SurrealkitPluginOptions = {},
): Promise<ViteDevServer> {
	if (!fixture) throw new Error('fixture not created');

	server = await createServer({
		configFile: false,
		root: fixture.root,
		logLevel: 'silent',
		appType: 'spa',
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

	return server;
}

/** Records every HMR payload the dev server sends. */
function recordPayloads(devServer: ViteDevServer): Array<{ type?: string }> {
	const payloads: Array<{ type?: string }> = [];
	const send = devServer.hot.send.bind(devServer.hot);
	devServer.hot.send = ((...args: unknown[]) => {
		payloads.push(args[0] as { type?: string });
		return (send as (...a: unknown[]) => unknown)(...args);
	}) as typeof devServer.hot.send;
	return payloads;
}

const reloads = (payloads: Array<{ type?: string }>) =>
	payloads.filter((p) => p?.type === 'full-reload').length;

describe('generate mode (dev server)', () => {
	it('runs `surrealkit compile`, never `sync`', async () => {
		fixture = createFixture();
		await startServer();
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'a generation triggered by the change',
		);
		expect(fixture.invocations()).toEqual(['compile', 'compile']);
	});

	it('waits for the first generation before the server is ready', async () => {
		fixture = createFixture();
		fixture.sleepFor(0.6);
		await startServer();

		// createServer resolved: the startup generation already finished.
		expect(fixture.invocations()).toEqual(['compile']);
		expect(fixture.events()).toEqual(['done']);
	});

	it('still starts when the first generation fails, and says why', async () => {
		fixture = createFixture();
		fixture.failWith(3);
		const errors: string[] = [];
		const devServer = await createServer({
			configFile: false,
			root: fixture.root,
			logLevel: 'error',
			customLogger: {
				error: (message: string) => {
					errors.push(message);
				},
				info: () => undefined,
				warn: () => undefined,
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
				}),
			],
		});
		server = devServer;

		expect(fixture.events()).toEqual(['failed']);
		expect(errors.join('\n')).toMatch(/compile failed \(exit code 3\)/);
	});

	it('forwards schemas and generateArgs', async () => {
		fixture = createFixture();
		await startServer({
			schemas: ['core', 'billing'],
			generateArgs: ['--strict'],
		});

		expect(fixture.invocations()).toEqual([
			'compile --schema core --schema billing --strict',
		]);
	});

	it('terminates a running generation when the server closes', async () => {
		fixture = createFixture();
		const devServer = await startServer();
		await settleWatcher();

		fixture.sleepFor(30);
		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'the long generation to start',
		);

		const started = Date.now();
		await devServer.close();
		server = undefined;

		expect(Date.now() - started).toBeLessThan(5000);
		expect(fixture.events()).toEqual(['done', 'terminated']);
	});

	it('reloads once after a successful generation that changed the output', async () => {
		fixture = createFixture();
		fixture.publish('export const v = 1;\n');
		const devServer = await startServer({
			generatedDirs: ['src/generated'],
		});
		const payloads = recordPayloads(devServer);
		await settleWatcher();

		fixture.publish('export const v = 2;\n');
		fixture.write('database/schema/user.surql', 'DEFINE TABLE v2;\n');
		await waitFor(() => reloads(payloads) === 1, 'a full reload');

		// Successful but unchanged output: no reload.
		fixture.write('database/schema/user.surql', 'DEFINE TABLE v2b;\n');
		await waitFor(
			() => (fixture?.events().length ?? 0) >= 3,
			'the unchanged generation',
		);
		await sleep(200);
		expect(reloads(payloads)).toBe(1);
	});

	it('never reloads after a failed generation', async () => {
		fixture = createFixture();
		const devServer = await startServer({
			generatedDirs: ['src/generated'],
		});
		const payloads = recordPayloads(devServer);
		await settleWatcher();

		fixture.failWith(1);
		fixture.publish('export const v = 3;\n');
		fixture.write('database/schema/user.surql', 'DEFINE TABLE broken;\n');
		await waitFor(
			() => fixture?.events().includes('failed') ?? false,
			'the failed generation',
		);
		await sleep(200);
		expect(reloads(payloads)).toBe(0);
	});

	it('reloads after every successful generation when no generatedDirs are given', async () => {
		fixture = createFixture();
		const devServer = await startServer();
		const payloads = recordPayloads(devServer);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(() => reloads(payloads) === 1, 'a full reload');
	});

	it('does not reload when reloadOnGenerate is false', async () => {
		fixture = createFixture();
		const devServer = await startServer({ reloadOnGenerate: false });
		const payloads = recordPayloads(devServer);
		await settleWatcher();

		fixture.write('database/schema/user.surql', 'DEFINE TABLE user;\n');
		await waitFor(
			() => (fixture?.events().length ?? 0) >= 2,
			'the generation',
		);
		await sleep(200);
		expect(reloads(payloads)).toBe(0);
	});

	it('suppresses per-file updates of generated output', async () => {
		fixture = createFixture();
		const devServer = await startServer({
			generatedDirs: ['src/generated'],
		});
		const plugin = devServer.config.plugins.find(
			(p) => p.name === 'vite-plugin-surrealkit',
		) as Plugin;
		const hotUpdate = (
			typeof plugin.hotUpdate === 'function'
				? plugin.hotUpdate
				: plugin.hotUpdate?.handler
		) as (options: { file: string }) => unknown;
		const at = (relative: string) => ({
			file: `${fixture?.root}/${relative}`,
		});

		expect(hotUpdate.call({}, at(fixture.generatedFile))).toEqual([]);
		expect(hotUpdate.call({}, at('src/main.js'))).toBeUndefined();
	});

	const hotUpdateOf = (devServer: ViteDevServer) => {
		const plugin = devServer.config.plugins.find(
			(p) => p.name === 'vite-plugin-surrealkit',
		) as Plugin;
		const hook = (
			typeof plugin.hotUpdate === 'function'
				? plugin.hotUpdate
				: plugin.hotUpdate?.handler
		) as (options: { file: string }) => unknown;
		return (relative: string) =>
			hook.call({}, { file: `${fixture?.root}/${relative}` });
	};

	it('holds back every per-file update while a generation runs, then reloads once', async () => {
		fixture = createFixture();
		// No generatedDirs: the emitters may write anywhere.
		const devServer = await startServer();
		const payloads = recordPayloads(devServer);
		const hotUpdate = hotUpdateOf(devServer);
		await settleWatcher();

		fixture.sleepFor(1);
		fixture.write('database/schema/user.surql', 'DEFINE TABLE slow;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'the generation to start',
		);
		expect(hotUpdate('src/generated/half.ts')).toEqual([]);
		expect(hotUpdate('src/main.js')).toEqual([]);
		await waitFor(() => reloads(payloads) === 1, 'a full reload');
		// Idle again: per-file updates flow.
		expect(hotUpdate('src/main.js')).toBeUndefined();
	});

	it('reloads for held-back updates even when the output did not change', async () => {
		fixture = createFixture();
		const devServer = await startServer({
			generatedDirs: ['src/generated'],
			reloadOnGenerate: false,
		});
		const payloads = recordPayloads(devServer);
		const hotUpdate = hotUpdateOf(devServer);
		await settleWatcher();

		fixture.sleepFor(1);
		fixture.write('database/schema/user.surql', 'DEFINE TABLE same;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'the generation to start',
		);
		expect(hotUpdate('src/main.js')).toEqual([]);
		await waitFor(() => reloads(payloads) === 1, 'a full reload');
	});

	it('never reloads after a failed generation, even with held-back updates', async () => {
		fixture = createFixture();
		const devServer = await startServer({
			generatedDirs: ['src/generated'],
		});
		const payloads = recordPayloads(devServer);
		const hotUpdate = hotUpdateOf(devServer);
		await settleWatcher();

		fixture.failWith(1);
		fixture.sleepFor(1);
		fixture.write('database/schema/user.surql', 'DEFINE TABLE broken;\n');
		await waitFor(
			() => (fixture?.invocations().length ?? 0) >= 2,
			'the generation to start',
		);
		expect(hotUpdate('src/main.js')).toEqual([]);
		await waitFor(
			() => fixture?.events().includes('failed') ?? false,
			'the failed generation',
		);
		await sleep(200);
		expect(reloads(payloads)).toBe(0);
	});
});

describe('generate mode (build)', () => {
	const runBuild = (options: SurrealkitPluginOptions = {}) => {
		if (!fixture) throw new Error('fixture not created');
		return build({
			configFile: false,
			root: fixture.root,
			logLevel: 'silent',
			plugins: [
				surrealkitPlugin({
					mode: 'generate',
					binary: fixture.binary,
					env: fixture.env,
					include: ['build'],
					...options,
				}),
			],
		});
	};

	it('generates once before bundling', async () => {
		fixture = createFixture();
		await runBuild();
		expect(fixture.invocations()).toEqual(['compile']);
	});

	it('fails the build when generation fails', async () => {
		fixture = createFixture();
		fixture.failWith(2);
		await expect(runBuild()).rejects.toThrow(
			/compile failed \(exit code 2\)/,
		);
	});
});

describe('options', () => {
	it('rejects database options in generate mode', () => {
		expect(() =>
			surrealkitPlugin({ mode: 'generate', targets: ['x'] }),
		).toThrow(
			/targets only apply to mode 'sync'; mode 'generate' never connects to a database/,
		);
		expect(() =>
			surrealkitPlugin({
				mode: 'generate',
				all: true,
				syncArgs: ['--x'],
			}),
		).toThrow(/all, syncArgs only apply to mode 'sync'/);
	});

	it('rejects generate options in sync mode', () => {
		expect(() =>
			surrealkitPlugin({ generatedDirs: ['src/generated'] }),
		).toThrow(/generatedDirs only apply to mode 'generate'/);
	});

	it('rejects an unknown mode', () => {
		expect(() =>
			surrealkitPlugin({ mode: 'watch' as unknown as 'sync' }),
		).toThrow(/unknown mode "watch"/);
	});
});
