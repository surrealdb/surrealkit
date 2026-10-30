#!/usr/bin/env node
/**
 * Disposable-engine round trips of the generated corpus modules, through
 * `surrealkit test --exec`.
 *
 * 1. Copies the golden corpus's portable modules (`schema/*.surql`) into a
 *    throwaway project and compiles it with `surrealkit compile`, which gives
 *    the semanticDigest the database must end up with.
 * 2. Runs `surrealkit test --exec "<vitest> run test/roundtrip.test.ts"`:
 *    SurrealKit creates a disposable database on the server, syncs the
 *    project into it, runs the suite against it with SURREALKIT_EXEC_*
 *    variables, and removes it.
 *
 * The server is either started here (`SURREAL_BIN`, a `surreal` binary, on a
 * free local port with an in-memory store) or given (`SURREALDB_HOST`,
 * `SURREALDB_USER`, `SURREALDB_PASSWORD`, e.g. a CI service container).
 * `SURREALKIT_BIN` names the surrealkit binary (default: the workspace's
 * target/debug/surrealkit). Nothing here touches any other database.
 */
import { spawn, spawnSync } from 'node:child_process';
import {
	copyFileSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const pkg = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const workspace = resolve(pkg, '..', '..');
const corpus = join(
	workspace,
	'crates',
	'surrealkit',
	'tests',
	'fixtures',
	'typegen_corpus',
);
const exe = process.platform === 'win32' ? '.exe' : '';
const surrealkit =
	process.env.SURREALKIT_BIN ??
	join(workspace, 'target', 'debug', `surrealkit${exe}`);

const freePort = () =>
	new Promise((ok, fail) => {
		const server = createServer();
		server.on('error', fail);
		server.listen(0, '127.0.0.1', () => {
			const { port } = server.address();
			server.close(() => ok(port));
		});
	});

const run = (args, options) => {
	const result = spawnSync(surrealkit, args, {
		stdio: 'inherit',
		...options,
	});
	if (result.error) throw result.error;
	return result.status ?? 1;
};

const waitForServer = async (url) => {
	for (let attempt = 0; attempt < 100; attempt++) {
		try {
			const response = await fetch(`${url}/health`);
			if (response.ok) return;
		} catch {
			// not up yet
		}
		await new Promise((r) => setTimeout(r, 100));
	}
	throw new Error(`no SurrealDB at ${url}`);
};

const project = mkdtempSync(join(tmpdir(), 'effect-surrealkit-roundtrip-'));
let server;
let exitCode = 1;
try {
	const schema = join(project, 'database', 'schema');
	mkdirSync(schema, { recursive: true });
	for (const file of readdirSync(join(corpus, 'schema')).sort()) {
		if (file.endsWith('.surql'))
			copyFileSync(join(corpus, 'schema', file), join(schema, file));
	}
	const document = join(project, 'schema.json');
	// The corpus's acknowledged error codes, each with its reason (corpus.json).
	const acknowledge = Object.entries(
		JSON.parse(readFileSync(join(corpus, 'corpus.json'), 'utf8'))
			.acknowledge ?? {},
	)
		.map(
			([code, reason]) =>
				`${JSON.stringify(code)} = ${JSON.stringify(reason)}`,
		)
		.join(', ');
	writeFileSync(
		join(project, 'surrealkit.toml'),
		`[typegen]\nschema_version = 2\njson = ${JSON.stringify(document)}\nacknowledge = { ${acknowledge} }\n`,
	);
	if (
		run(['--folder', join(project, 'database'), 'compile'], {
			cwd: project,
		}) !== 0
	) {
		throw new Error('surrealkit compile failed');
	}

	const env = { ...process.env, SURREALKIT_ROUNDTRIP_DOCUMENT: document };
	if (process.env.SURREAL_BIN !== undefined) {
		const port = await freePort();
		server = spawn(
			process.env.SURREAL_BIN,
			[
				'start',
				'--bind',
				`127.0.0.1:${port}`,
				'--user',
				'root',
				'--pass',
				'secret',
				'--log',
				'error',
				'memory',
			],
			{ stdio: 'ignore' },
		);
		await waitForServer(`http://127.0.0.1:${port}`);
		Object.assign(env, {
			SURREALDB_HOST: `ws://127.0.0.1:${port}`,
			SURREALDB_USER: 'root',
			SURREALDB_PASSWORD: 'secret',
		});
	} else if (process.env.SURREALDB_HOST === undefined) {
		throw new Error(
			'set SURREAL_BIN (a surreal binary) or SURREALDB_HOST (a disposable server)',
		);
	}
	Object.assign(env, {
		SURREALDB_NAMESPACE: env.SURREALDB_NAMESPACE ?? 'roundtrip',
		SURREALDB_NAME: env.SURREALDB_NAME ?? 'roundtrip',
		SURREALDB_AUTH_LEVEL: 'root',
	});
	const vitest = [
		'node',
		join('node_modules', 'vitest', 'vitest.mjs'),
		'run',
		join('test', 'roundtrip.test.ts'),
	];
	exitCode = run(
		[
			'--folder',
			join(project, 'database'),
			'test',
			'--no-seed',
			'--exec',
			vitest.join(' '),
		],
		{ cwd: pkg, env },
	);
} catch (error) {
	console.error(`roundtrip: ${error.message}`);
	exitCode = 1;
} finally {
	if (server !== undefined && server.exitCode === null) {
		const exited = new Promise((r) => server.once('exit', r));
		server.kill();
		await exited;
	}
	rmSync(project, { recursive: true, force: true });
}
process.exit(exitCode);
