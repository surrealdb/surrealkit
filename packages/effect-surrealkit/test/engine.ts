/**
 * A disposable SurrealDB engine for the engine suites.
 *
 * - `SURREAL_URL` (e.g. `ws://127.0.0.1:8000/rpc`): use an engine someone
 *   else started (a throwaway container); root/root credentials.
 * - `SURREAL_BIN` (e.g. `/usr/local/bin/surreal`): start
 *   `surreal start --bind 127.0.0.1:<free port> memory` and stop it after.
 *
 * Without either, the engine suites are skipped and say so. They never touch
 * a shared database: every run uses a fresh in-memory namespace.
 */
import { type ChildProcess, spawn } from 'node:child_process';
import { createServer } from 'node:net';

import { Surreal } from 'surrealdb';

export const engineConfigured =
	process.env.SURREAL_URL !== undefined ||
	process.env.SURREAL_BIN !== undefined;

export const engineSkipReason =
	'set SURREAL_BIN (a surreal binary) or SURREAL_URL (a disposable engine) to run the engine round-trip suite';

const freePort = (): Promise<number> =>
	new Promise((resolve, reject) => {
		const server = createServer();
		server.unref();
		server.on('error', reject);
		server.listen(0, '127.0.0.1', () => {
			const address = server.address();
			server.close(() => {
				if (address === null || typeof address === 'string') {
					reject(new Error('no port'));
				} else {
					resolve(address.port);
				}
			});
		});
	});

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface Engine {
	readonly db: Surreal;
	readonly version: string;
	/**
	 * Runs SurrealQL over the engine's HTTP `/sql` endpoint, in the same
	 * namespace and database, and returns the last statement's JSON result.
	 */
	readonly http: (sql: string) => Promise<unknown>;
	readonly stop: () => Promise<void>;
}

const httpSql =
	(rpc: string, namespace: string) =>
	async (sql: string): Promise<unknown> => {
		const url = new URL(rpc);
		url.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';
		url.pathname = '/sql';
		const response = await fetch(url, {
			method: 'POST',
			headers: {
				Accept: 'application/json',
				Authorization: `Basic ${btoa('root:root')}`,
				'surreal-ns': namespace,
				'surreal-db': namespace,
			},
			body: sql,
		});
		const statements = (await response.json()) as Array<{
			readonly status: string;
			readonly result: unknown;
		}>;
		const last = statements[statements.length - 1];
		if (!Array.isArray(statements) || last?.status !== 'OK') {
			throw new Error(`HTTP /sql failed: ${JSON.stringify(statements)}`);
		}
		return last.result;
	};

const connect = async (url: string, namespace: string): Promise<Surreal> => {
	const db = new Surreal();
	await db.connect(url, {
		authentication: { username: 'root', password: 'root' },
	});
	await db.query(
		`DEFINE NAMESPACE ${namespace}; USE NS ${namespace}; DEFINE DATABASE ${namespace};`,
	);
	await db.use({ namespace, database: namespace });
	return db;
};

export const startEngine = async (): Promise<Engine> => {
	const namespace = `effect_surrealkit_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
	let child: ChildProcess | undefined;
	let url = process.env.SURREAL_URL;
	if (url === undefined) {
		const bin = process.env.SURREAL_BIN;
		if (bin === undefined) throw new Error(engineSkipReason);
		const port = await freePort();
		child = spawn(
			bin,
			[
				'start',
				'--bind',
				`127.0.0.1:${port}`,
				'--user',
				'root',
				'--pass',
				'root',
				'--log',
				'error',
				'memory',
			],
			{ stdio: 'ignore' },
		);
		url = `ws://127.0.0.1:${port}/rpc`;
	}
	let db: Surreal | undefined;
	let lastError: unknown;
	for (let attempt = 0; attempt < 100 && db === undefined; attempt++) {
		try {
			db = await connect(url, namespace);
		} catch (error) {
			lastError = error;
			await delay(100);
		}
	}
	if (db === undefined) {
		child?.kill();
		throw new Error(
			`could not reach the disposable engine at ${url}: ${String(lastError)}`,
		);
	}
	const version = await db.version();
	const connected = db;
	return {
		db: connected,
		version: version.version,
		http: httpSql(url, namespace),
		stop: async () => {
			await connected
				.query(`REMOVE NAMESPACE IF EXISTS ${namespace}`)
				.catch(() => undefined);
			await connected.close();
			if (child !== undefined && child.exitCode === null) {
				const exited = new Promise((resolve) =>
					child?.once('exit', resolve),
				);
				child.kill();
				await exited;
			}
		},
	};
};

/** Fails a promise that does not settle, instead of hanging the suite. */
export const within = <A>(promise: Promise<A>, ms = 5000): Promise<A> =>
	Promise.race([
		promise,
		new Promise<never>((_, reject) =>
			setTimeout(() => reject(new Error(`no answer within ${ms}ms`)), ms),
		),
	]);
