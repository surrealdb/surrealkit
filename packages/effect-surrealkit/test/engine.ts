/**
 * A disposable SurrealDB engine for the engine suites.
 *
 * - `SURREAL_URL` (e.g. `ws://127.0.0.1:8000/rpc`): use an engine someone
 *   else started (a throwaway container); root/root credentials.
 * - `SURREAL_BIN` (e.g. `/usr/local/bin/surreal`): start
 *   `surreal start --bind 127.0.0.1:<free port> memory` and stop it after.
 * - With neither set: the embedded engine of `@surrealdb/node`, in process and
 *   in memory, so the engine suites run on a plain `npm test`.
 *
 * The suites never touch a shared database: every run uses a fresh
 * namespace. The embedded engine has no HTTP endpoint, so the suites that
 * read SurrealDB's JSON over `/sql` need one of the two variables and say so
 * when they are skipped.
 */
import { type ChildProcess, spawn } from 'node:child_process';
import { createServer } from 'node:net';

import { type ConnectOptions, Surreal } from 'surrealdb';

/** How the engine is provided. */
export type EngineMode = 'embedded' | 'binary' | 'remote';

export const engineMode: EngineMode =
	process.env.SURREAL_URL !== undefined
		? 'remote'
		: process.env.SURREAL_BIN !== undefined
			? 'binary'
			: 'embedded';

const embeddedEngine = async (): Promise<Surreal> => {
	const { createNodeEngines } = await import('@surrealdb/node');
	return new Surreal({ engines: createNodeEngines() });
};

/**
 * Why the embedded engine cannot run here (no native build for the platform
 * or the Node release), or `undefined` when it starts.
 */
const embeddedFailure = async (): Promise<string | undefined> => {
	try {
		const db = await embeddedEngine();
		try {
			await db.connect('mem://');
			await db.version();
		} finally {
			await db.close();
		}
		return undefined;
	} catch (error) {
		return error instanceof Error ? error.message : String(error);
	}
};

const unavailable =
	engineMode === 'embedded' ? await embeddedFailure() : undefined;

/** False only when no engine can be started; the engine suites then skip. */
export const engineConfigured = unavailable === undefined;

export const engineSkipReason = `the embedded engine did not start (${unavailable}); set SURREAL_BIN (a surreal binary) or SURREAL_URL (a disposable engine) to run the engine suites`;

/** Whether the engine serves HTTP `/sql`, which the embedded engine does not. */
export const httpConfigured = engineMode !== 'embedded';

export const httpSkipReason =
	'the embedded engine has no HTTP /sql endpoint; set SURREAL_BIN (a surreal binary) or SURREAL_URL (a disposable engine) to run the JSON round trips';

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
	readonly mode: EngineMode;
	/** As the engine reports it, e.g. `surrealdb-4.0.1`. */
	readonly version: string;
	readonly majorVersion: number;
	/** The fresh namespace of this run; its database has the same name. */
	readonly namespace: string;
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

type EngineOptions = Pick<ConnectOptions, 'retry'>;

const connect = async (
	url: string,
	namespace: string,
	options: EngineOptions,
): Promise<Surreal> => {
	const db = new Surreal();
	await db.connect(url, {
		...options,
		authentication: { username: 'root', password: 'root' },
	});
	await db.query(
		`DEFINE NAMESPACE ${namespace}; USE NS ${namespace}; DEFINE DATABASE ${namespace};`,
	);
	await db.use({ namespace, database: namespace });
	return db;
};

const engineVersion = async (db: Surreal) => {
	const { version } = await db.version();
	const major = /^(?:surrealdb-)?(\d+)\./u.exec(version)?.[1];
	if (major === undefined) {
		throw new Error(
			`could not determine SurrealDB major version: ${version}`,
		);
	}
	return { version, majorVersion: Number(major) };
};

const startEmbedded = async (
	namespace: string,
	options: EngineOptions,
): Promise<Engine> => {
	const db = await embeddedEngine();
	try {
		await db.connect('mem://', {
			...options,
			namespace,
			database: namespace,
		});
		return {
			db,
			mode: 'embedded',
			namespace,
			...(await engineVersion(db)),
			http: async () => {
				throw new Error(httpSkipReason);
			},
			stop: async () => {
				await db.close();
			},
		};
	} catch (error) {
		await db.close();
		throw error;
	}
};

export const startEngine = async (
	options: EngineOptions = {},
): Promise<Engine> => {
	const namespace = `effect_surrealkit_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
	if (engineMode === 'embedded') return startEmbedded(namespace, options);

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
			db = await connect(url, namespace, options);
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
	const connected = db;
	return {
		db: connected,
		mode: engineMode,
		namespace,
		...(await engineVersion(connected)),
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
