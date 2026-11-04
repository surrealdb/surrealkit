// The cache against a real engine: the one-request LIVE and SELECT, keyed
// and windowed notifications, and an engine restart. Skipped without SURREAL_BIN
// or SURREAL_URL (see engine.ts).

import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { RecordId, Surreal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Observable, QueryCache } from '../src/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from './engine.js';

type Person = { id: RecordId<'person'>; name: string; team: string };

/** Resolves once `test` holds for the observable's data. */
async function until<T>(
	observable: Observable<T>,
	test: (data: T | undefined) => boolean,
	ms = 5000,
): Promise<T | undefined> {
	const deadline = Date.now() + ms;
	while (Date.now() < deadline) {
		const state = observable.get();
		if (state.status === 'error') throw state.error;
		if (state.status === 'success' && test(state.data)) return state.data;
		await new Promise((go) => setTimeout(go, 20));
	}
	throw new Error(
		`timed out; last state ${JSON.stringify(observable.get())}`,
	);
}

const names = (rows: Person[] | undefined) =>
	(rows ?? []).map((row) => row.name);

describe.skipIf(!engineConfigured)(
	`against an engine (${engineSkipReason})`,
	() => {
		let engine: Engine;
		let cache: QueryCache;

		beforeAll(async () => {
			engine = await startEngine();
			cache = new QueryCache(engine.db);
			await engine.db.query(
				"CREATE person:ada SET name = 'ada', team = 'red'; CREATE person:bob SET name = 'bob', team = 'blue';",
			);
		}, 30_000);

		afterAll(async () => {
			cache?.dispose();
			await engine?.stop();
		});

		it('applies notifications by record id', async () => {
			const live = cache.observeLive<Person>('SELECT * FROM person');
			const off = live.subscribe(() => {});
			expect(
				names(await until(live, (rows) => rows?.length === 2)),
			).toEqual(['ada', 'bob']);
			const [ada] = live.get().data ?? [];

			await engine.db.query(
				"CREATE person:cy SET name = 'cy', team = 'red'",
			);
			await until(live, (rows) => rows?.length === 3);
			await engine.db.query("UPDATE $id SET name = 'ada lovelace'", {
				id: new RecordId('person', 'ada'),
			});
			const updated = await until(live, (rows) =>
				names(rows).includes('ada lovelace'),
			);
			expect(updated?.[0]?.id).toBeInstanceOf(RecordId);
			expect(updated?.[0]).not.toBe(ada);
			await engine.db.query('DELETE person:cy');
			expect(
				names(await until(live, (rows) => rows?.length === 2)),
			).toEqual(['ada lovelace', 'bob']);
			off();
			await new Promise((go) => setTimeout(go, 50));
			expect(cache.stats().live).toBe(0);
		});

		it('shares one watcher between window queries on a table', async () => {
			const page = cache.observeLive<Person>(
				'SELECT * FROM person ORDER BY name LIMIT $lim',
				{ lim: 1 },
			);
			const red = cache.observeLive<Person>(
				'SELECT * FROM person WHERE team = $team',
				{ team: 'red' },
			);
			const count = cache.observeLive<{ n: number }>(
				'SELECT count() AS n FROM person GROUP ALL',
			);
			const offs = [page, red, count].map((live) =>
				live.subscribe(() => {}),
			);
			await until(count, (rows) => rows?.length === 1);
			expect(cache.stats().live).toBe(1);
			const before = (count.get().data?.[0]?.n ?? 0) as number;
			await engine.db.query(
				"CREATE person:aa SET name = 'aa', team = 'red'",
			);
			await until(count, (rows) => rows?.[0]?.n === before + 1);
			await until(page, (rows) => rows?.[0]?.name === 'aa');
			await until(red, (rows) => names(rows).includes('aa'));
			await engine.db.query('DELETE person:aa');
			await until(count, (rows) => rows?.[0]?.n === before);
			for (const off of offs) off();
			await new Promise((go) => setTimeout(go, 50));
			expect(cache.stats().live).toBe(0);
		});

		it('drops a record an update moves out of the WHERE', async () => {
			const live = cache.observeLive<Person>(
				'SELECT * FROM person WHERE team = $team',
				{ team: 'blue' },
			);
			const off = live.subscribe(() => {});
			expect(
				names(await until(live, (rows) => rows?.length === 1)),
			).toEqual(['bob']);
			await engine.db.query("UPDATE person:bob SET team = 'green'");
			await until(live, (rows) => rows?.length === 0);
			await engine.db.query("UPDATE person:bob SET team = 'blue'");
			await until(live, (rows) => rows?.length === 1);
			off();
		});

		it('re-runs a window on each change', async () => {
			const live = cache.observeLive<Person>(
				'SELECT * FROM person ORDER BY name LIMIT $lim',
				{ lim: 2 },
			);
			const off = live.subscribe(() => {});
			expect(
				names(await until(live, (rows) => rows?.length === 2)),
			).toEqual(['ada lovelace', 'bob']);
			await engine.db.query(
				"CREATE person:abe SET name = 'abe', team = 'blue'",
			);
			expect(
				names(await until(live, (rows) => rows?.[0]?.name === 'abe')),
			).toEqual(['abe', 'ada lovelace']);
			await engine.db.query('DELETE person:abe');
			await until(live, (rows) => rows?.[1]?.name === 'bob');
			off();
		});
	},
);

describe.skipIf(process.env.SURREAL_BIN === undefined)(
	'across an engine restart (set SURREAL_BIN to run)',
	() => {
		it('re-registers live queries once the SDK reconnects', async () => {
			const bin = process.env.SURREAL_BIN as string;
			const dir = await mkdtemp(path.join(tmpdir(), 'surrealkit-query-'));
			const port = await freePort();
			const start = () =>
				spawn(
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
						`surrealkv://${dir}/db`,
					],
					{ stdio: 'ignore' },
				);
			const stop = async (child: ChildProcess) => {
				const exited = new Promise((go) => child.once('exit', go));
				child.kill();
				await exited;
			};
			let child = start();
			const db = new Surreal();
			try {
				await retry(() =>
					db.connect(`ws://127.0.0.1:${port}/rpc`, {
						authentication: { username: 'root', password: 'root' },
						reconnect: {
							enabled: true,
							attempts: -1,
							retryDelay: 50,
							retryDelayMax: 200,
						},
					}),
				);
				await db.query(
					'DEFINE NAMESPACE t; USE NS t; DEFINE DATABASE t;',
				);
				await db.use({ namespace: 't', database: 't' });
				await db.query(
					"CREATE person:ada SET name = 'ada', team = 'red'",
				);

				const cache = new QueryCache(db);
				const live = cache.observeLive<Person>('SELECT * FROM person');
				const off = live.subscribe(() => {});
				await until(live, (rows) => rows?.length === 1);

				let reconnected = 0;
				db.subscribe('connected', () => {
					reconnected += 1;
				});
				await stop(child);
				child = start();
				await until(
					{
						...live,
						get: () =>
							reconnected > 0
								? live.get()
								: {
										status: 'pending',
										data: undefined,
										error: undefined,
									},
					},
					() => true,
					15_000,
				);
				await retry(() =>
					db.query(
						"CREATE person:bob SET name = 'bob', team = 'blue'",
					),
				);
				expect(
					names(
						await until(live, (rows) => rows?.length === 2, 10_000),
					),
				).toEqual(['ada', 'bob']);
				expect(cache.stats().live).toBe(1);
				off();
				cache.dispose();
			} finally {
				await db.close().catch(() => undefined);
				await stop(child);
				await rm(dir, { recursive: true, force: true });
			}
		}, 40_000);
	},
);

async function retry<T>(run: () => PromiseLike<T>, attempts = 100): Promise<T> {
	let last: unknown;
	for (let i = 0; i < attempts; i += 1) {
		try {
			return await run();
		} catch (error) {
			last = error;
			await new Promise((go) => setTimeout(go, 100));
		}
	}
	throw last;
}

function freePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = createServer();
		server.on('error', reject);
		server.listen(0, '127.0.0.1', () => {
			const address = server.address();
			server.close(() =>
				typeof address === 'object' && address
					? resolve(address.port)
					: reject(new Error('no port')),
			);
		});
	});
}
