import { DateTime, RecordId, Table } from 'surrealdb';
import { describe, expect, it, vi } from 'vitest';

import { preloadQuery, QueryCache } from '../src/index.js';
import { MockDb, type Row, settle } from './mock.js';

const USERS = 'SELECT * FROM user';
const PAGE = 'SELECT * FROM user ORDER BY name LIMIT $lim START $start';

function users(...names: string[]): Row[] {
	return names.map((name) => ({ id: `user:${name}`, name }));
}

function setup(rows: Row[] = users('ada', 'bob')) {
	const db = new MockDb();
	db.select = () => rows;
	const cache = new QueryCache(db);
	return { db, cache };
}

describe('one-shot queries', () => {
	it('resolves a single statement to its own result', async () => {
		const { cache } = setup();
		expect(await cache.fetch(USERS)).toEqual(users('ada', 'bob'));
	});

	it('resolves several statements to the tuple', async () => {
		const { db, cache } = setup();
		db.select = (text) => (text.includes('post') ? 1 : 2);
		expect(
			await cache.fetch('SELECT 1 FROM post;\nSELECT 2 FROM user'),
		).toEqual([1, 2]);
	});

	it('runs once for every observer of the same text and params', async () => {
		const { db, cache } = setup();
		const a = cache.observe(USERS);
		const b = cache.observe(USERS, { unused: undefined });
		const offA = a.subscribe(() => {});
		const offB = b.subscribe(() => {});
		await settle();
		expect(db.ran(/^SELECT/)).toHaveLength(1);
		expect(b.get()).toEqual({
			status: 'success',
			data: users('ada', 'bob'),
			error: undefined,
		});
		offA();
		offB();
	});

	it('reports a failure with its query and params', async () => {
		const { db, cache } = setup();
		db.select = () => {
			throw new Error('boom');
		};
		const q = cache.observe(USERS, { a: 1 });
		const off = q.subscribe(() => {});
		await settle();
		const state = q.get();
		expect(state.status).toBe('error');
		expect(state.error).toMatchObject({
			code: 'query',
			message: 'boom',
			query: USERS,
			params: { a: 1 },
		});
		off();
	});
});

describe('live lifecycle', () => {
	it('opens LIVE and SELECT in one request, and kills it when the last observer leaves', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		expect(live.get()).toEqual({
			status: 'pending',
			data: [],
			error: undefined,
		});
		const offA = live.subscribe(() => {});
		const offB = cache.observeLive<Row>(USERS).subscribe(() => {});
		await settle();
		expect(db.statements).toEqual(['LIVE SELECT * FROM user', USERS]);
		expect(db.open.size).toBe(1);
		expect(live.get().data).toEqual(users('ada', 'bob'));

		offA();
		await settle();
		expect(db.open.size).toBe(1);
		offB();
		await settle();
		expect(db.open.size).toBe(0);
		expect(db.killed).toEqual(['live-1']);
		expect(cache.stats()).toMatchObject({ observed: 0, live: 0 });
	});

	it('sends each statement whole when the text ends with a line comment', async () => {
		const { db, cache } = setup();
		const off = cache
			.observeLive<Row>('SELECT * FROM user -- everyone')
			.subscribe(() => {});
		await settle();
		expect(db.statements).toEqual([
			'LIVE SELECT * FROM user -- everyone',
			'SELECT * FROM user -- everyone',
		]);
		off();
	});

	it('keeps the live query across an unsubscribe and subscribe in one tick', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const off = live.subscribe(() => {});
		await settle();
		off();
		const again = live.subscribe(() => {});
		await settle();
		expect(db.killed).toEqual([]);
		expect(db.ran(/^LIVE/)).toHaveLength(1);
		again();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('kills the old live query and opens a new one when params change', async () => {
		const { db, cache } = setup();
		const TEAM = 'SELECT * FROM user FETCH team';
		const first = cache.observeLive<Row>(`${TEAM} -- $a`, { a: 1 });
		const off = first.subscribe(() => {});
		await settle();
		const offSecond = cache
			.observeLive<Row>(TEAM, { a: 2 })
			.subscribe(() => {});
		off();
		await settle();
		expect(db.killed).toEqual(['live-1']);
		expect([...db.open.values()].map((live) => live.params)).toEqual([
			{ a: 2 },
		]);
		offSecond();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('keeps the shared watcher when a window query changes params', async () => {
		const { db, cache } = setup();
		const off = cache
			.observeLive<Row>(PAGE, { lim: 2, start: 0 })
			.subscribe(() => {});
		await settle();
		const offSecond = cache
			.observeLive<Row>(PAGE, { lim: 2, start: 2 })
			.subscribe(() => {});
		off();
		await settle();
		expect(db.ran(/^LIVE/)).toEqual(['LIVE SELECT id FROM user']);
		expect(db.killed).toEqual([]);
		offSecond();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('shares one watcher between window queries on one target', async () => {
		const { db, cache } = setup();
		const texts: [string, object?][] = [
			[PAGE, { lim: 2, start: 0 }],
			['SELECT * FROM user WHERE team = $team', { team: 'red' }],
			['SELECT count() FROM user GROUP ALL'],
		];
		const observed = texts.map(([text, params]) =>
			cache.observeLive<Row>(text, params),
		);
		const offs = observed.map((live) => live.subscribe(() => {}));
		await settle();
		expect(db.ran(/^LIVE/)).toEqual(['LIVE SELECT id FROM user']);
		expect(cache.stats().live).toBe(1);
		const before = db.ran(/^SELECT/).length;

		db.emit('FROM user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: { id: 'user:cy', name: 'cy' },
		});
		await settle();
		expect(db.ran(/^SELECT/).length - before).toBe(3);

		offs[0]?.();
		offs[1]?.();
		await settle();
		expect(db.open.size).toBe(1);
		offs[2]?.();
		await settle();
		expect(db.open.size).toBe(0);
		expect(cache.stats().live).toBe(0);
	});

	it('keeps separate watchers for watch texts that read different params', async () => {
		const { db, cache } = setup();
		const text = 'SELECT * FROM user WHERE team = $team ORDER BY name';
		const live = {
			[text]: {
				query: 'LIVE SELECT * FROM user WHERE team = $team',
				exit: 'delete' as const,
				refusal: { clauses: ['ORDER BY'], windowable: true },
			},
		};
		const shared = new QueryCache(db, { live });
		const offs = ['red', 'blue', 'red'].map((team) =>
			shared.observeLive<Row>(text, { team }).subscribe(() => {}),
		);
		await settle();
		expect([...db.open.values()].map((open) => open.params)).toEqual([
			{ team: 'red' },
			{ team: 'blue' },
		]);
		for (const off of offs) off();
		await settle();
		expect(db.open.size).toBe(0);
		void cache;
	});

	it('kills a live query that resolves after its observer left', async () => {
		const { db, cache } = setup();
		db.hold();
		const off = cache.observeLive<Row>(USERS).subscribe(() => {});
		await settle();
		off();
		await settle();
		db.release();
		await settle();
		expect(db.ran(/^LIVE/)).toHaveLength(1);
		expect(db.open.size).toBe(0);
		expect(cache.stats().live).toBe(0);
	});

	it('re-registers and re-reads observed live queries after a reconnect', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const off = live.subscribe(() => {});
		const idle = cache.observeLive<Row>('SELECT * FROM post');
		idle.subscribe(() => {})();
		await settle();
		db.select = () => users('ada', 'bob', 'cy');
		db.reconnect();
		await settle();
		expect(db.ran(/^LIVE/)).toEqual([
			'LIVE SELECT * FROM user',
			'LIVE SELECT * FROM post',
			'LIVE SELECT * FROM user',
		]);
		expect([...db.open.keys()]).toEqual(['live-3']);
		expect(live.get().data).toEqual(users('ada', 'bob', 'cy'));
		db.emit('FROM user', {
			action: 'DELETE',
			recordId: 'user:cy',
			value: {},
		});
		expect(live.get().data).toEqual(users('ada', 'bob'));
		off();
		await settle();
		expect(db.open.size).toBe(0);
		expect(cache.stats().live).toBe(0);
	});

	it('fails with disconnected, keeping the rows, until the connection returns', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const once = cache.observe(USERS);
		const offs = [live.subscribe(() => {}), once.subscribe(() => {})];
		await settle();
		db.disconnect();
		expect(live.get()).toMatchObject({
			status: 'error',
			data: users('ada', 'bob'),
			error: { code: 'disconnected' },
		});
		expect(once.get()).toMatchObject({
			status: 'error',
			error: { code: 'disconnected' },
		});
		expect(cache.stats().live).toBe(0);

		// The SDK announces every retry; observers hear of the drop once.
		let heard = 0;
		const offHeard = live.subscribe(() => {
			heard += 1;
		});
		const before = live.get();
		db.disconnect();
		db.disconnect();
		expect(heard).toBe(0);
		expect(live.get()).toBe(before);
		offHeard();

		db.select = () => users('ada', 'bob', 'cy');
		db.reconnect();
		await settle();
		expect(live.get()).toEqual({
			status: 'success',
			data: users('ada', 'bob', 'cy'),
			error: undefined,
		});
		expect(once.get().status).toBe('success');
		expect(db.open.size).toBe(1);
		for (const off of offs) off();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('restarts one-shot queries after a reconnect', async () => {
		const { db, cache } = setup();
		const off = cache.observe(USERS).subscribe(() => {});
		await settle();
		db.reconnect();
		await settle();
		expect(db.ran(/^SELECT/)).toHaveLength(2);
		off();
	});

	it('surfaces a KILLED notification as an error', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const off = live.subscribe(() => {});
		await settle();
		db.emit('FROM user', { action: 'KILLED' });
		expect(live.get()).toMatchObject({
			status: 'error',
			error: { code: 'killed' },
		});
		expect(live.get().data).toEqual(users('ada', 'bob'));
		off();
		await settle();
		expect(cache.stats().live).toBe(0);
	});

	it('refuses text that cannot be live without running anything', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive('SELECT * FROM ONLY user:ada');
		const off = live.subscribe(() => {});
		await settle();
		expect(live.get()).toMatchObject({
			status: 'error',
			error: {
				code: 'unsupported',
				message: 'a live query cannot select FROM ONLY',
			},
		});
		expect(db.statements).toEqual([]);
		off();
	});

	it('drops unobserved data after gcTime', async () => {
		vi.useFakeTimers();
		try {
			const db = new MockDb();
			const cache = new QueryCache(db, { gcTime: 1000 });
			cache.observeLive(USERS).subscribe(() => {})();
			await vi.advanceTimersByTimeAsync(0);
			expect(cache.stats().entries).toBe(1);
			await vi.advanceTimersByTimeAsync(1000);
			expect(cache.stats().entries).toBe(0);
		} finally {
			vi.useRealTimers();
		}
	});
});

describe('reconciling notifications', () => {
	it('applies CREATE, UPDATE and DELETE by record id, keeping unchanged rows', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const seen: unknown[] = [];
		const off = live.subscribe(() => seen.push(live.get().data));
		await settle();
		const [ada] = live.get().data as Row[];
		db.emit('FROM user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: { id: 'user:cy', name: 'cy' },
		});
		db.emit('FROM user', {
			action: 'UPDATE',
			recordId: 'user:bob',
			value: { id: 'user:bob', name: 'robert' },
		});
		db.emit('FROM user', {
			action: 'DELETE',
			recordId: 'user:cy',
			value: { id: 'user:cy', name: 'cy' },
		});
		const rows = live.get().data as Row[];
		expect(rows).toEqual([
			{ id: 'user:ada', name: 'ada' },
			{ id: 'user:bob', name: 'robert' },
		]);
		expect(rows[0]).toBe(ada);
		expect(seen).toHaveLength(4);
		off();
	});

	it('applies notifications that arrived before it subscribed', async () => {
		const { db, cache } = setup();
		const live = cache.observeLive<Row>(USERS);
		const off = live.subscribe(() => {});
		// The LIVE statement has registered but the subscription is not yet attached.
		await Promise.resolve();
		db.emit('FROM user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: { id: 'user:cy', name: 'cy' },
		});
		await settle();
		expect(live.get().data).toEqual(users('ada', 'bob', 'cy'));
		off();
	});

	it('re-reads the snapshot when rows carry no id', async () => {
		const { db, cache } = setup();
		db.select = () => [{ name: 'ada' }];
		const live = cache.observeLive('SELECT name FROM user');
		const off = live.subscribe(() => {});
		await settle();
		db.select = () => [{ name: 'ada' }, { name: 'cy' }];
		db.emit('user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: { name: 'cy' },
		});
		await settle();
		expect(live.get().data).toEqual([{ name: 'ada' }, { name: 'cy' }]);
		expect(db.ran(/^SELECT/)).toHaveLength(2);
		off();
	});

	it('re-reads a VALUE query on each change to its source', async () => {
		const { db, cache } = setup();
		db.select = () => ['ada'];
		const live = cache.observeLive('SELECT VALUE name FROM user');
		const off = live.subscribe(() => {});
		await settle();
		expect(db.ran(/^LIVE/)).toEqual(['LIVE SELECT id FROM user']);
		db.select = () => ['ada', 'cy'];
		db.emit('FROM user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: 'cy',
		});
		await settle();
		expect(live.get().data).toEqual(['ada', 'cy']);
		off();
	});
});

describe('windowed live queries', () => {
	it('watches the table and re-runs the window on each change', async () => {
		const { db, cache } = setup();
		db.select = (_text, params) =>
			users('ada', 'bob', 'cy', 'dee').slice(
				params?.start as number,
				(params?.start as number) + (params?.lim as number),
			);
		const live = cache.observeLive<Row>(PAGE, { lim: 2, start: 1 });
		const off = live.subscribe(() => {});
		await settle();
		expect(db.ran(/^LIVE/)).toEqual(['LIVE SELECT id FROM user']);
		expect(live.get().data).toEqual(users('bob', 'cy'));
		const [bob] = live.get().data as Row[];

		db.select = () => [
			{ id: 'user:bob', name: 'bob' },
			{ id: 'user:abe', name: 'abe' },
		];
		db.emit('FROM user', {
			action: 'CREATE',
			recordId: 'user:abe',
			value: { id: 'user:abe', name: 'abe' },
		});
		await settle();
		expect(live.get().data).toEqual([
			{ id: 'user:bob', name: 'bob' },
			{ id: 'user:abe', name: 'abe' },
		]);
		expect((live.get().data as Row[])[0]).toBe(bob);
		off();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('keys a param source by its table value, apart from a string', async () => {
		const { db, cache } = setup();
		const text = 'SELECT * FROM $t WHERE active';
		const offs = [
			{ t: new Table('user') },
			{ t: new Table('user') },
			{ t: new Table('post') },
			{ t: 'user' },
		].map((params) =>
			cache.observeLive<Row>(text, params).subscribe(() => {}),
		);
		await settle();
		expect(db.ran(/^LIVE/)).toEqual([
			'LIVE SELECT id FROM $t',
			'LIVE SELECT id FROM $t',
			'LIVE SELECT id FROM $t',
		]);
		expect(cache.stats().live).toBe(3);
		for (const off of offs) off();
	});

	it('coalesces a burst of notifications into one re-run plus one follow-up', async () => {
		const { db, cache } = setup();
		const off = cache
			.observeLive<Row>(PAGE, { lim: 2, start: 0 })
			.subscribe(() => {});
		await settle();
		db.hold();
		for (let i = 0; i < 5; i += 1) {
			db.emit('FROM user', {
				action: 'UPDATE',
				recordId: 'user:ada',
				value: { id: 'user:ada', name: `${i}` },
			});
		}
		await settle();
		db.release();
		await settle();
		expect(db.ran(/^SELECT/)).toHaveLength(3);
		off();
	});
});

describe('server payloads', () => {
	it('round-trips SDK values in data and params', async () => {
		const db = new MockDb();
		const at = new DateTime('2026-01-02T03:04:05Z');
		db.select = () => [{ id: new RecordId('user', 'ada'), at }];
		const payload = await preloadQuery(db, USERS, {
			team: new RecordId('team', 'red'),
		});
		expect(JSON.parse(JSON.stringify(payload))).toEqual(payload);

		const browser = new MockDb();
		const cache = new QueryCache(browser);
		cache.hydrate(payload);
		const query = cache.observe<Array<{ id: RecordId; at: DateTime }>>(
			USERS,
			{
				team: new RecordId('team', 'red'),
			},
		);
		const state = query.get();
		expect(state.status).toBe('success');
		const [row] = state.data ?? [];
		expect(row?.id).toBeInstanceOf(RecordId);
		expect(row?.id.toString()).toBe('user:ada');
		expect(row?.at).toBeInstanceOf(DateTime);
		expect(String(row?.at)).toBe(String(at));
	});

	it('does not refetch a hydrated one-shot query on its first observer', async () => {
		const server = new MockDb();
		server.select = () => users('ada');
		const payload = await preloadQuery(server, USERS);
		const { db, cache } = setup();
		cache.hydrate(payload);
		const off = cache.observe(USERS).subscribe(() => {});
		await settle();
		expect(db.statements).toEqual([]);
		off();
	});

	it('renders a hydrated live query at once, then opens it and re-reads', async () => {
		const server = new MockDb();
		server.select = () => users('ada');
		const payload = await preloadQuery(server, USERS);
		const { db, cache } = setup(users('ada', 'bob'));
		cache.hydrate(payload);
		const live = cache.observeLive<Row>(USERS);
		expect(live.get()).toEqual({
			status: 'success',
			data: users('ada'),
			error: undefined,
		});
		const off = live.subscribe(() => {});
		await settle();
		expect(db.open.size).toBe(1);
		expect(live.get().data).toEqual(users('ada', 'bob'));
		off();
	});
});

describe('invalidate and reset', () => {
	it('re-runs observed matches and drops unobserved ones', async () => {
		const { db, cache } = setup();
		const off = cache.observe(USERS, { a: 1 }).subscribe(() => {});
		cache.observe(USERS, { a: 2 }).subscribe(() => {})();
		await settle();
		await cache.invalidate(USERS);
		expect(db.ran(/^SELECT/)).toHaveLength(3);
		expect(cache.stats().entries).toBe(1);
		off();
	});

	it('restarts observed live queries under the new identity', async () => {
		const { db, cache } = setup();
		const off = cache.observeLive<Row>(USERS).subscribe(() => {});
		await settle();
		await cache.reset();
		await settle();
		expect(db.killed).toEqual(['live-1']);
		expect([...db.open.keys()]).toEqual(['live-2']);
		off();
		await settle();
		expect(db.open.size).toBe(0);
	});
});

describe('no leaked live queries', () => {
	it('leaves nothing open after any interleaving of observers, params and reconnects', async () => {
		// mulberry32: a seeded generator, so a failure replays.
		let seed = 7;
		const random = (n: number) => {
			seed = (seed + 0x6d2b79f5) | 0;
			let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
			t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
			return ((t ^ (t >>> 14)) >>> 0) % n;
		};
		const { db, cache } = setup();
		const offs: (() => void)[] = [];
		for (let step = 0; step < 400; step += 1) {
			const roll = random(10);
			if (roll < 4) {
				const texts = [
					PAGE,
					USERS,
					'SELECT * FROM post',
					'SELECT * FROM post LIMIT $lim',
				];
				const text = texts[random(texts.length)] as string;
				offs.push(
					cache
						.observeLive<Row>(text, { lim: 2, start: random(3) })
						.subscribe(() => {}),
				);
			} else if (roll < 7 && offs.length > 0) {
				offs.splice(random(offs.length), 1)[0]?.();
			} else if (roll === 7) {
				db.reconnect();
			} else if (roll === 8) {
				db.hold();
			} else {
				db.release();
			}
			if (random(3) === 0) await settle();
		}
		db.release();
		for (const off of offs) off();
		await settle();
		expect(db.ran(/^LIVE/).length).toBeGreaterThan(80);
		expect(db.killed.length).toBeGreaterThan(30);
		expect(db.open.size).toBe(0);
		expect(cache.stats()).toMatchObject({ observed: 0, live: 0 });
	});
});

describe('an engine live manifest', () => {
	it('subscribes with the engine text and applies notifications by id', async () => {
		const db = new MockDb();
		db.select = () => users('ada');
		const text = "SELECT * FROM user WHERE team = 'red'";
		const cache = new QueryCache(db, {
			live: { [text]: { query: `LIVE ${text}`, exit: 'delete' } },
		});
		const live = cache.observeLive<Row>(text);
		const off = live.subscribe(() => {});
		await settle();
		expect(db.ran(/^LIVE/)).toEqual([`LIVE ${text}`]);
		db.emit('user', {
			action: 'CREATE',
			recordId: 'user:cy',
			value: { id: 'user:cy', name: 'cy' },
		});
		expect(live.get().data).toEqual(users('ada', 'cy'));
		expect(db.ran(/^SELECT/)).toHaveLength(1);
		off();
	});

	it('shares one watcher between window queries the engine describes differently', async () => {
		const db = new MockDb();
		const page =
			'SELECT id, name FROM user ORDER BY name LIMIT $lim START $start';
		const team = 'SELECT id, name FROM user WHERE team = $team';
		const count = 'SELECT count() AS total FROM user GROUP ALL';
		const cache = new QueryCache(db, {
			live: {
				[page]: {
					query: 'LIVE SELECT id, name FROM user',
					refusal: {
						clauses: ['ORDER BY', 'LIMIT', 'START'],
						windowable: true,
					},
				},
				[team]: {
					query: `LIVE ${team}`,
					exit: 'silent',
					watch: 'LIVE SELECT id FROM user',
				},
				[count]: {
					query: 'LIVE SELECT count() AS total FROM user',
					refusal: { clauses: ['GROUP BY'], windowable: true },
				},
			},
		});
		const offs = [
			cache.observeLive(page, { lim: 2, start: 0 }),
			cache.observeLive(team, { team: 'red' }),
			cache.observeLive(count),
		].map((live) => live.subscribe(() => {}));
		await settle();
		expect(db.ran(/^LIVE/)).toEqual(['LIVE SELECT id FROM user']);
		for (const off of offs) off();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('is added by cacheFor after the cache exists', async () => {
		const { cacheFor } = await import('../src/index.js');
		const db = new MockDb();
		const text = 'SELECT * FROM user';
		cacheFor(db);
		cacheFor(db, {
			live: {
				[text]: { refusal: { clauses: ['ONLY'], windowable: false } },
			},
		});
		const live = cacheFor(db).observeLive(text);
		const off = live.subscribe(() => {});
		await settle();
		expect(live.get()).toMatchObject({
			status: 'error',
			error: { code: 'unsupported' },
		});
		off();
	});
});
