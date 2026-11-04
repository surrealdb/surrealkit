import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import { afterEach, describe, expect, it } from 'vitest';

import { preloadQuery } from '../../surrealkit-query/src/index.js';
import { MockDb, settle } from '../../surrealkit-query/test/mock.js';
import Live from './harness/Live.svelte';
import OneShot from './harness/OneShot.svelte';
import Runes from './harness/Runes.svelte';

const PAGE = 'SELECT * FROM user ORDER BY name LIMIT $lim START $start';
const NAMES = ['ada', 'bob', 'cy', 'dee', 'eve'];

function database() {
	const db = new MockDb();
	db.select = (_text, params) => {
		const start = (params?.start as number) ?? 0;
		const lim = (params?.lim as number) ?? NAMES.length;
		return NAMES.slice(start, start + lim).map((name) => ({
			id: `user:${name}`,
			name,
		}));
	};
	return db;
}

function names(): string[] {
	return screen
		.queryAllByRole('listitem')
		.map((item) => item.textContent ?? '');
}

afterEach(() => cleanup());

describe('<LiveQuery>', () => {
	it('subscribes on mount and kills the live query on destroy', async () => {
		const db = database();
		const view = render(Live, { db, q: 'SELECT * FROM user' });
		expect(screen.getByText('loading')).toBeTruthy();
		await settle();
		expect(names()).toEqual(NAMES);
		expect(db.open.size).toBe(1);

		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
		expect(db.killed).toEqual(['live-1']);
	});

	it('renders notifications', async () => {
		const db = database();
		render(Live, { db, q: 'SELECT * FROM user' });
		await settle();
		db.emit('user', { action: 'DELETE', recordId: 'user:bob', value: {} });
		db.emit('user', {
			action: 'UPDATE',
			recordId: 'user:ada',
			value: { id: 'user:ada', name: 'ada lovelace' },
		});
		await settle();
		expect(names()).toEqual(['ada lovelace', 'cy', 'dee', 'eve']);
	});

	it('kills and resubscribes when params change, and not when they are equal', async () => {
		const db = database();
		const view = render(Live, {
			db,
			q: PAGE,
			params: { lim: 2, start: 0 },
		});
		await settle();
		expect(names()).toEqual(['ada', 'bob']);

		await view.rerender({ params: { start: 0, lim: 2 } });
		await settle();
		expect(db.ran(/^LIVE/)).toHaveLength(1);

		await view.rerender({ params: { lim: 2, start: 2 } });
		await settle();
		expect(names()).toEqual(['cy', 'dee']);
		expect(db.killed).toEqual(['live-1']);
		expect([...db.open.values()].map((live) => live.params)).toEqual([
			{ lim: 2, start: 2 },
		]);

		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('re-runs the window when a record changes', async () => {
		const db = database();
		render(Live, { db, q: PAGE, params: { lim: 2, start: 0 } });
		await settle();
		NAMES.unshift('abe');
		try {
			db.emit('user', {
				action: 'CREATE',
				recordId: 'user:abe',
				value: { id: 'user:abe', name: 'abe' },
			});
			await settle();
			expect(names()).toEqual(['abe', 'ada']);
		} finally {
			NAMES.shift();
		}
	});

	it('shares one live query between components showing the same query', async () => {
		const db = database();
		const view = render(Live, { db, q: 'SELECT * FROM user', copies: 3 });
		await settle();
		expect(screen.getAllByRole('list')).toHaveLength(3);
		expect(db.ran(/^LIVE/)).toHaveLength(1);
		await view.rerender({ copies: 1 });
		await settle();
		expect(db.open.size).toBe(1);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('resubscribes after a reconnect', async () => {
		const db = database();
		const view = render(Live, { db, q: 'SELECT * FROM user' });
		await settle();
		db.reconnect();
		await settle();
		expect([...db.open.keys()]).toEqual(['live-2']);
		db.emit('user', { action: 'DELETE', recordId: 'user:ada', value: {} });
		await settle();
		expect(names()).toEqual(['bob', 'cy', 'dee', 'eve']);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('renders the error snippet when the engine kills the query', async () => {
		const db = database();
		render(Live, { db, q: 'SELECT * FROM user' });
		await settle();
		db.emit('user', { action: 'KILLED' });
		await settle();
		expect(screen.getByText('error: killed')).toBeTruthy();
	});
});

describe('<Query>', () => {
	it('runs again when params change', async () => {
		const db = database();
		const view = render(OneShot, {
			db,
			q: PAGE,
			params: { lim: 1, start: 0 },
		});
		await settle();
		expect(
			screen.getByText('[{"id":"user:ada","name":"ada"}]'),
		).toBeTruthy();
		await view.rerender({ params: { lim: 1, start: 1 } });
		await settle();
		expect(
			screen.getByText('[{"id":"user:bob","name":"bob"}]'),
		).toBeTruthy();
		expect(db.ran(/^SELECT/)).toHaveLength(2);
	});

	it('renders a server payload without fetching', async () => {
		const server = database();
		const payload = await preloadQuery(server, PAGE, { lim: 1, start: 4 });
		const db = database();
		render(OneShot, { db, q: payload });
		expect(
			screen.getByText('[{"id":"user:eve","name":"eve"}]'),
		).toBeTruthy();
		await settle();
		expect(db.statements).toEqual([]);
	});

	it('renders the error snippet on failure', async () => {
		const db = database();
		db.select = () => {
			throw new Error('no table');
		};
		render(OneShot, { db, q: 'SELECT * FROM nothing' });
		await settle();
		expect(screen.getByText('error: no table')).toBeTruthy();
	});
});

describe('createLive', () => {
	it('follows $state params', async () => {
		const db = database();
		const view = render(Runes, { db });
		await settle();
		expect(names()).toEqual(['ada', 'bob']);

		await fireEvent.click(screen.getByText('same'));
		await settle();
		expect(db.ran(/^LIVE/)).toHaveLength(1);

		await fireEvent.click(screen.getByText('next'));
		await settle();
		expect(names()).toEqual(['cy', 'dee']);
		expect(db.killed).toEqual(['live-1']);

		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});
});
