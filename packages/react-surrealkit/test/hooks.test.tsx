import { act, cleanup, render, screen } from '@testing-library/react';
import { StrictMode } from 'react';
import { afterEach, describe, expect, it } from 'vitest';

import { preloadQuery } from '../../surrealkit-query/src/index.js';
import { MockDb, settle } from '../../surrealkit-query/test/mock.js';
import {
	type Preloaded,
	SurrealProvider,
	useLive,
	useQuery,
} from '../src/index.js';

const PAGE = 'SELECT * FROM user ORDER BY name LIMIT $lim START $start';
const NAMES = ['ada', 'bob', 'cy', 'dee', 'eve'];

type Row = { id: string; name: string };

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

function Live({
	q,
	params,
}: {
	q: string | Preloaded;
	params?: Record<string, unknown>;
}) {
	const live = useLive(q, params);
	if (live.error) return <p>error: {live.error.code}</p>;
	if (live.loading) return <p>loading</p>;
	return (
		<ul>
			{(live.data as Row[]).map((row) => (
				<li key={row.id}>{row.name}</li>
			))}
		</ul>
	);
}

function OneShot({
	q,
	params,
}: {
	q: string | Preloaded;
	params?: Record<string, unknown>;
}) {
	const query = useQuery(q, params);
	if (query.loading) return <p>loading</p>;
	return <pre>{JSON.stringify(query.data)}</pre>;
}

function names(): string[] {
	return screen
		.queryAllByRole('listitem')
		.map((item) => item.textContent ?? '');
}

async function flush() {
	await act(settle);
}

afterEach(() => cleanup());

describe('useLive', () => {
	it('subscribes on mount and kills the live query on unmount', async () => {
		const db = database();
		const view = render(
			<SurrealProvider client={db}>
				<Live q='SELECT * FROM user' />
			</SurrealProvider>,
		);
		expect(screen.getByText('loading')).toBeTruthy();
		await flush();
		expect(names()).toEqual(NAMES);
		expect(db.open.size).toBe(1);

		await act(async () => {
			db.emit('user', {
				action: 'DELETE',
				recordId: 'user:ada',
				value: {},
			});
		});
		expect(names()).toEqual(['bob', 'cy', 'dee', 'eve']);

		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
		expect(db.killed).toEqual(['live-1']);
	});

	it('keeps one live query through strict mode', async () => {
		const db = database();
		const view = render(
			<StrictMode>
				<SurrealProvider client={db}>
					<Live q='SELECT * FROM user' />
				</SurrealProvider>
			</StrictMode>,
		);
		await flush();
		expect(db.ran(/^LIVE/)).toHaveLength(1);
		expect(db.killed).toEqual([]);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('kills and resubscribes when params change, and not when they are equal', async () => {
		const db = database();
		const tree = (params: Record<string, unknown>) => (
			<SurrealProvider client={db}>
				<Live q={PAGE} params={params} />
			</SurrealProvider>
		);
		const view = render(tree({ lim: 2, start: 0 }));
		await flush();
		expect(names()).toEqual(['ada', 'bob']);

		view.rerender(tree({ start: 0, lim: 2 }));
		await flush();
		expect(db.ran(/^LIVE/)).toHaveLength(1);

		view.rerender(tree({ lim: 2, start: 2 }));
		await flush();
		expect(names()).toEqual(['cy', 'dee']);
		expect(db.killed).toEqual(['live-1']);

		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('shares one live query between components', async () => {
		const db = database();
		const view = render(
			<SurrealProvider client={db}>
				<Live q='SELECT * FROM user' />
				<Live q='SELECT * FROM user' />
			</SurrealProvider>,
		);
		await flush();
		expect(screen.getAllByRole('list')).toHaveLength(2);
		expect(db.ran(/^LIVE/)).toHaveLength(1);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('resubscribes after a reconnect', async () => {
		const db = database();
		render(
			<SurrealProvider client={db}>
				<Live q='SELECT * FROM user' />
			</SurrealProvider>,
		);
		await flush();
		await act(async () => {
			db.reconnect();
			await settle();
		});
		expect([...db.open.keys()]).toEqual(['live-2']);
	});

	it('renders a server payload at once, then goes live', async () => {
		const server = database();
		const payload = await preloadQuery(server, PAGE, { lim: 2, start: 3 });
		const db = database();
		render(
			<SurrealProvider client={db}>
				<Live q={payload} />
			</SurrealProvider>,
		);
		expect(names()).toEqual(['dee', 'eve']);
		await flush();
		expect([...db.open.values()].map((live) => live.params)).toEqual([
			{ lim: 2, start: 3 },
		]);
	});
});

describe('SurrealProvider live', () => {
	it('subscribes with the engine text from the manifest', async () => {
		const db = database();
		const text = "SELECT * FROM user WHERE team = 'red'";
		const view = render(
			<SurrealProvider
				client={db}
				live={{ [text]: { query: `LIVE ${text}`, exit: 'delete' } }}
			>
				<Live q={text} />
			</SurrealProvider>,
		);
		await flush();
		expect(db.ran(/^LIVE/)).toEqual([`LIVE ${text}`]);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});
});

describe('useQuery', () => {
	it('runs again when params change', async () => {
		const db = database();
		const tree = (start: number) => (
			<SurrealProvider client={db}>
				<OneShot q={PAGE} params={{ lim: 1, start }} />
			</SurrealProvider>
		);
		const view = render(tree(0));
		await flush();
		expect(
			screen.getByText('[{"id":"user:ada","name":"ada"}]'),
		).toBeTruthy();
		view.rerender(tree(1));
		await flush();
		expect(
			screen.getByText('[{"id":"user:bob","name":"bob"}]'),
		).toBeTruthy();
	});

	it('renders a server payload without fetching', async () => {
		const payload = await preloadQuery(database(), PAGE, {
			lim: 1,
			start: 4,
		});
		const db = database();
		render(
			<SurrealProvider client={db}>
				<OneShot q={payload} />
			</SurrealProvider>,
		);
		expect(
			screen.getByText('[{"id":"user:eve","name":"eve"}]'),
		).toBeTruthy();
		await flush();
		expect(db.statements).toEqual([]);
	});

	it('throws without a client', () => {
		expect(() => render(<OneShot q='SELECT 1' />)).toThrow(/no client/);
	});
});
