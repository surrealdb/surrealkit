import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { preloadQuery } from '../../surrealkit-query/src/index.js';
import { MockDb, settle } from '../../surrealkit-query/test/mock.js';
import { LiveQuery, Query, SurrealProvider, useLive } from '../src/index.js';

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

function List({ rows, stale }: { rows: unknown; stale?: boolean }) {
	return (
		<ul data-stale={String(stale ?? false)}>
			{(rows as Row[]).map((row) => (
				<li key={row.id}>{row.name}</li>
			))}
		</ul>
	);
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

describe('<LiveQuery>', () => {
	it('subscribes on mount and kills the live query on unmount', async () => {
		const db = database();
		const view = render(
			<SurrealProvider client={db}>
				<LiveQuery q='SELECT * FROM user' loading={<p>loading</p>}>
					{(rows) => <List rows={rows} />}
				</LiveQuery>
			</SurrealProvider>,
		);
		expect(screen.getByText('loading')).toBeTruthy();
		await flush();
		expect(names()).toEqual(NAMES);
		view.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('keeps the previous rows, marked stale, while new params load', async () => {
		const db = database();
		const tree = (start: number) => (
			<SurrealProvider client={db}>
				<LiveQuery q={PAGE} params={{ lim: 2, start }}>
					{(rows, { stale }) => <List rows={rows} stale={stale} />}
				</LiveQuery>
			</SurrealProvider>
		);
		const view = render(tree(0));
		await flush();
		db.hold();
		view.rerender(tree(2));
		await flush();
		expect(names()).toEqual(['ada', 'bob']);
		expect(screen.getByRole('list').dataset.stale).toBe('true');
		db.release();
		await flush();
		expect(names()).toEqual(['cy', 'dee']);
		expect(screen.getByRole('list').dataset.stale).toBe('false');
	});

	it('maps rows with select, and renders empty on an empty selection', async () => {
		const db = database();
		const tree = (letter: string) => (
			<SurrealProvider client={db}>
				<LiveQuery
					q='SELECT * FROM user'
					select={(rows) =>
						(rows as Row[]).filter((row) =>
							row.name.startsWith(letter),
						)
					}
					empty={<p>nobody</p>}
				>
					{(rows) => <List rows={rows} />}
				</LiveQuery>
			</SurrealProvider>
		);
		const view = render(tree('e'));
		await flush();
		expect(names()).toEqual(['eve']);
		view.rerender(tree('z'));
		await flush();
		expect(screen.getByText('nobody')).toBeTruthy();
	});

	it('subscribes only while enabled', async () => {
		const db = database();
		const tree = (enabled: boolean) => (
			<SurrealProvider client={db}>
				<LiveQuery q='SELECT * FROM user' enabled={enabled}>
					{(rows) => <List rows={rows} />}
				</LiveQuery>
			</SurrealProvider>
		);
		const view = render(tree(false));
		await flush();
		expect(db.statements).toEqual([]);
		view.rerender(tree(true));
		await flush();
		expect(db.open.size).toBe(1);
		view.rerender(tree(false));
		await flush();
		expect(db.open.size).toBe(0);
		expect(names()).toEqual([]);
	});

	it("shows a useLive result as q, from the hook's one live query", async () => {
		const db = database();
		function Users() {
			const users = useLive('SELECT * FROM user');
			return (
				<>
					<p data-testid='count'>{users.data.length}</p>
					<LiveQuery q={users}>
						{(rows) => <List rows={rows} />}
					</LiveQuery>
				</>
			);
		}
		render(
			<SurrealProvider client={db}>
				<Users />
			</SurrealProvider>,
		);
		await flush();
		expect(screen.getByTestId('count').textContent).toBe('5');
		expect(names()).toEqual(NAMES);
		expect(db.ran(/^LIVE/)).toHaveLength(1);
	});

	it('renders the error prop while the connection is down, and recovers', async () => {
		const db = database();
		render(
			<SurrealProvider client={db}>
				<LiveQuery
					q='SELECT * FROM user'
					error={(failure) => <p>error: {failure.code}</p>}
				>
					{(rows) => <List rows={rows} />}
				</LiveQuery>
			</SurrealProvider>,
		);
		await flush();
		await act(async () => db.disconnect());
		expect(screen.getByText('error: disconnected')).toBeTruthy();
		await act(async () => {
			db.reconnect();
			await settle();
		});
		expect(names()).toEqual(NAMES);
	});

	it('renders the error prop when the engine kills the query', async () => {
		const db = database();
		render(
			<SurrealProvider client={db}>
				<LiveQuery
					q='SELECT * FROM user'
					error={(failure) => <p>error: {failure.code}</p>}
				>
					{(rows) => <List rows={rows} />}
				</LiveQuery>
			</SurrealProvider>,
		);
		await flush();
		await act(async () => {
			db.emit('FROM user', { action: 'KILLED' });
		});
		expect(screen.getByText('error: killed')).toBeTruthy();
	});
});

describe('<Query>', () => {
	it('renders a server payload without fetching', async () => {
		const payload = await preloadQuery(database(), PAGE, {
			lim: 1,
			start: 4,
		});
		const db = database();
		render(
			<SurrealProvider client={db}>
				<Query q={payload}>{(rows) => <List rows={rows} />}</Query>
			</SurrealProvider>,
		);
		expect(names()).toEqual(['eve']);
		await flush();
		expect(db.statements).toEqual([]);
	});

	it('takes the same props as <LiveQuery>', async () => {
		const db = database();
		render(
			<SurrealProvider client={db}>
				<Query
					q={PAGE}
					params={{ lim: 2, start: 0 }}
					select={(rows) => (rows as Row[]).length}
				>
					{(count) => <p>{count} people</p>}
				</Query>
			</SurrealProvider>,
		);
		await flush();
		expect(screen.getByText('2 people')).toBeTruthy();
	});
});
