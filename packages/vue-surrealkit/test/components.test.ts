import { afterEach, describe, expect, it } from 'vitest';
import { type Component, defineComponent, h, ref } from 'vue';

import { preloadQuery } from '../../surrealkit-query/src/index.js';
import { LiveQuery as Live, Query as Once, useLive } from '../src/index.js';
import {
	database,
	NAMES,
	names,
	PAGE,
	type Row,
	settle,
	withSurreal,
} from './helpers.js';

// The components' public types follow their `q`; these tests drive them
// untyped, through render functions.
const LiveQuery = Live as unknown as Component;
const Query = Once as unknown as Component;

type Scope = { data: unknown; stale: boolean; error?: { code: string } };
const list = ({ data, stale }: Scope) =>
	h(
		'ul',
		{ 'data-stale': String(stale) },
		(data as Row[]).map((row) => h('li', row.name)),
	);

const unmounts: (() => void)[] = [];
afterEach(() => {
	for (const unmount of unmounts.splice(0)) unmount();
});

describe('<LiveQuery>', () => {
	it('renders loading, then the rows, and kills the live query on unmount', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						LiveQuery,
						{ q: 'SELECT * FROM user' },
						{ default: list, loading: () => 'loading' },
					),
			),
		);
		expect(wrapper.text()).toBe('loading');
		await settle();
		expect(names(wrapper)).toEqual(NAMES);
		db.emit('FROM user', {
			action: 'DELETE',
			recordId: 'user:ada',
			value: {},
		});
		await settle();
		expect(names(wrapper)).toEqual(['bob', 'cy', 'dee', 'eve']);
		wrapper.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('keeps the previous rows, marked stale, while new params load', async () => {
		const db = database();
		const start = ref(0);
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						LiveQuery,
						{ q: PAGE, params: { lim: 2, start: start.value } },
						{ default: list },
					),
			),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		db.hold();
		start.value = 2;
		await settle();
		expect(names(wrapper)).toEqual(['ada', 'bob']);
		expect(wrapper.find('ul').attributes('data-stale')).toBe('true');
		db.release();
		await settle();
		expect(names(wrapper)).toEqual(['cy', 'dee']);
		expect(wrapper.find('ul').attributes('data-stale')).toBe('false');
	});

	it('maps with select, renders empty on an empty selection, and emits both models', async () => {
		const db = database();
		const letter = ref('e');
		const data = ref<unknown>();
		const selected = ref<unknown>();
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						LiveQuery,
						{
							q: 'SELECT * FROM user',
							select: (rows: Row[]) =>
								rows.filter((row) =>
									row.name.startsWith(letter.value),
								),
							'onUpdate:data': (value: unknown) => {
								data.value = value;
							},
							'onUpdate:selected': (value: unknown) => {
								selected.value = value;
							},
						},
						{ default: list, empty: () => 'nobody' },
					),
			),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(names(wrapper)).toEqual(['eve']);
		expect((data.value as Row[]).length).toBe(5);
		expect((selected.value as Row[]).map((row) => row.name)).toEqual([
			'eve',
		]);
		letter.value = 'z';
		await settle();
		expect(wrapper.text()).toBe('nobody');
		expect(selected.value).toEqual([]);
	});

	it('renders nothing and subscribes to nothing while not enabled', async () => {
		const db = database();
		const enabled = ref(false);
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						LiveQuery,
						{ q: 'SELECT * FROM user', enabled: enabled.value },
						{ default: list, loading: () => 'loading' },
					),
			),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(wrapper.text()).toBe('');
		expect(db.statements).toEqual([]);
		enabled.value = true;
		await settle();
		expect(names(wrapper)).toEqual(NAMES);
		enabled.value = false;
		await settle();
		expect(db.open.size).toBe(0);
	});

	it("shows a useLive result as q, from the composable's one live query", async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const users = useLive('SELECT * FROM user');
				return () =>
					h('div', [
						h(
							'p',
							{ class: 'count' },
							String(users.data.value.length),
						),
						h(LiveQuery, { q: users }, { default: list }),
					]);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(wrapper.find('.count').text()).toBe('5');
		expect(names(wrapper)).toEqual(NAMES);
		expect(db.ran(/^LIVE/)).toHaveLength(1);
	});

	it("shows a composable's own selection when it is given as q", async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const short = useLive('SELECT * FROM user', undefined, {
					select: (rows) =>
						(rows as Row[]).filter((row) => row.name.length === 2),
				});
				return () =>
					h(
						LiveQuery,
						{ q: short },
						{ default: list, empty: () => 'none' },
					);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(names(wrapper)).toEqual(['cy']);
	});

	it('renders the error slot when the engine kills the query or the connection drops', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						LiveQuery,
						{ q: 'SELECT * FROM user' },
						{
							default: list,
							error: ({ error }: { error: { code: string } }) =>
								`error: ${error.code}`,
						},
					),
			),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		db.disconnect();
		await settle();
		expect(wrapper.text()).toBe('error: disconnected');
		db.reconnect();
		await settle();
		expect(names(wrapper)).toEqual(NAMES);
		db.emit('FROM user', { action: 'KILLED' });
		await settle();
		expect(wrapper.text()).toBe('error: killed');
	});
});

describe('<Query>', () => {
	it('renders a server payload without fetching', async () => {
		const payload = await preloadQuery(database(), PAGE, {
			lim: 1,
			start: 4,
		});
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () => h(Query, { q: payload }, { default: list }),
			),
		);
		unmounts.push(() => wrapper.unmount());
		expect(names(wrapper)).toEqual(['eve']);
		await settle();
		expect(db.statements).toEqual([]);
	});

	it('takes the same props as <LiveQuery>', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(
				() => () =>
					h(
						Query,
						{
							q: PAGE,
							params: { lim: 2, start: 0 },
							select: (rows: Row[]) => rows.length,
						},
						{ default: ({ data }: Scope) => `${data} people` },
					),
			),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(wrapper.text()).toBe('2 people');
	});
});
