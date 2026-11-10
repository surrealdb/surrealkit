import { afterEach, describe, expect, it } from 'vitest';
import { defineComponent, h, nextTick, reactive, ref } from 'vue';

import { useLive, useQuery } from '../src/index.js';
import {
	database,
	NAMES,
	names,
	PAGE,
	type Row,
	settle,
	withSurreal,
} from './helpers.js';

const unmounts: (() => void)[] = [];
afterEach(() => {
	for (const unmount of unmounts.splice(0)) unmount();
});

describe('useLive', () => {
	it('subscribes with the component and kills the live query on unmount', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { data } = useLive('SELECT * FROM user');
				return () =>
					h(
						'ul',
						(data.value as Row[]).map((row) => h('li', row.name)),
					);
			}),
		);
		await settle();
		expect(names(wrapper)).toEqual(NAMES);
		expect(db.open.size).toBe(1);
		wrapper.unmount();
		await settle();
		expect(db.open.size).toBe(0);
	});

	it('follows ref params by value, and keeps the previous rows stale', async () => {
		const db = database();
		const start = ref(0);
		const lim = ref(2);
		let stale = ref(false);
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const live = useLive(PAGE, () => ({
					lim: lim.value,
					start: start.value,
				}));
				stale = live.stale as typeof stale;
				return () =>
					h(
						'ul',
						(live.data.value as Row[]).map((row) =>
							h('li', row.name),
						),
					);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(names(wrapper)).toEqual(['ada', 'bob']);

		lim.value = 2;
		await settle();
		expect(db.ran(/^SELECT/)).toHaveLength(1);

		db.hold();
		start.value = 2;
		await settle();
		expect(names(wrapper)).toEqual(['ada', 'bob']);
		expect(stale.value).toBe(true);
		db.release();
		await settle();
		expect(names(wrapper)).toEqual(['cy', 'dee']);
		expect(stale.value).toBe(false);
	});

	it('tracks the properties of a reactive() params object', async () => {
		const db = database();
		const params = reactive({ lim: 1, start: 0 });
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { data } = useLive(PAGE, params);
				return () =>
					h(
						'ul',
						(data.value as Row[]).map((row) => h('li', row.name)),
					);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		params.start = 4;
		await settle();
		expect(names(wrapper)).toEqual(['eve']);
	});

	it('subscribes only while enabled', async () => {
		const db = database();
		const on = ref(false);
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { data } = useLive('SELECT * FROM user', undefined, {
					enabled: on,
				});
				return () => h('p', String((data.value as Row[]).length));
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(db.statements).toEqual([]);
		on.value = true;
		await settle();
		expect(wrapper.text()).toBe('5');
		on.value = false;
		await settle();
		expect(db.open.size).toBe(0);
		expect(wrapper.text()).toBe('0');
	});

	it('maps the rows into selected', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { selected } = useLive('SELECT * FROM user', undefined, {
					select: (rows) =>
						(rows as Row[]).filter((row) => row.name.length === 2),
				});
				return () =>
					h(
						'ul',
						(selected.value as Row[]).map((row) =>
							h('li', row.name),
						),
					);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(names(wrapper)).toEqual(['cy']);
	});

	it('fails with disconnected while the connection is down, and recovers', async () => {
		const db = database();
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { data, error } = useLive('SELECT * FROM user');
				return () =>
					error.value
						? h('p', error.value.code)
						: h(
								'ul',
								(data.value as Row[]).map((row) =>
									h('li', row.name),
								),
							);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		db.disconnect();
		await nextTick();
		expect(wrapper.text()).toBe('disconnected');
		db.reconnect();
		await settle();
		expect(names(wrapper)).toEqual(NAMES);
	});
});

describe('useQuery', () => {
	it('runs once per params, and again when they change', async () => {
		const db = database();
		const start = ref(0);
		const wrapper = withSurreal(
			db,
			defineComponent(() => {
				const { data } = useQuery(PAGE, () => ({
					lim: 1,
					start: start.value,
				}));
				return () =>
					h(
						'ul',
						((data.value ?? []) as Row[]).map((row) =>
							h('li', row.name),
						),
					);
			}),
		);
		unmounts.push(() => wrapper.unmount());
		await settle();
		expect(names(wrapper)).toEqual(['ada']);
		start.value = 1;
		await settle();
		expect(names(wrapper)).toEqual(['bob']);
		expect(db.ran(/^SELECT/)).toHaveLength(2);
		expect(db.open.size).toBe(0);
	});
});
