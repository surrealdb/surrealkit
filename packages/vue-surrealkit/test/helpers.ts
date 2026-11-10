import { mount } from '@vue/test-utils';
import { type Component, defineComponent, h } from 'vue';

import { MockDb, settle } from '../../surrealkit-query/test/mock.js';
import { provideSurreal } from '../src/index.js';

export { MockDb, settle };

export const NAMES = ['ada', 'bob', 'cy', 'dee', 'eve'];
export const PAGE = 'SELECT * FROM user ORDER BY name LIMIT $lim START $start';
export type Row = { id: string; name: string };

/** A mock engine whose SELECTs page through NAMES by `$lim` and `$start`. */
export function database(): MockDb {
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

/** Mounts `render` under a component that provides `db`. */
export function withSurreal(db: MockDb, inner: Component) {
	return mount(
		defineComponent({
			setup() {
				provideSurreal(db);
				return () => h(inner);
			},
		}),
	);
}

export const names = (wrapper: {
	findAll(selector: string): { text(): string }[];
}) => wrapper.findAll('li').map((item) => item.text());
