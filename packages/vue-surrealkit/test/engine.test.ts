// The components against a real engine, through the core's harness (see
// ../surrealkit-query/test/engine.ts). Skipped without SURREAL_BIN or
// SURREAL_URL.

import { afterAll, beforeAll, describe, it } from 'vitest';
import { type Component, defineComponent, h } from 'vue';

import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
} from '../../surrealkit-query/test/engine.js';
import { cacheFor, LiveQuery as Live } from '../src/index.js';
import { withSurreal } from './helpers.js';

const LiveQuery = Live as unknown as Component;

/** Resolves once `test` holds, or throws after `ms`. */
async function until(test: () => boolean, ms = 5000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!test()) {
		if (Date.now() > deadline) throw new Error('timed out');
		await new Promise((go) => setTimeout(go, 20));
	}
}

describe.skipIf(!engineConfigured)(
	`against an engine (${engineSkipReason})`,
	() => {
		let engine: Engine;

		beforeAll(async () => {
			engine = await startEngine();
			await engine.db.query(
				"CREATE task:a SET title = 'a', status = 'todo'; CREATE task:b SET title = 'b', status = 'done';",
			);
		}, 30_000);

		afterAll(async () => {
			await engine?.stop();
		});

		it('follows create, update and delete, and kills the live query on unmount', async () => {
			const wrapper = withSurreal(
				engine.db as never,
				defineComponent(
					() => () =>
						h(
							LiveQuery,
							{
								q: 'SELECT * FROM task WHERE status = $status ORDER BY title',
								params: { status: 'todo' },
							},
							{
								default: ({
									data,
								}: {
									data: { title: string }[];
								}) =>
									h(
										'ul',
										data.map((task) => h('li', task.title)),
									),
								empty: () => 'none',
							},
						),
				),
			);
			const titles = () =>
				wrapper
					.findAll('li')
					.map((item) => item.text())
					.join(',');
			await until(() => titles() === 'a');
			await engine.db.query(
				"CREATE task:c SET title = 'c', status = 'todo'",
			);
			await until(() => titles() === 'a,c');
			await engine.db.query("UPDATE task:a SET status = 'done'");
			await until(() => titles() === 'c');
			await engine.db.query('DELETE task:c');
			await until(() => wrapper.text() === 'none');
			wrapper.unmount();
			await until(() => cacheFor(engine.db as never).stats().live === 0);
		});
	},
);
