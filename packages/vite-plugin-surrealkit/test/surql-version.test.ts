/**
 * The `surql` option with an effect-surrealkit that has no `.surql` loader:
 * the plugin refuses it by name instead of failing later.
 */
import { describe, expect, it, vi } from 'vitest';

import { surrealkitPlugin } from '../src/index.js';

// An older effect-surrealkit/mapper: the module exists, the loader does not.
vi.mock('effect-surrealkit/mapper', () => ({
	loadSurqlModule: undefined,
	surqlProjectFor: undefined,
}));

describe('surql', () => {
	it('refuses an effect-surrealkit without the loader', async () => {
		const load = surrealkitPlugin({ surql: true }).load as unknown as (
			this: unknown,
			id: string,
		) => Promise<unknown>;
		await expect(
			load.call(
				{ addWatchFile: () => {}, error: () => {} },
				'/q/a/x.surql',
			),
		).rejects.toThrow(
			/needs an effect-surrealkit with the \.surql loader .*the installed one lacks it/u,
		);
	});
});
