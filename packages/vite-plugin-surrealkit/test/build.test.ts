import { build } from 'vite';
import { afterEach, describe, expect, it } from 'vitest';

import {
	type SurrealkitPluginOptions,
	surrealkitPlugin,
} from '../src/index.js';
import { createFixture, type Fixture } from './helpers.js';

let fixture: Fixture | undefined;

afterEach(() => {
	fixture?.cleanup();
	fixture = undefined;
});

function runBuild(options: SurrealkitPluginOptions = {}): Promise<unknown> {
	if (!fixture) throw new Error('fixture not created');

	return build({
		configFile: false,
		root: fixture.root,
		logLevel: 'silent',
		plugins: [
			surrealkitPlugin({
				binary: fixture.binary,
				env: fixture.env,
				include: ['build'],
				...options,
			}),
		],
	});
}

describe('build', () => {
	it('syncs once per build', async () => {
		fixture = createFixture();
		await runBuild();

		expect(fixture.invocations()).toEqual(['sync']);
	});

	it('does not sync during build by default', async () => {
		fixture = createFixture();
		await runBuild({ include: undefined });

		expect(fixture.invocations()).toHaveLength(0);
	});

	it('skips the sync when runOnStartup is false', async () => {
		fixture = createFixture();
		await runBuild({ runOnStartup: false });

		expect(fixture.invocations()).toHaveLength(0);
	});

	it('forwards schemas, targets and extra args as CLI flags', async () => {
		fixture = createFixture();
		await runBuild({
			schemas: ['core', 'billing'],
			targets: ['primary'],
			syncArgs: ['--allow-shared-prune'],
		});

		expect(fixture.invocations()).toEqual([
			'sync --schema core --schema billing --target primary --allow-shared-prune',
		]);
	});

	it('forwards --all', async () => {
		fixture = createFixture();
		await runBuild({ all: true });

		expect(fixture.invocations()).toEqual(['sync --all']);
	});

	it('fails the build when sync exits non-zero', async () => {
		fixture = createFixture();
		fixture.failWith(2);

		await expect(runBuild()).rejects.toThrow(/sync failed \(exit code 2\)/);
	});

	it('does not fail the build when failBuildOnError is false', async () => {
		fixture = createFixture();
		fixture.failWith(2);

		await expect(
			runBuild({ failBuildOnError: false }),
		).resolves.toBeDefined();
		expect(fixture.invocations()).toEqual(['sync']);
	});

	it('reports a missing binary without crashing the build', async () => {
		fixture = createFixture();

		await expect(
			runBuild({
				binary: 'surrealkit-does-not-exist',
				failBuildOnError: false,
			}),
		).resolves.toBeDefined();
		expect(fixture.invocations()).toHaveLength(0);
	});
});
