import { type ChildProcess, spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { exitGuardInstalled, killTree, stopOnExit } from '../src/process.js';
import { createFixture, type Fixture, waitFor } from './helpers.js';

const root = path.resolve(import.meta.dirname, '..');
const viteBin = path.join(root, 'node_modules', 'vite', 'bin', 'vite.js');

describe('killTree', () => {
	it('ends the whole tree with taskkill on Windows, not just the shell', () => {
		const calls: Array<[string, string[]]> = [];
		const child = {
			pid: 4242,
			kill: () => true,
		} as unknown as ChildProcess;
		killTree(
			{ child, group: false },
			'SIGTERM',
			'win32',
			(command, args) => {
				calls.push([command, args]);
			},
		);
		expect(calls).toEqual([['taskkill', ['/pid', '4242', '/T', '/F']]]);
	});

	it('signals the child itself when it leads no group', () => {
		const sent: string[] = [];
		const child = {
			pid: 4242,
			kill: (signal: string) => sent.push(signal),
		} as unknown as ChildProcess;
		killTree({ child, group: false }, 'SIGTERM', 'linux');
		expect(sent).toEqual(['SIGTERM']);
	});
});

describe('stopOnExit', () => {
	it('guards the process only while a command runs', async () => {
		const child = Object.assign(new EventEmitter(), {
			pid: undefined,
			kill: () => true,
		}) as unknown as ChildProcess;
		let finish = (): void => {};
		const exited = new Promise<void>((resolve) => {
			finish = resolve;
		});
		expect(exitGuardInstalled()).toBe(false);
		stopOnExit({ child, group: false }, exited);
		expect(exitGuardInstalled()).toBe(true);
		finish();
		await exited;
		await new Promise((resolve) => setImmediate(resolve));
		expect(exitGuardInstalled()).toBe(false);
	});
});

// A generation runs in its own process group, which the terminal's Ctrl-C does
// not reach, and Vite handles only SIGTERM. Run the Vite CLI with the plugin,
// signal it during the first generation, and check the generation is stopped.
describe.skipIf(process.platform === 'win32')(
	'signals to the dev server',
	() => {
		let fixture: Fixture | undefined;
		let vite: ChildProcess | undefined;

		afterEach(() => {
			vite?.kill('SIGKILL');
			vite = undefined;
			fixture?.cleanup();
			fixture = undefined;
		});

		for (const signal of ['SIGINT', 'SIGHUP'] as const) {
			it(`${signal} stops a running generation`, async () => {
				fixture = createFixture();
				fixture.sleepFor(30);
				fs.writeFileSync(
					path.join(fixture.root, 'vite.config.mts'),
					[
						`import { surrealkitPlugin } from ${JSON.stringify(path.join(root, 'src', 'index.ts'))};`,
						'export default {',
						'\tlogLevel: "silent",',
						'\tplugins: [surrealkitPlugin({',
						'\t\tmode: "generate",',
						`\t\tbinary: ${JSON.stringify(fixture.binary)},`,
						`\t\tenv: ${JSON.stringify(fixture.env)},`,
						'\t})],',
						'};',
						'',
					].join('\n'),
				);
				vite = spawn(process.execPath, [viteBin, '--port', '0'], {
					cwd: fixture.root,
					stdio: 'ignore',
				});
				const exited = new Promise<void>((resolve) =>
					vite?.once('exit', () => resolve()),
				);
				await waitFor(
					() => (fixture?.invocations().length ?? 0) >= 1,
					'the first generation to start',
					15_000,
				);
				vite.kill(signal);
				await waitFor(
					() => fixture?.events().includes('terminated') ?? false,
					'the generation to be terminated',
				);
				await exited;
			});
		}
	},
);
