/**
 * `statements --verify` without the optional peer `@surrealdb/node`: the
 * command fails with the install command and writes nothing.
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterAll, describe, expect, it, vi } from 'vitest';

vi.mock('@surrealdb/node', () => {
	throw new Error("Cannot find package '@surrealdb/node'");
});

const { run } = await import('../src/cli/run.js');

const dir = mkdtempSync(join(tmpdir(), 'effect-surrealkit-verify-peer-'));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe('statements --verify without @surrealdb/node', () => {
	it('fails with the install command and writes nothing', async () => {
		let stderr = '';
		const code = await run(
			[
				'statements',
				'--input',
				join(import.meta.dirname, 'fixtures', 'statements'),
				'--out',
				join(dir, 'out'),
				'--verify',
			],
			{
				cwd: dir,
				stdin: async () => '',
				stdout: () => {},
				stderr: (text) => {
					stderr += text;
				},
			},
		);
		expect(code).toBe(1);
		expect(stderr).toContain(
			'npm install --save-dev @surrealdb/node @surrealdb/node-native',
		);
		expect(stderr).not.toContain('modules in');
	});
});
