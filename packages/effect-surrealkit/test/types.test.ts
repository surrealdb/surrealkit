/**
 * Compiles the golden generated modules with the positive and negative type
 * fixtures (test/types) under strict + exactOptionalPropertyTypes. A negative
 * fixture that stops failing is an "Unused '@ts-expect-error'" error.
 */
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

const root = join(import.meta.dirname, '..');

describe('generated modules type-check', () => {
	it('tsc -p test/types reports no errors', () => {
		const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc');
		const result = spawnSync(
			process.execPath,
			[tsc, '-p', join(root, 'test', 'types')],
			{
				cwd: root,
				encoding: 'utf8',
			},
		);
		expect(`${result.stdout}${result.stderr}`).toBe('');
		expect(result.status).toBe(0);
	});
});
