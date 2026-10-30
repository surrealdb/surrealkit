/**
 * Declaration emit of generated modules in a pnpm-style consumer. pnpm does not
 * hoist: `surrealdb` (a peer) is reachable from the consumer's own
 * node_modules, and from effect-surrealkit's directory in the virtual store,
 * never through a common ancestor. A declaration that needs an SDK type then
 * has a portable name for it only if the consumer's program resolves
 * `surrealdb` itself, so a fresh build (no tsbuildinfo) must report no TS2883.
 */
import { spawnSync } from 'node:child_process';
import {
	cpSync,
	mkdirSync,
	mkdtempSync,
	realpathSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { goldenDir } from './documents.js';

const pkg = join(import.meta.dirname, '..');
const modules = join(pkg, 'node_modules');
const link = (target: string, path: string) => {
	mkdirSync(join(path, '..'), { recursive: true });
	symlinkSync(target, path, 'junction');
};

describe('declaration emit in a pnpm consumer', () => {
	let root = '';

	beforeAll(() => {
		const built = spawnSync(
			process.execPath,
			[join(modules, 'tsup', 'dist', 'cli-default.js')],
			{ cwd: pkg, encoding: 'utf8' },
		);
		expect(built.status, `${built.stdout}${built.stderr}`).toBe(0);

		root = mkdtempSync(
			join(realpathSync(tmpdir()), 'effect-surrealkit-pnpm-'),
		);
		// The virtual store: each package at
		// node_modules/.pnpm/<name>@<version>/node_modules/<name>, its
		// dependencies symlinked beside it.
		const store = join(root, 'node_modules', '.pnpm');
		const entry = (name: string, version: string) =>
			join(store, `${name.replace('/', '+')}@${version}`, 'node_modules');
		const place = (
			name: string,
			version: string,
			deps: ReadonlyArray<readonly [string, string]>,
		) => {
			const dir = join(entry(name, version), name);
			cpSync(join(modules, name), dir, { recursive: true });
			for (const [dep, depVersion] of deps)
				link(
					join(entry(dep, depVersion), dep),
					join(entry(name, version), dep),
				);
		};
		place('uuidv7', '1.2.1', []);
		place('@surrealdb/cbor', '2.0.0-alpha.4', []);
		place('@surrealdb/sqon', '0.1.6', [
			['@surrealdb/cbor', '2.0.0-alpha.4'],
			['uuidv7', '1.2.1'],
		]);
		place('surrealdb', '2.1.1', [['@surrealdb/sqon', '0.1.6']]);
		// effect is large and has no dependencies: link it in place.
		link(join(modules, 'effect'), join(entry('effect', '4.0.6'), 'effect'));
		const self = join(
			entry('effect-surrealkit', '0.1.0'),
			'effect-surrealkit',
		);
		mkdirSync(self, { recursive: true });
		cpSync(join(pkg, 'package.json'), join(self, 'package.json'));
		cpSync(join(pkg, 'dist'), join(self, 'dist'), { recursive: true });
		const consumer: Array<readonly [string, string]> = [
			['effect', '4.0.6'],
			['surrealdb', '2.1.1'],
			['effect-surrealkit', '0.1.0'],
		];
		for (const [name, version] of consumer) {
			// The peers resolved beside effect-surrealkit, and every direct
			// dependency of the consumer (it declares the peers, as it must).
			if (name !== 'effect-surrealkit')
				link(
					join(entry(name, version), name),
					join(entry('effect-surrealkit', '0.1.0'), name),
				);
			link(
				join(entry(name, version), name),
				join(root, 'node_modules', name),
			);
		}

		cpSync(goldenDir('corpus'), join(root, 'src'), { recursive: true });
		writeFileSync(
			join(root, 'tsconfig.json'),
			JSON.stringify({
				compilerOptions: {
					target: 'ES2022',
					module: 'ESNext',
					moduleResolution: 'Bundler',
					lib: ['ES2022'],
					strict: true,
					exactOptionalPropertyTypes: true,
					noUnusedLocals: true,
					verbatimModuleSyntax: true,
					isolatedModules: true,
					declaration: true,
					emitDeclarationOnly: true,
					incremental: false,
					skipLibCheck: true,
					outDir: 'out',
					types: [],
				},
				include: ['src/**/*.ts'],
			}),
		);
	});

	afterAll(() => {
		if (root !== '' && process.env.KEEP_PNPM_ROOT !== '1')
			rmSync(root, { recursive: true, force: true });
		else console.log('kept', root);
	});

	it('the corpus modules emit declarations with no non-portable type', () => {
		const result = spawnSync(
			process.execPath,
			[join(modules, 'typescript', 'bin', 'tsc'), '-p', root],
			{ cwd: root, encoding: 'utf8' },
		);
		const output = `${result.stdout}${result.stderr}`;
		expect(output).not.toMatch(/TS2883|TS2742/);
		expect(output).toBe('');
		expect(result.status).toBe(0);
	});
});
