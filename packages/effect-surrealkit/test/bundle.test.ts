/**
 * What a bundler keeps of `effect-surrealkit/statement`: `part` checks its
 * text with the analyzer, and code that only uses the builders must not
 * carry the analyzer with it. Builder and client code never carries the
 * testing helpers, which import `@surrealdb/node` only when a memory
 * database starts.
 */
import { createRequire } from 'node:module';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

interface Built {
	readonly outputFiles: ReadonlyArray<{ readonly text: string }>;
}

// esbuild, as tsup (a dev dependency) resolves it: no dependency of its own.
const fromTsup = createRequire(
	createRequire(import.meta.url).resolve('tsup/package.json'),
);
const { build } = fromTsup('esbuild') as {
	readonly build: (options: Record<string, unknown>) => Promise<Built>;
};

const source = (...path: ReadonlyArray<string>) =>
	join(import.meta.dirname, '..', 'src', ...path);
const statement = source('statement', 'index.ts');
const client = source('client', 'index.ts');
const testing = source('testing', 'index.ts');

const bundle = async (contents: string): Promise<string> => {
	const result = await build({
		stdin: { contents, resolveDir: import.meta.dirname, loader: 'ts' },
		bundle: true,
		write: false,
		format: 'esm',
		platform: 'neutral',
		treeShaking: true,
		// The optional engine of the testing helpers stays a runtime import.
		external: ['effect', 'surrealdb', '@surrealdb/node'],
		logLevel: 'silent',
	});
	return result.outputFiles.map((file) => file.text).join('\n');
};

// Text only the analyzer has.
const analyzerMarker = /returnInTransaction|unterminatedStatement/u;
// Text only the testing helpers have, and the engine they import.
const testingMarker = /the scripted client has no reply|@surrealdb\/node/u;

describe('bundling the statement module', () => {
	it('leaves the analyzer out of code that only uses builders', async () => {
		const text = await bundle(
			`import { eq, make, param, select } from ${JSON.stringify(statement)};
			console.log(eq, make, param, select);`,
		);
		expect(text).toContain('SELECT');
		expect(text).not.toMatch(analyzerMarker);
	});

	it('leaves the testing helpers out of builder and client code', async () => {
		const text = await bundle(
			`import { eq, param, select } from ${JSON.stringify(statement)};
			import { layer, make, run, transaction } from ${JSON.stringify(client)};
			console.log(eq, param, select, layer, make, run, transaction);`,
		);
		expect(text).toContain('SELECT');
		expect(text).not.toMatch(testingMarker);
		expect(text).not.toMatch(analyzerMarker);
	});

	it('imports @surrealdb/node only lazily, from the testing helpers', async () => {
		const text = await bundle(
			`import { fake, memoryLayer } from ${JSON.stringify(testing)};
			console.log(fake, memoryLayer);`,
		);
		expect(text).toMatch(/import\(\s*"@surrealdb\/node"\s*\)/u);
		expect(text).not.toMatch(/^\s*import[^(]*["']@surrealdb\/node["']/mu);
	});

	it('keeps it for code that declares parts', async () => {
		const text = await bundle(
			`import { part, sequence } from ${JSON.stringify(statement)};
			console.log(part, sequence);`,
		);
		expect(text).toMatch(analyzerMarker);
	});
});
