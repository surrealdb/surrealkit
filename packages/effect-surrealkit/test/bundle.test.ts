/**
 * What a bundler keeps of `effect-surrealkit/statement`: `part` checks its
 * text with the analyzer, and code that only uses the builders must not
 * carry the analyzer with it. Builder and client code never carries the
 * testing helpers, which import `@surrealdb/node` only when a memory
 * database starts, nor the built-in function catalogue, of which code
 * keeps only the families it imports.
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
const functions = source('functions', 'index.ts');

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
// Text only the built-in function catalogue has.
const catalogueMarker = /string::is_email|vector::distance::euclidean/u;

describe('bundling the statement module', () => {
	it('leaves the analyzer out of code that only uses builders', async () => {
		const text = await bundle(
			`import { block, collect, eq, follow, make, object, outgoing, param, select } from ${JSON.stringify(statement)};
			console.log(block, collect, eq, follow, make, object, outgoing, param, select);`,
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

	it('leaves the built-in function catalogue out of builder-only code', async () => {
		const text = await bundle(
			`import { eq, make, param, select } from ${JSON.stringify(statement)};
			import { layer, run } from ${JSON.stringify(client)};
			console.log(eq, make, param, select, layer, run);`,
		);
		expect(text).toContain('SELECT');
		expect(text).not.toMatch(catalogueMarker);
		expect(text).not.toContain('generate-functions');
	});

	it('keeps only the families code imports from the catalogue', async () => {
		const text = await bundle(
			`import { string } from ${JSON.stringify(functions)};
			console.log(string.isEmail);`,
		);
		expect(text).toContain('string::is_email');
		expect(text).not.toContain('vector::distance::euclidean');
	});

	it('keeps it for code that declares parts', async () => {
		const text = await bundle(
			`import { part, sequence } from ${JSON.stringify(statement)};
			console.log(part, sequence);`,
		);
		expect(text).toMatch(analyzerMarker);
	});
});
