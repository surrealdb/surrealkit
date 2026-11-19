/**
 * What a bundler keeps of `effect-surrealkit/statement`: `part` checks its
 * text with the analyzer, and code that only uses the builders must not
 * carry the analyzer with it.
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

const statement = join(
	import.meta.dirname,
	'..',
	'src',
	'statement',
	'index.ts',
);

const bundle = async (contents: string): Promise<string> => {
	const result = await build({
		stdin: { contents, resolveDir: import.meta.dirname, loader: 'ts' },
		bundle: true,
		write: false,
		format: 'esm',
		platform: 'neutral',
		treeShaking: true,
		external: ['effect', 'surrealdb'],
		logLevel: 'silent',
	});
	return result.outputFiles.map((file) => file.text).join('\n');
};

// Text only the analyzer has.
const analyzerMarker = /returnInTransaction|unterminatedStatement/u;

describe('bundling the statement module', () => {
	it('leaves the analyzer out of code that only uses builders', async () => {
		const text = await bundle(
			`import { eq, make, param, select } from ${JSON.stringify(statement)};
			console.log(eq, make, param, select);`,
		);
		expect(text).toContain('SELECT');
		expect(text).not.toMatch(analyzerMarker);
	});

	it('keeps it for code that declares parts', async () => {
		const text = await bundle(
			`import { part, sequence } from ${JSON.stringify(statement)};
			console.log(part, sequence);`,
		);
		expect(text).toMatch(analyzerMarker);
	});
});
