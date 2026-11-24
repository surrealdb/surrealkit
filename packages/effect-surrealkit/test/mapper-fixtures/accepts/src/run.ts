/**
 * Run by `npm run test:mapper` under `node --import effect-surrealkit/register`
 * and `bun --preload effect-surrealkit/register`, after the type check has
 * written what the content mapper answered it to `../.described` (or, for
 * the all-fixtures loop, after the runner has asked the mapper process). For each
 * `.surql` file: the loader's text is the mapper's text, and the module the
 * hook loads holds the same values as the mapper's text run as TypeScript.
 */
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

import { loadSurqlModule } from 'effect-surrealkit/mapper';

// The project: `accepts`, or the one named on the command line (the
// all-fixtures loop of `npm run test:mapper`), each with `queries/` and
// `.described/`.
const root =
	process.argv[2] ?? join(dirname(fileURLToPath(import.meta.url)), '..');
const queries = join(root, 'queries');
const described = join(root, '.described');
const runtime =
	typeof (globalThis as { Bun?: unknown }).Bun === 'undefined'
		? `Node ${process.versions.node}`
		: `Bun ${(globalThis as unknown as { Bun: { version: string } }).Bun.version}`;

const failures: string[] = [];
let checked = 0;
for (const scope of readdirSync(queries).sort()) {
	for (const entry of readdirSync(join(queries, scope)).sort()) {
		const name = entry.replace(/\.surql$/u, '');
		const file = join(queries, scope, entry);
		const text = join(described, scope, `${name}.ts`);
		const mapped = readFileSync(text, 'utf8');
		if (loadSurqlModule(file) !== mapped)
			failures.push(
				`${scope}/${entry}: the loader's text is not the mapper's`,
			);
		const hooked = (await import(file)) as Record<string, unknown>;
		const typed = (await import(text)) as Record<string, unknown>;
		if (
			Object.keys(hooked).join() !== name ||
			!isDeepStrictEqual({ ...hooked }, { ...typed })
		)
			failures.push(
				`${scope}/${entry}: the loaded module differs from the mapper's`,
			);
		checked++;
	}
}
if (failures.length > 0) {
	console.error(failures.join('\n'));
	process.exit(1);
}
console.log(
	`${checked} .surql modules loaded on ${runtime} as the mapper described them`,
);
