/**
 * The sibling `effect-surrealkit`, for the `surql` tests. It is no dependency
 * of this package: the tests link its built copy into `node_modules` when it
 * is there (`npm run build` in `packages/effect-surrealkit`), and skip
 * without it, unless `SURQL_TESTS=required`, as in CI, where they fail.
 */
import fs from 'node:fs';
import path from 'node:path';

export const sibling = path.resolve(
	import.meta.dirname,
	'..',
	'..',
	'effect-surrealkit',
);
const linked = path.resolve(
	import.meta.dirname,
	'..',
	'node_modules',
	'effect-surrealkit',
);

/** Whether `effect-surrealkit/mapper` resolves from this package. */
export function linkEffectSurrealkit(): boolean {
	const built = fs.existsSync(path.join(sibling, 'dist', 'mapper.js'));
	if (built && !fs.existsSync(linked)) {
		fs.mkdirSync(path.dirname(linked), { recursive: true });
		fs.symlinkSync(path.relative(path.dirname(linked), sibling), linked);
	}
	const available =
		built && fs.existsSync(path.join(linked, 'dist', 'mapper.js'));
	if (!available && process.env.SURQL_TESTS === 'required')
		throw new Error(
			`SURQL_TESTS=required, and effect-surrealkit is not built at ${sibling}`,
		);
	if (!available)
		console.warn(
			'[vite-plugin-surrealkit] surql tests skipped: build packages/effect-surrealkit first',
		);
	return available;
}

/** The functions of `effect-surrealkit/mapper` the tests call. */
export interface MapperForTests {
	loadSurqlModule(file: string, options?: { tsconfig?: string }): string;
	surqlProjectFor(
		file: string,
		tsconfig?: string,
	): { readonly config?: string; readonly documents: ReadonlyArray<string> };
}

export const mapperModule = 'effect-surrealkit/mapper';
