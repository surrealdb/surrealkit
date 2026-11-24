/**
 * The `surql` option: a `.surql` import is the module effect-surrealkit's
 * content mapper gives TypeScript, in a build and on the dev server.
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import {
	loadSurqlModule,
	SurqlModuleError,
	surqlProjectFor,
	surqlScript,
} from 'effect-surrealkit/mapper';
import { build, createServer } from 'vite';
import { afterEach, describe, expect, it } from 'vitest';

import { surrealkitPlugin } from '../src/index.js';

const effectSurrealkit = path.join(
	import.meta.dirname,
	'..',
	'node_modules',
	'effect-surrealkit',
);
const bindsDocument = path.join(
	effectSurrealkit,
	'test',
	'fixtures',
	'binds.v2.json',
);
const record = fs.readFileSync(
	path.join(
		effectSurrealkit,
		'test',
		'fixtures',
		'statements-binds',
		'gauges',
		'record.surql',
	),
	'utf8',
);

const roots: string[] = [];
afterEach(() => {
	for (const root of roots.splice(0))
		fs.rmSync(root, { recursive: true, force: true });
});

/**
 * A project inside this package, so `effect-surrealkit` resolves: a
 * `tsconfig.json` whose mapper entry gives `queries/gauges` the binds schema,
 * one `.surql` file, and an entry that re-exports it.
 */
function project(): { root: string; file: string } {
	const root = fs.mkdtempSync(path.join(import.meta.dirname, 'tmp-surql-'));
	roots.push(root);
	fs.copyFileSync(bindsDocument, path.join(root, 'binds.json'));
	fs.writeFileSync(
		path.join(root, 'tsconfig.json'),
		JSON.stringify({
			contentMappers: [
				{
					package: 'effect-surrealkit',
					extensions: ['.surql'],
					options: { schemas: { 'queries/gauges': 'binds.json' } },
				},
			],
		}),
	);
	fs.mkdirSync(path.join(root, 'queries', 'gauges'), { recursive: true });
	const file = path.join(root, 'queries', 'gauges', 'record.surql');
	fs.writeFileSync(file, record);
	fs.writeFileSync(
		path.join(root, 'main.ts'),
		"export { record } from './queries/gauges/record.surql';\n",
	);
	return { root, file };
}

/** The mapper's text, written beside the file and imported as TypeScript. */
async function described(root: string, file: string): Promise<unknown> {
	const text = path.join(root, 'described.ts');
	fs.writeFileSync(text, loadSurqlModule(file));
	return ((await import(pathToFileURL(text).href)) as { record: unknown })
		.record;
}

describe('surql', () => {
	it('loads a .surql file as the module the mapper describes', async () => {
		const { file } = project();
		const plugin = surrealkitPlugin({ surql: true });
		const watched: string[] = [];
		const load = plugin.load as unknown as (
			this: unknown,
			id: string,
		) => Promise<{ code: string; moduleType: string } | null>;
		const context = {
			addWatchFile: (watchedFile: string) => watched.push(watchedFile),
			error: (message: string) => {
				throw new Error(message);
			},
		};
		const loaded = await load.call(context, `${file}?import`);
		expect(loaded).toEqual({
			code: surqlScript(loadSurqlModule(file)),
			moduleType: 'js',
		});
		expect(loaded?.code).toContain('binds: {');
		expect(loaded?.code.endsWith('\n});\n')).toBe(true);
		expect(watched).toEqual(surqlProjectFor(file).documents);
		expect(await load.call(context, '/x/main.ts')).toBeNull();
		expect(
			await (surrealkitPlugin().load as unknown as typeof load).call(
				context,
				file,
			),
		).toBeNull();
	});

	it('builds it into the bundle', async () => {
		const { root, file } = project();
		await build({
			configFile: false,
			root,
			logLevel: 'silent',
			build: {
				ssr: path.join(root, 'main.ts'),
				outDir: path.join(root, 'out'),
			},
			plugins: [
				surrealkitPlugin({
					surql: true,
					include: ['build'],
					runOnStartup: false,
				}),
			],
		});
		const built = (await import(
			pathToFileURL(path.join(root, 'out', 'main.js')).href
		)) as { record: unknown };
		expect(built.record).toEqual(await described(root, file));
	});

	it('serves it on the dev server', async () => {
		const { root, file } = project();
		const server = await createServer({
			configFile: false,
			root,
			logLevel: 'silent',
			server: { middlewareMode: true, ws: false },
			plugins: [surrealkitPlugin({ surql: true, include: ['build'] })],
		});
		try {
			const served = (await server.ssrLoadModule(
				path.join(root, 'main.ts'),
			)) as { record: unknown };
			expect(served.record).toEqual(await described(root, file));
		} finally {
			await server.close();
		}
	});

	it('fails the build on a file with problems, naming it', async () => {
		const { root } = project();
		fs.writeFileSync(
			path.join(root, 'queries', 'gauges', 'broken.surql'),
			"RETURN 'open;\n",
		);
		fs.writeFileSync(
			path.join(root, 'main.ts'),
			"export { broken } from './queries/gauges/broken.surql';\n",
		);
		expect(() =>
			loadSurqlModule(
				path.join(root, 'queries', 'gauges', 'broken.surql'),
			),
		).toThrow(SurqlModuleError);
		await expect(
			build({
				configFile: false,
				root,
				logLevel: 'silent',
				build: {
					ssr: path.join(root, 'main.ts'),
					outDir: path.join(root, 'out'),
				},
				plugins: [
					surrealkitPlugin({
						surql: true,
						include: ['build'],
						runOnStartup: false,
					}),
				],
			}),
		).rejects.toThrow(/gauges\/broken\.surql:1:\d+: unterminated string/u);
	});

	it('reads the options of a named tsconfig.json', async () => {
		const { root, file } = project();
		fs.renameSync(
			path.join(root, 'tsconfig.json'),
			path.join(root, 'tsconfig.app.json'),
		);
		const plugin = surrealkitPlugin({
			surql: { tsconfig: 'tsconfig.app.json' },
			include: ['build'],
		});
		(plugin.configResolved as unknown as (config: unknown) => void)({
			root,
		});
		const loaded = await (
			plugin.load as unknown as (
				this: unknown,
				id: string,
			) => Promise<{ code: string } | null>
		).call({ addWatchFile: () => {}, error: () => {} }, file);
		expect(loaded?.code).toBe(
			surqlScript(
				loadSurqlModule(file, {
					tsconfig: path.join(root, 'tsconfig.app.json'),
				}),
			),
		);
		expect(loaded?.code).toContain('binds: {');
	});
});
