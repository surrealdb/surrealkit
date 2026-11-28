/**
 * The `surql` option: a `.surql` import is the module effect-surrealkit's
 * content mapper gives TypeScript, in a build and on the dev server.
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { build, createServer, transformWithOxc } from 'vite';
import { afterEach, describe, expect, it } from 'vitest';

import { surrealkitPlugin } from '../src/index.js';
import {
	linkEffectSurrealkit,
	type MapperForTests,
	mapperModule,
	sibling,
} from './effect-surrealkit.js';

const available = linkEffectSurrealkit();
const mapper = available
	? ((await import(mapperModule)) as MapperForTests)
	: undefined;

const authored = path.join(sibling, 'test', 'fixtures');
const bindsDocument = path.join(authored, 'binds.v2.json');

const roots: string[] = [];
afterEach(() => {
	for (const root of roots.splice(0))
		fs.rmSync(root, { recursive: true, force: true });
});

/** A project inside this package, so `effect-surrealkit` resolves. */
function tempProject(): string {
	const root = fs.mkdtempSync(path.join(import.meta.dirname, 'tmp-surql-'));
	roots.push(root);
	return root;
}

function writeTsconfig(root: string, options: object, name = 'tsconfig.json') {
	fs.writeFileSync(
		path.join(root, name),
		JSON.stringify({
			contentMappers: [
				{
					package: 'effect-surrealkit',
					extensions: ['.surql'],
					options,
				},
			],
		}),
	);
}

/**
 * The `gauges/record.surql` fixture under `queries/`, with a tsconfig.json
 * whose mapper entry gives it the binds schema, and an entry module.
 */
function project(): { root: string; file: string } {
	const root = tempProject();
	fs.copyFileSync(bindsDocument, path.join(root, 'binds.json'));
	writeTsconfig(root, { input: 'queries', schema: 'binds.json' });
	fs.mkdirSync(path.join(root, 'queries', 'gauges'), { recursive: true });
	const file = path.join(root, 'queries', 'gauges', 'record.surql');
	fs.copyFileSync(
		path.join(authored, 'statements-binds', 'gauges', 'record.surql'),
		file,
	);
	fs.writeFileSync(
		path.join(root, 'main.ts'),
		"export { record } from './queries/gauges/record.surql';\n",
	);
	return { root, file };
}

type Load = (
	this: unknown,
	id: string,
) => Promise<{ code: string; moduleType: string } | null>;

const context = (watched: string[] = []) => ({
	addWatchFile: (file: string) => watched.push(file),
	error: (message: string) => {
		throw new Error(message);
	},
});

/** The mapper's text, written beside the file and imported as TypeScript. */
async function described(root: string, file: string, name: string, tag = name) {
	const text = path.join(root, `described-${tag}.ts`);
	fs.writeFileSync(text, mapper?.loadSurqlModule(file) ?? '');
	return (
		(await import(pathToFileURL(text).href)) as Record<string, unknown>
	)[name];
}

describe.skipIf(!available)('surql', () => {
	it('loads a .surql file as the mapper module, transformed by Vite', async () => {
		const { root, file } = project();
		const plugin = surrealkitPlugin({ surql: true });
		const watched: string[] = [];
		const load = plugin.load as unknown as Load;
		const loaded = await load.call(context(watched), `${file}?import`);
		const text = mapper?.loadSurqlModule(file) ?? '';
		expect(text).toContain('binds: {');
		expect(loaded).toEqual({
			code: (await transformWithOxc(text, `${file}.ts`, { lang: 'ts' }))
				.code,
			map: expect.anything(),
			moduleType: 'js',
		});
		expect(loaded?.code).not.toContain('as const');
		// The tsconfig.json and the schema document are watched.
		expect(watched).toEqual([
			path.join(root, 'tsconfig.json'),
			path.join(root, 'binds.json'),
		]);
		expect(await load.call(context(), '/x/main.ts')).toBeNull();
		expect(
			await (surrealkitPlugin().load as unknown as Load).call(
				context(),
				file,
			),
		).toBeNull();
	});

	it('gives every authored fixture, with and without a schema, the mapper module', async () => {
		let checked = 0;
		for (const [corpus, schema] of [
			['statements', undefined],
			['statements-binds', undefined],
			['statements-binds', bindsDocument],
			['statements-presence', undefined],
			[
				'statements-presence',
				path.join(
					sibling,
					'..',
					'..',
					'crates/surrealkit/tests/fixtures/typegen_corpus/compiled/field-presence.v2.json',
				),
			],
		] as const) {
			const root = tempProject();
			fs.cpSync(path.join(authored, corpus), path.join(root, 'queries'), {
				recursive: true,
			});
			writeTsconfig(root, {
				input: 'queries',
				...(schema !== undefined && { schema }),
			});
			const load = surrealkitPlugin({ surql: true })
				.load as unknown as Load;
			for (const scope of fs
				.readdirSync(path.join(root, 'queries'))
				.sort())
				for (const entry of fs
					.readdirSync(path.join(root, 'queries', scope))
					.sort()) {
					const file = path.join(root, 'queries', scope, entry);
					const name = entry.slice(0, -'.surql'.length);
					const loaded = await load.call(context(), file);
					const script = path.join(
						root,
						`loaded-${scope}-${name}.mjs`,
					);
					fs.writeFileSync(script, loaded?.code ?? '');
					const viaVite = (
						(await import(pathToFileURL(script).href)) as Record<
							string,
							unknown
						>
					)[name];
					expect(viaVite, `${corpus}/${scope}/${entry}`).toEqual(
						await described(root, file, name, `${scope}-${name}`),
					);
					checked++;
				}
		}
		expect(checked).toBeGreaterThan(20);
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
		expect(built.record).toEqual(await described(root, file, 'record'));
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
			expect(served.record).toEqual(
				await described(root, file, 'record'),
			);
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

	it('reads and watches the options of a named tsconfig.json', async () => {
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
		const watched: string[] = [];
		const loaded = await (plugin.load as unknown as Load).call(
			context(watched),
			file,
		);
		expect(loaded?.code).toContain('binds: {');
		expect(watched[0]).toBe(path.join(root, 'tsconfig.app.json'));
	});
});
