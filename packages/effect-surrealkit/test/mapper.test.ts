/**
 * `.surql` imports: the module one file yields is what
 * `renderStatementModules` renders for a scope that holds only that file,
 * byte for byte what `effect-surrealkit statements` writes, run on the same
 * input root; the mapper and the loaders accept exactly the layouts the
 * command accepts and read one literal configuration, or refuse. The hosts
 * that need TypeScript 7.1, Node's hook and Bun run in the mapper fixtures
 * (`npm run test:mapper`).
 */
import {
	cpSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';

import { afterEach, describe, expect, it } from 'vitest';

import { run } from '../src/cli/run.js';
import {
	decodeSurqlOptions,
	EMPTY_SURQL_MODULE,
	loadSurqlModule,
	mapperEntryIn,
	openSurqlProject,
	renderSurqlModule,
	SurqlModuleError,
	surqlBunPlugin,
	surqlLayout,
	surqlProjectFor,
} from '../src/mapper/index.js';
import { offsetOf } from '../src/mapper/module.js';
import { nearestTsconfig, stripJsonComments } from '../src/mapper/project.js';
import { frame, serveRpc } from '../src/mapper/rpc.js';
import { DIAGNOSTIC_CODES, mapperHandlers } from '../src/mapper/server.js';
import { corpusPath } from './documents.js';

const fixture = (name: string) => join(import.meta.dirname, 'fixtures', name);
const bindsDocument = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
const presenceDocument = corpusPath('compiled', 'field-presence.v2.json');

/** The authored fixtures, each an input root, and the schemas to use. */
const corpora = [
	{ name: 'statements', schemas: [undefined] },
	{ name: 'statements-binds', schemas: [undefined, bindsDocument] },
	{ name: 'statements-presence', schemas: [undefined, presenceDocument] },
] as const;

const filesOf = (corpus: string) =>
	readdirSync(fixture(corpus))
		.sort()
		.flatMap((scope) =>
			readdirSync(join(fixture(corpus), scope))
				.sort()
				.map((file) => ({
					scope,
					name: file.slice(0, -'.surql'.length),
					path: join(fixture(corpus), scope, file),
					sql: readFileSync(
						join(fixture(corpus), scope, file),
						'utf8',
					),
				})),
		);

const dirs: string[] = [];
const tempDir = () => {
	const dir = mkdtempSync(join(tmpdir(), 'effect-surrealkit-mapper-'));
	dirs.push(dir);
	return dir;
};
afterEach(() => {
	for (const dir of dirs.splice(0))
		rmSync(dir, { recursive: true, force: true });
});

/** A `tsconfig.json` with an `effect-surrealkit` entry holding `options`. */
const tsconfig = (dir: string, options: unknown, extra: object = {}) => {
	const path = join(dir, 'tsconfig.json');
	writeFileSync(
		path,
		JSON.stringify({
			...extra,
			contentMappers: [
				{
					package: 'effect-surrealkit',
					extensions: ['.surql'],
					options,
				},
			],
		}),
	);
	return path;
};

/** What the command writes, run on `input`: each scope's module. */
const command = async (
	input: string,
	schema: string | undefined,
): Promise<Map<string, string>> => {
	const dir = tempDir();
	let stderr = '';
	const code = await run(
		[
			'statements',
			'--input',
			input,
			'--out',
			join(dir, 'out'),
			...(schema === undefined ? [] : ['--schema', schema]),
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
	expect(code, stderr).toBe(0);
	return new Map(
		readdirSync(join(dir, 'out'))
			.filter((scope) => scope !== 'index.ts')
			.map((scope) => [
				scope,
				readFileSync(join(dir, 'out', scope, 'statements.ts'), 'utf8'),
			]),
	);
};

/** One file's module text: the mark, its import, its constant. */
const parts = (text: string) => {
	const mark = text.slice(0, text.indexOf('\n'));
	const rest = text.slice(mark.length + 2);
	const cut = rest.indexOf('\n\n');
	return {
		mark,
		imports: rest.slice(0, cut).split('\n'),
		constant: rest.slice(cut + 2).replace(/\n$/u, ''),
	};
};

/** The one-file modules of a scope, joined as the renderer joins a scope. */
const compose = (texts: ReadonlyArray<string>): string => {
	const imports = new Set(texts.flatMap((text) => parts(text).imports));
	const ordered = [
		'import * as $analyze from "effect-surrealkit/analyze";',
		'import * as $statement from "effect-surrealkit/statement";',
	].filter((line) => imports.has(line));
	return [
		parts(texts[0] as string).mark,
		'',
		...ordered,
		'',
		texts.map((text) => parts(text).constant).join('\n\n'),
		'',
	].join('\n');
};

/** Speaks the mapper's protocol to `mapperHandlers` over in-memory pipes. */
const mapper = () => {
	const input = new PassThrough();
	const output = new PassThrough();
	const served = serveRpc(input, output, mapperHandlers());
	let buffer = Buffer.alloc(0);
	const waiting = new Map<string, (reply: unknown) => void>();
	output.on('data', (chunk: Buffer) => {
		buffer = Buffer.concat([buffer, chunk]);
		for (;;) {
			const end = buffer.indexOf('\r\n\r\n');
			if (end === -1) return;
			const length = Number(
				/Content-Length: (\d+)/u.exec(
					buffer.subarray(0, end).toString(),
				)?.[1],
			);
			if (buffer.length < end + 4 + length) return;
			const reply = JSON.parse(
				buffer.subarray(end + 4, end + 4 + length).toString(),
			) as { id: string };
			buffer = buffer.subarray(end + 4 + length);
			waiting.get(reply.id)?.(reply);
		}
	});
	let next = 0;
	const call = (method: string, params: unknown) =>
		// biome-ignore lint/suspicious/noExplicitAny: a reply is JSON the assertions read
		new Promise<{ result?: any; error?: any }>((done) => {
			const id = `api${++next}`;
			waiting.set(id, done as (reply: unknown) => void);
			input.write(frame({ jsonrpc: '2.0', id, method, params }));
		});
	const open = (configFileName: string, options: unknown) =>
		call('openProject', {
			configFileName,
			projectHandle: 'h',
			options,
			compilerOptions: {},
		});
	const transform = async (fileName: string, content: string) =>
		(
			await call('transform', {
				fileName,
				content,
				projectHandle: 'h',
			})
		).result;
	return {
		call,
		open,
		transform,
		close: async () => {
			input.end();
			await served;
		},
	};
};

describe('the module of one .surql file', () => {
	for (const corpus of corpora)
		for (const schema of corpus.schemas) {
			const label = `${corpus.name}, ${schema === undefined ? 'no schema' : 'schema'}`;

			it(`is what the command writes on the same input, over the protocol (${label})`, async () => {
				const written = await command(fixture(corpus.name), schema);
				const options = {
					input: fixture(corpus.name),
					...(schema !== undefined && { schema }),
				};
				const config = tsconfig(tempDir(), options);
				const session = mapper();
				const init = await session.call('initialize', {
					positionEncodings: ['utf-8', 'utf-16'],
				});
				expect(init.result).toEqual({
					positionEncoding: 'utf-16',
					diagnosticSource: 'effect-surrealkit',
				});
				const opened = await session.open(config, options);
				expect(opened.result.optionDiagnostics).toEqual([]);
				expect(opened.result.watchedFiles).toEqual(
					schema === undefined ? [config] : [config, schema],
				);
				const byScope = new Map<string, string[]>();
				for (const file of filesOf(corpus.name)) {
					const result = await session.transform(file.path, file.sql);
					expect(
						result.diagnosticDirectives,
						file.path,
					).toBeUndefined();
					expect(result).toMatchObject({
						extension: '.ts',
						mappings: [],
					});
					// The file alone, through the renderer, is the same module.
					expect(result.text).toBe(
						renderSurqlModule(
							file,
							openSurqlProject('/', options).settingsFor(
								file.path,
							),
						).text,
					);
					byScope.set(file.scope, [
						...(byScope.get(file.scope) ?? []),
						result.text,
					]);
				}
				expect([...byScope.keys()]).toEqual([...written.keys()]);
				for (const [scope, texts] of byScope)
					expect(compose(texts)).toBe(written.get(scope));
				await session.close();
			});
		}
});

describe('one literal configuration', () => {
	const project = () => {
		const root = tempDir();
		mkdirSync(join(root, 'queries', 'gauges'), { recursive: true });
		cpSync(
			join(fixture('statements-binds'), 'gauges', 'readings.surql'),
			join(root, 'queries', 'gauges', 'readings.surql'),
		);
		cpSync(bindsDocument, join(root, 'binds.json'));
		return {
			root,
			file: join(root, 'queries', 'gauges', 'readings.surql'),
			options: { input: 'queries', schema: 'binds.json' },
		};
	};

	it('renders when the options TypeScript passes are the entry written in the file', async () => {
		const { root, file, options } = project();
		const config = tsconfig(root, options);
		const session = mapper();
		await session.open(config, options);
		const result = await session.transform(
			file,
			readFileSync(file, 'utf8'),
		);
		expect(result.diagnosticDirectives).toBeUndefined();
		expect(result.text).toContain('binds: {');
		expect(loadSurqlModule(file)).toBe(result.text);
		await session.close();
	});

	it('refuses every file, never with an option diagnostic, when the entry comes through extends', async () => {
		const { root, file, options } = project();
		mkdirSync(join(root, 'base'));
		writeFileSync(
			join(root, 'base', 'tsconfig.base.json'),
			JSON.stringify({
				contentMappers: [
					{
						package: 'effect-surrealkit',
						extensions: ['.surql'],
						options: { ...options, unknown: 1 },
					},
				],
			}),
		);
		const config = join(root, 'tsconfig.json');
		writeFileSync(
			config,
			JSON.stringify({ extends: './base/tsconfig.base.json' }),
		);
		const session = mapper();
		// What TypeScript passes: the inherited entry's options.
		const opened = await session.open(config, { ...options, unknown: 1 });
		expect(opened.result.optionDiagnostics).toEqual([]);
		const result = await session.transform(
			file,
			readFileSync(file, 'utf8'),
		);
		expect(result.text).toBe(EMPTY_SURQL_MODULE);
		expect(
			result.diagnosticDirectives.unusedExpectDirectiveDiagnostics,
		).toEqual([
			{
				code: DIAGNOSTIC_CODES.config,
				messageText: expect.stringMatching(
					/is not written in .*tsconfig\.json itself \(it comes through extends\)/u,
				),
			},
		]);
		// The loader refuses the same tsconfig.json.
		expect(() => loadSurqlModule(file)).toThrow(
			/has extends and no effect-surrealkit entry in contentMappers of its own/u,
		);
		await session.close();
	});

	it('refuses every file when the options passed differ from the entry in the file', async () => {
		const { root, file, options } = project();
		const config = tsconfig(root, options, {
			extends: './missing-base.json',
		});
		const session = mapper();
		const opened = await session.open(config, { input: 'queries' });
		expect(opened.result.optionDiagnostics).toEqual([]);
		const result = await session.transform(
			file,
			readFileSync(file, 'utf8'),
		);
		expect(result.text).toBe(EMPTY_SURQL_MODULE);
		expect(
			result.diagnosticDirectives.unusedExpectDirectiveDiagnostics[0]
				.messageText,
		).toMatch(
			/the options TypeScript passed differ from those written in/u,
		);
		// The entry of the file itself, with extends beside it, is the loaders'.
		expect(loadSurqlModule(file)).toContain('binds: {');
		await session.close();
	});

	it('refuses an inferred project and a file with no project', async () => {
		const { file } = project();
		const session = mapper();
		const opened = await session.open('', { input: 'queries' });
		expect(opened.result.optionDiagnostics).toEqual([]);
		const result = await session.transform(file, 'RETURN 1;');
		expect(
			result.diagnosticDirectives.unusedExpectDirectiveDiagnostics,
		).toEqual([
			{
				code: DIAGNOSTIC_CODES.config,
				messageText: expect.stringMatching(/has no tsconfig\.json/u),
			},
		]);
		const unknown = await session.call('transform', {
			fileName: file,
			content: 'RETURN 1;',
			projectHandle: 'other',
		});
		expect(
			unknown.result.diagnosticDirectives
				.unusedExpectDirectiveDiagnostics[0].messageText,
		).toMatch(/has no project for this file/u);
		await session.close();
	});

	it('reports an option of the entry in the file itself as an option diagnostic', async () => {
		const { root, options } = project();
		const config = tsconfig(root, { ...options, colour: 1 });
		const session = mapper();
		const opened = await session.open(config, { ...options, colour: 1 });
		expect(
			opened.result.optionDiagnostics.map(
				(d: { path: string[] }) => d.path,
			),
		).toEqual([['colour']]);
		await session.close();
	});

	it('the loaders refuse a file with no tsconfig.json entry above it', () => {
		const root = tempDir();
		mkdirSync(join(root, 'q', 'a'), { recursive: true });
		writeFileSync(join(root, 'q', 'a', 'x.surql'), 'RETURN 1;');
		writeFileSync(join(root, 'tsconfig.json'), '{}');
		expect(() => loadSurqlModule(join(root, 'q', 'a', 'x.surql'))).toThrow(
			/has no effect-surrealkit entry in contentMappers/u,
		);
	});

	it('reads mapperEntryIn from the file alone', () => {
		const root = tempDir();
		writeFileSync(
			join(root, 'tsconfig.json'),
			`{
	// comments and trailing commas, as TypeScript reads them
	"extends": "./base.json",
	"contentMappers": [
		{ "package": "other", "extensions": [".x"], "options": { "a": "/*" } },
		{ "package": "effect-surrealkit", "extensions": [".surql"] },
	],
}`,
		);
		expect(mapperEntryIn(join(root, 'tsconfig.json'))).toEqual({
			found: true,
			options: {},
			extends: true,
		});
		mkdirSync(join(root, 'q', 'accounts'), { recursive: true });
		expect(nearestTsconfig(join(root, 'q', 'accounts'))).toBe(
			join(root, 'tsconfig.json'),
		);
		expect(stripJsonComments('{"a": "// no", /* x */ "b": [1,],}')).toBe(
			'{"a": "// no",  "b": [1]}',
		);
	});
});

describe('the layouts the command accepts', () => {
	/** Writes `files` (path → text) under a fresh directory. */
	const tree = (files: Record<string, string>) => {
		const root = tempDir();
		for (const [path, text] of Object.entries(files)) {
			mkdirSync(join(root, path, '..'), { recursive: true });
			writeFileSync(join(root, path), text);
		}
		return root;
	};
	const layout = (root: string, inputs: string[], file: string) =>
		surqlLayout(
			inputs.map((input) => join(root, input)),
			join(root, file),
		);

	it('names <input>/<scope>/<name>.surql as the command does', () => {
		const root = tree({
			'in/accounts/bindOwner.surql': 'RETURN 1;',
			'in/.hidden/x.surql': 'RETURN 1;',
			'in/accounts/.draft.surql': 'RETURN 1;',
		});
		expect(layout(root, ['in'], 'in/accounts/bindOwner.surql')).toEqual({
			scope: 'accounts',
			name: 'bindOwner',
		});
		expect(layout(root, ['in'], 'in/.hidden/x.surql')).toEqual({
			problems: [
				'.hidden/x.surql: the statements command ignores dot entries',
			],
		});
	});

	it('refuses a file outside the input, nested deeper, or with no input', () => {
		const root = tree({
			'a/sub/x.surql': 'RETURN 1;',
			'b/sub/x.surql': 'RETURN 1;',
			'a/sub/deeper/y.surql': 'RETURN 1;',
		});
		expect(layout(root, ['a'], 'b/sub/x.surql')).toEqual({
			problems: [
				expect.stringMatching(/^the file is outside the input/u),
			],
		});
		expect(layout(root, ['.'], 'a/sub/x.surql')).toEqual({
			problems: [
				expect.stringMatching(
					/^a\/sub\/x\.surql is not <scope>\/<name>\.surql/u,
				),
			],
		});
		expect(layout(root, [], 'a/sub/x.surql')).toEqual({
			problems: [expect.stringMatching(/name no input/u)],
		});
	});

	it('refuses a scope name used in two inputs (a/sub/x and b/sub/x)', () => {
		const root = tree({
			'a/sub/x.surql': 'RETURN 1;',
			'b/sub/x.surql': 'RETURN 1;',
		});
		for (const file of ['a/sub/x.surql', 'b/sub/x.surql'])
			expect(layout(root, ['a', 'b'], file)).toEqual({
				problems: expect.arrayContaining([
					expect.stringMatching(/the scope "sub" is in two inputs/u),
				]),
			});
		const apart = tree({
			'a/one/x.surql': 'RETURN 1;',
			'b/two/x.surql': 'RETURN 1;',
		});
		expect(layout(apart, ['a', 'b'], 'b/two/x.surql')).toEqual({
			scope: 'two',
			name: 'x',
		});
	});

	it('refuses what the command refuses anywhere in the input, as the command does', async () => {
		const cases: Array<[Record<string, string>, RegExp]> = [
			[
				{ 'README.md': '' },
				/README\.md: --input holds only scope directories/u,
			],
			[
				{ 'q/notes.txt': '' },
				/q\/notes\.txt: a scope holds only \.surql files/u,
			],
			[{ 'empty/.keep': '' }, /empty: has no \.surql files/u],
			[
				{ 'bad-scope/y.surql': 'RETURN 1;' },
				/scope "bad-scope" is not a JavaScript identifier/u,
			],
			[{ 'q/class.surql': 'RETURN 1;' }, /is a reserved word/u],
		];
		for (const [extra, message] of cases) {
			const root = tree({ 'q/x.surql': 'RETURN 1;', ...extra });
			expect(
				surqlLayout([root], join(root, 'q', 'x.surql')),
				String(message),
			).toEqual({
				problems: expect.arrayContaining([
					expect.stringMatching(message),
				]),
			});
			let stderr = '';
			const code = await run(
				[
					'statements',
					'--input',
					root,
					'--out',
					join(tempDir(), 'out'),
				],
				{
					cwd: root,
					stdin: async () => '',
					stdout: () => {},
					stderr: (text) => {
						stderr += text;
					},
				},
			);
			expect(code, String(message)).toBe(1);
			expect(stderr).toMatch(message);
		}
		// Scope names that differ only in case, in two inputs.
		const root = tree({
			'a/Q/x.surql': 'RETURN 1;',
			'b/q/y.surql': 'RETURN 1;',
		});
		expect(layout(root, ['a', 'b'], 'b/q/y.surql')).toEqual({
			problems: [expect.stringMatching(/differs only in case from "Q"/u)],
		});
	});

	it('is a problem inside the file through the mapper and an error from the loaders', async () => {
		const root = tree({
			'queries/a/sub/x.surql': 'RETURN 1;',
		});
		const options = { input: 'queries' };
		const config = tsconfig(root, options);
		const session = mapper();
		await session.open(config, options);
		const result = await session.transform(
			join(root, 'queries', 'a', 'sub', 'x.surql'),
			'RETURN 1;',
		);
		expect(result.text).toBe(EMPTY_SURQL_MODULE);
		expect(
			result.diagnosticDirectives.unusedExpectDirectiveDiagnostics,
		).toEqual(
			expect.arrayContaining([
				{
					code: DIAGNOSTIC_CODES.layout,
					messageText: expect.stringMatching(
						/is not <scope>\/<name>\.surql/u,
					),
				},
			]),
		);
		expect(() =>
			loadSurqlModule(join(root, 'queries', 'a', 'sub', 'x.surql')),
		).toThrow(SurqlModuleError);
		await session.close();
	});
});

describe('problems inside the .surql file', () => {
	it('places an analyzer error at its line and column', () => {
		const sql = "RETURN 1;\nRETURN 'open;\n";
		const module = renderSurqlModule({ scope: 'q', name: 'broken', sql });
		expect(module.text).toBe(EMPTY_SURQL_MODULE);
		expect(module.problems).toHaveLength(1);
		const [problem] = module.problems;
		expect(problem).toMatchObject({ kind: 'analysis', line: 2 });
		expect(problem?.offset).toBe(
			offsetOf(sql, problem?.line ?? 0, problem?.column ?? 0),
		);
	});

	it('reports a projected field the schema does not declare at its line', () => {
		const settings = openSurqlProject('/', {
			schema: presenceDocument,
		}).settingsFor('/q/people/misspelt.surql');
		const table = Object.entries(
			settings.declared?.detail?.tables ?? {},
		).find(([, t]) => !t.acceptsUndeclared)?.[0];
		expect(table).toBeDefined();
		const sql = `RETURN 1;\nSELECT nmae FROM ${table};\n`;
		const module = renderSurqlModule(
			{ scope: 'people', name: 'misspelt', sql },
			settings,
		);
		expect(module.problems.map((p) => [p.kind, p.line, p.message])).toEqual(
			[
				[
					'undeclared',
					2,
					`selects \`nmae\` of \`${table}\`, which declares no such field`,
				],
			],
		);
	});

	it('answers each problem as an expect directive over an empty range, inside the file', async () => {
		const root = tempDir();
		mkdirSync(join(root, 'queries', 'q'), { recursive: true });
		const sql = "RETURN 1;\nRETURN 'open;\n";
		writeFileSync(join(root, 'queries', 'q', 'broken.surql'), sql);
		const options = { input: 'queries' };
		const session = mapper();
		await session.open(tsconfig(root, options), options);
		const result = await session.transform(
			join(root, 'queries', 'q', 'broken.surql'),
			sql,
		);
		const [problem] = renderSurqlModule({
			scope: 'q',
			name: 'broken',
			sql,
		}).problems;
		expect(result.text).toBe(EMPTY_SURQL_MODULE);
		expect(result.diagnosticDirectives).toEqual({
			unusedExpectDirectiveDiagnostics: [
				{
					code: DIAGNOSTIC_CODES.analysis,
					messageText: `${problem?.message} (${problem?.code})`,
				},
			],
			directives: [[problem?.offset, 1, 0, 0, 1, 0]],
		});
		expect(result.diagnostics).toBeUndefined();
		// An empty file still gets a range inside it.
		const empty = await session.transform(
			join(root, 'queries', 'q', 'broken.surql'),
			'',
		);
		expect(empty.diagnosticDirectives.directives).toEqual([
			[0, 0, 0, 0, 1, 0],
		]);
		await session.close();
	});

	it('refuses a file whose schema document cannot be read', async () => {
		const root = tempDir();
		mkdirSync(join(root, 'queries', 'q'), { recursive: true });
		writeFileSync(join(root, 'queries', 'q', 'all.surql'), 'RETURN 1;');
		const options = {
			input: 'queries',
			schemas: { 'queries/q': 'missing.json' },
		};
		const session = mapper();
		const opened = await session.open(tsconfig(root, options), options);
		expect(
			opened.result.optionDiagnostics.map(
				(d: { path: string[] }) => d.path,
			),
		).toEqual([['schemas', 'queries/q']]);
		const result = await session.transform(
			join(root, 'queries', 'q', 'all.surql'),
			'RETURN 1;',
		);
		expect(result.text).toBe(EMPTY_SURQL_MODULE);
		expect(
			result.diagnosticDirectives.unusedExpectDirectiveDiagnostics,
		).toEqual([
			{
				code: DIAGNOSTIC_CODES.schema,
				messageText: expect.stringMatching(
					/^the schema document of this directory cannot be used: cannot read .*missing\.json/u,
				),
			},
		]);
		await session.close();
	});
});

describe('the options', () => {
	it('decodes input, schema, schemas and holes, and names each it cannot use', () => {
		expect(
			decodeSurqlOptions({
				input: ['a', 'b'],
				schema: 's.json',
				schemas: { a: 'a.json' },
				holes: { open: '<<', close: '>>' },
			}),
		).toEqual({
			options: {
				input: ['a', 'b'],
				schema: 's.json',
				schemas: { a: 'a.json' },
				holes: { open: '<<', close: '>>' },
			},
			problems: [],
		});
		expect(
			decodeSurqlOptions({
				input: [],
				schema: 1,
				schemas: { a: 2 },
				holes: 'x',
				y: 1,
			}).problems.map((p) => p.path),
		).toEqual([['input'], ['schema'], ['schemas', 'a'], ['holes'], ['y']]);
		expect(decodeSurqlOptions([]).problems).toEqual([
			{ path: [], message: 'options must be an object' },
		]);
	});

	it('gives a directory in schemas its document and any other the default', () => {
		const project = openSurqlProject('/root', {
			input: 'queries',
			schema: presenceDocument,
			schemas: { 'queries/gauges': bindsDocument },
		});
		expect(project.inputs).toEqual(['/root/queries']);
		expect(project.documents).toEqual(
			[bindsDocument, presenceDocument].sort(),
		);
		expect(
			project.settingsFor('/root/queries/gauges/x.surql').declared
				?.tables,
		).toContain('gauge');
		expect(
			project.settingsFor('/root/queries/other/x.surql').declared?.tables,
		).not.toContain('gauge');
	});

	it('changes its identity with a schema document', () => {
		const root = tempDir();
		cpSync(bindsDocument, join(root, 's.json'));
		const before = openSurqlProject(root, { schema: 's.json' }).identity;
		expect(openSurqlProject(root, { schema: 's.json' }).identity).toBe(
			before,
		);
		writeFileSync(
			join(root, 's.json'),
			readFileSync(join(root, 's.json'), 'utf8').replace(
				'"gauge"',
				'"gauges"',
			),
		);
		expect(openSurqlProject(root, { schema: 's.json' }).identity).not.toBe(
			before,
		);
	});

	it('names the tsconfig.json a loader read', () => {
		const root = tempDir();
		const config = tsconfig(root, { input: 'q' });
		mkdirSync(join(root, 'q', 'a'), { recursive: true });
		expect(surqlProjectFor(join(root, 'q', 'a', 'x.surql')).config).toBe(
			config,
		);
	});
});

describe('the Bun plugin', () => {
	it('loads the module the loader renders, as TypeScript', () => {
		const root = tempDir();
		cpSync(fixture('statements-binds'), join(root, 'queries'), {
			recursive: true,
		});
		cpSync(bindsDocument, join(root, 'binds.json'));
		tsconfig(root, { input: 'queries', schema: 'binds.json' });
		const file = join(root, 'queries', 'gauges', 'readings.surql');
		let loaded: { contents: string; loader: string } | undefined;
		surqlBunPlugin().setup({
			onLoad: (options, load) => {
				expect(options.filter.test(file)).toBe(true);
				loaded = load({ path: file });
			},
		});
		expect(loaded).toEqual({
			contents: loadSurqlModule(file),
			loader: 'ts',
		});
		expect(loaded?.contents).toContain('binds: {');
	});
});

describe('the JSON-RPC transport', () => {
	it('reads messages split anywhere, answers in order, and skips notifications', async () => {
		const input = new PassThrough();
		const output = new PassThrough();
		const order: string[] = [];
		const served = serveRpc(input, output, {
			slow: async () => {
				await new Promise((done) => setTimeout(done, 20));
				order.push('slow');
				return { a: 1 };
			},
			fast: () => {
				order.push('fast');
				return undefined;
			},
			fails: () => {
				throw new Error('no');
			},
		});
		const chunks: Buffer[] = [];
		output.on('data', (chunk: Buffer) => chunks.push(chunk));
		const bytes = Buffer.concat([
			frame({ jsonrpc: '2.0', id: 1, method: 'slow' }),
			frame({ jsonrpc: '2.0', method: 'fast' }),
			frame({ jsonrpc: '2.0', id: 2, method: 'fails' }),
			frame({ jsonrpc: '2.0', id: 3, method: 'missing' }),
			frame({
				jsonrpc: '2.0',
				id: 4,
				method: 'fast',
				params: { é: 'ü' },
			}),
		]);
		for (let at = 0; at < bytes.length; at += 7)
			input.write(bytes.subarray(at, at + 7));
		input.end();
		await served;
		expect(order).toEqual(['slow', 'fast', 'fast']);
		const replies = Buffer.concat(chunks)
			.toString()
			.split(/Content-Length: \d+\r\n\r\n/u)
			.filter(Boolean)
			.map((body) => JSON.parse(body));
		expect(replies).toEqual([
			{ jsonrpc: '2.0', id: 1, result: { a: 1 } },
			{ jsonrpc: '2.0', id: 2, error: { code: -32603, message: 'no' } },
			{
				jsonrpc: '2.0',
				id: 3,
				error: { code: -32601, message: 'unknown method missing' },
			},
			{ jsonrpc: '2.0', id: 4, result: {} },
		]);
	});
});
