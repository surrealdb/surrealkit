/**
 * `.surql` imports: the module one file yields is what
 * `renderStatementModules` renders for a scope that holds only that file,
 * byte for byte what `effect-surrealkit statements` writes for it, and what
 * the content mapper answers over its protocol, schema included. The hosts
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

import ts from 'typescript';
import { afterEach, describe, expect, it } from 'vitest';

import { STATEMENTS_MARK } from '../src/analyze/render.js';
import { run } from '../src/cli/run.js';
import {
	decodeSurqlOptions,
	EMPTY_SURQL_MODULE,
	loadSurqlModule,
	openSurqlProject,
	renderSurqlModule,
	SurqlModuleError,
	surqlBunPlugin,
	surqlFileAt,
	surqlProjectFor,
	surqlScript,
} from '../src/mapper/index.js';
import { offsetOf } from '../src/mapper/module.js';
import {
	mapperOptionsIn,
	nearestTsconfig,
	stripJsonComments,
} from '../src/mapper/project.js';
import { frame, serveRpc } from '../src/mapper/rpc.js';
import { DIAGNOSTIC_CODES, mapperHandlers } from '../src/mapper/server.js';
import { corpusPath, goldenDir } from './documents.js';

const fixture = (name: string) => join(import.meta.dirname, 'fixtures', name);

/** The authored fixtures, their goldens and the schema each was made with. */
const corpora = [
	{ name: 'statements', schema: undefined },
	{
		name: 'statements-binds',
		schema: join(import.meta.dirname, 'fixtures', 'binds.v2.json'),
	},
	{
		name: 'statements-presence',
		schema: corpusPath('compiled', 'field-presence.v2.json'),
	},
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

const quiet = (cwd: string) => {
	let stderr = '';
	return {
		io: {
			cwd,
			stdin: async () => '',
			stdout: () => {},
			stderr: (text: string) => {
				stderr += text;
			},
		},
		stderr: () => stderr,
	};
};

/** What the command writes for a scope that holds only this file. */
const commandOutput = async (
	file: { scope: string; name: string; sql: string },
	schema: string | undefined,
): Promise<string> => {
	const dir = tempDir();
	mkdirSync(join(dir, 'in', file.scope), { recursive: true });
	writeFileSync(join(dir, 'in', file.scope, `${file.name}.surql`), file.sql);
	const { io, stderr } = quiet(dir);
	const code = await run(
		[
			'statements',
			'--input',
			'in',
			'--out',
			'out',
			...(schema === undefined ? [] : ['--schema', schema]),
		],
		io,
	);
	expect(code, stderr()).toBe(0);
	return readFileSync(join(dir, 'out', file.scope, 'statements.ts'), 'utf8');
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
	return {
		call,
		close: async () => {
			input.end();
			await served;
		},
	};
};

describe('the module of one .surql file', () => {
	for (const corpus of corpora) {
		it(`is what the command writes for it alone (${corpus.name})`, async () => {
			const { declaredNames, decodeDocument } = await import(
				'../src/emitter/index.js'
			);
			const { Effect } = await import('effect');
			const declared =
				corpus.schema === undefined
					? undefined
					: declaredNames(
							await Effect.runPromise(
								decodeDocument(
									JSON.parse(
										readFileSync(corpus.schema, 'utf8'),
									),
								),
							),
						);
			const files = filesOf(corpus.name);
			expect(files.length).toBeGreaterThan(0);
			for (const file of files) {
				const module = renderSurqlModule(file, {
					...(declared !== undefined && { declared }),
				});
				expect(module.problems).toEqual([]);
				expect(module.text).toBe(
					await commandOutput(file, corpus.schema),
				);
			}
		});

		it(`composes into the command's module of each scope (${corpus.name})`, async () => {
			const project = openSurqlProject(
				'/',
				corpus.schema === undefined
					? undefined
					: { schema: corpus.schema },
			);
			const byScope = new Map<string, string[]>();
			for (const file of filesOf(corpus.name)) {
				const { text } = renderSurqlModule(
					file,
					project.settingsFor(file.path),
				);
				byScope.set(file.scope, [
					...(byScope.get(file.scope) ?? []),
					text,
				]);
			}
			for (const [scope, texts] of byScope) {
				// Each one-file module is the mark, its imports, and its constant.
				const imports = new Set<string>();
				const constants: string[] = [];
				for (const text of texts) {
					expect(text.startsWith(`${STATEMENTS_MARK}\n\n`)).toBe(
						true,
					);
					const rest = text.slice(STATEMENTS_MARK.length + 2);
					const cut = rest.indexOf('\n\n');
					for (const line of rest.slice(0, cut).split('\n'))
						imports.add(line);
					constants.push(rest.slice(cut + 2).replace(/\n$/u, ''));
				}
				const ordered = [
					'import * as $analyze from "effect-surrealkit/analyze";',
					'import * as $statement from "effect-surrealkit/statement";',
				].filter((line) => imports.has(line));
				const composed = [
					STATEMENTS_MARK,
					'',
					...ordered,
					'',
					constants.join('\n\n'),
					'',
				].join('\n');
				expect(composed).toBe(
					readFileSync(
						join(goldenDir(corpus.name), scope, 'statements.ts'),
						'utf8',
					),
				);
			}
		});

		it(`is what the content mapper answers, schema from its options (${corpus.name})`, async () => {
			const root = tempDir();
			const session = mapper();
			const init = await session.call('initialize', {
				positionEncodings: ['utf-8', 'utf-16'],
			});
			expect(init.result).toEqual({
				positionEncoding: 'utf-16',
				diagnosticSource: 'effect-surrealkit',
			});
			const opened = await session.call('openProject', {
				configFileName: join(root, 'tsconfig.json'),
				projectHandle: 'effect-surrealkit@0.1.0:0',
				options:
					corpus.schema === undefined
						? {}
						: { schema: corpus.schema },
				compilerOptions: {},
			});
			expect(opened.result.optionDiagnostics).toEqual([]);
			expect(opened.result.watchedFiles).toEqual(
				corpus.schema === undefined ? [] : [corpus.schema],
			);
			for (const file of filesOf(corpus.name)) {
				const { result } = await session.call('transform', {
					fileName: file.path,
					content: file.sql,
					projectHandle: 'effect-surrealkit@0.1.0:0',
				});
				expect(result).toEqual({
					extension: '.ts',
					text: await commandOutput(file, corpus.schema),
					mappings: [],
				});
			}
			expect(
				(
					await session.call('closeProject', {
						projectHandle: 'effect-surrealkit@0.1.0:0',
					})
				).result,
			).toEqual({});
			await session.close();
		});
	}

	it('is written as JavaScript by removing only its types', () => {
		const transpile = (text: string) =>
			ts.transpileModule(text, {
				compilerOptions: {
					module: ts.ModuleKind.ESNext,
					target: ts.ScriptTarget.ES2022,
				},
			}).outputText;
		let checked = 0;
		for (const corpus of corpora)
			for (const file of filesOf(corpus.name)) {
				const { text } = renderSurqlModule(file);
				const script = surqlScript(text);
				expect(script).not.toBe(text);
				expect(script.endsWith('\n});\n')).toBe(true);
				// The same program once TypeScript has removed the types, and
				// valid JavaScript as it stands.
				expect(transpile(script)).toBe(transpile(text));
				expect(
					ts.transpileModule(script, {
						fileName: 'x.js',
						reportDiagnostics: true,
						compilerOptions: { allowJs: true },
					}).diagnostics,
				).toEqual([]);
				checked++;
			}
		expect(checked).toBeGreaterThan(20);
		expect(surqlScript(EMPTY_SURQL_MODULE)).toBe(EMPTY_SURQL_MODULE);
		expect(() => surqlScript('export const x = 1 as const;\n')).toThrow(
			/not a one-file \.surql module/u,
		);
	});

	it('is named by its parent directory and file name', () => {
		expect(surqlFileAt('/q/accounts/bindOwner.surql', 'x')).toEqual({
			scope: 'accounts',
			name: 'bindOwner',
			sql: 'x',
		});
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
		expect(sql.slice(0, problem?.offset).split('\n')).toHaveLength(
			problem?.line ?? 0,
		);
	});

	it('reports a projected field the schema does not declare at its line', () => {
		const project = openSurqlProject('/', {
			schema: corpusPath('compiled', 'field-presence.v2.json'),
		});
		const file = '/q/people/misspelt.surql';
		const settings = project.settingsFor(file);
		const table = Object.entries(
			settings.declared?.detail?.tables ?? {},
		).find(([, t]) => !t.acceptsUndeclared)?.[0];
		expect(table).toBeDefined();
		const sql = `RETURN 1;\nSELECT nmae FROM ${table};\n`;
		const module = renderSurqlModule(surqlFileAt(file, sql), settings);
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
		const session = mapper();
		await session.call('initialize', { positionEncodings: ['utf-16'] });
		await session.call('openProject', {
			configFileName: '/q/tsconfig.json',
			projectHandle: 'h',
			options: {},
		});
		const sql = "RETURN 1;\nRETURN 'open;\n";
		const { result } = await session.call('transform', {
			fileName: '/q/q/broken.surql',
			content: sql,
			projectHandle: 'h',
		});
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
		// An empty file still gets a range inside it.
		const empty = await session.call('transform', {
			fileName: '/q/q/Bad-Name.surql',
			content: '',
			projectHandle: 'h',
		});
		expect(empty.result.diagnosticDirectives.directives).toEqual([
			[0, 0, 0, 0, 1, 0],
		]);
		expect(result.diagnostics).toBeUndefined();
		await session.close();
	});

	it('refuses a file whose schema document cannot be read, and says so in the options', async () => {
		const root = tempDir();
		const session = mapper();
		await session.call('initialize', {});
		const opened = await session.call('openProject', {
			configFileName: join(root, 'tsconfig.json'),
			projectHandle: 'h',
			options: { schemas: { queries: 'missing.json' }, colour: 1 },
		});
		expect(
			opened.result.optionDiagnostics.map(
				(d: { path: string[] }) => d.path,
			),
		).toEqual([['colour'], ['schemas', 'queries']]);
		const { result } = await session.call('transform', {
			fileName: join(root, 'queries', 'all.surql'),
			content: 'SELECT * FROM person;',
			projectHandle: 'h',
		});
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
		// Another directory takes no document and renders.
		const other = await session.call('transform', {
			fileName: join(root, 'other', 'all.surql'),
			content: 'SELECT * FROM person;',
			projectHandle: 'h',
		});
		expect(other.result.diagnosticDirectives).toBeUndefined();
		await session.close();
	});

	it('ignores the options of an inferred project and says why', async () => {
		const session = mapper();
		const opened = await session.call('openProject', {
			configFileName: '',
			projectHandle: 'h',
			options: { schema: 'x.json' },
		});
		expect(opened.result.optionDiagnostics).toEqual([
			{
				path: [],
				messageText:
					'the options need a tsconfig.json for their paths; an inferred project takes none',
				code: DIAGNOSTIC_CODES.option,
			},
		]);
		await session.close();
	});
});

describe('the options', () => {
	it('decodes schema, schemas and holes, and names each it cannot use', () => {
		expect(
			decodeSurqlOptions({
				schema: 's.json',
				schemas: { a: 'a.json' },
				holes: { open: '<<', close: '>>' },
			}),
		).toEqual({
			options: {
				schema: 's.json',
				schemas: { a: 'a.json' },
				holes: { open: '<<', close: '>>' },
			},
			problems: [],
		});
		expect(
			decodeSurqlOptions({
				schema: 1,
				schemas: { a: 2 },
				holes: 'x',
				y: 1,
			}).problems.map((p) => p.path),
		).toEqual([['schema'], ['schemas', 'a'], ['holes'], ['y']]);
		expect(decodeSurqlOptions([]).problems).toEqual([
			{ path: [], message: 'options must be an object' },
		]);
	});

	it('gives a directory in schemas its document and any other the default', () => {
		const binds = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
		const presence = corpusPath('compiled', 'field-presence.v2.json');
		const project = openSurqlProject('/root', {
			schema: presence,
			schemas: { 'queries/gauges': binds },
		});
		expect(project.documents).toEqual([binds, presence].sort());
		expect(
			project.settingsFor('/root/queries/gauges/x.surql').declared
				?.tables,
		).toContain('gauge');
		expect(
			project.settingsFor('/root/queries/other/x.surql').declared?.tables,
		).not.toContain('gauge');
		expect(
			openSurqlProject('/root', undefined).settingsFor('/root/x.surql'),
		).toEqual({});
	});

	it('changes its identity with a schema document', () => {
		const root = tempDir();
		cpSync(
			join(import.meta.dirname, 'fixtures', 'binds.v2.json'),
			join(root, 's.json'),
		);
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

	it('reads the effect-surrealkit entry of the nearest tsconfig.json', () => {
		const root = tempDir();
		writeFileSync(
			join(root, 'tsconfig.json'),
			`{
	// comments and trailing commas, as TypeScript reads them
	"compilerOptions": { "strict": true, },
	"contentMappers": [
		{ "package": "other", "extensions": [".x"], "options": { "a": "/*" } },
		{ "package": "effect-surrealkit", "extensions": [".surql"], "options": { "holes": { "open": "<<", "close": ">>" } } },
	],
}`,
		);
		mkdirSync(join(root, 'q', 'accounts'), { recursive: true });
		expect(nearestTsconfig(join(root, 'q', 'accounts'))).toBe(
			join(root, 'tsconfig.json'),
		);
		expect(mapperOptionsIn(join(root, 'tsconfig.json'))).toEqual({
			holes: { open: '<<', close: '>>' },
		});
		expect(stripJsonComments('{"a": "// no", /* x */ "b": [1,],}')).toBe(
			'{"a": "// no",  "b": [1]}',
		);
		expect(
			surqlProjectFor(join(root, 'q', 'accounts', 'x.surql')).settingsFor(
				join(root, 'q', 'accounts', 'x.surql'),
			),
		).toEqual({ holes: { open: '<<', close: '>>' } });
	});
});

describe('the run-time loaders', () => {
	it('load the module the mapper describes, and refuse a file with problems', async () => {
		const root = tempDir();
		cpSync(
			join(import.meta.dirname, 'fixtures', 'binds.v2.json'),
			join(root, 'binds.json'),
		);
		writeFileSync(
			join(root, 'tsconfig.json'),
			JSON.stringify({
				contentMappers: [
					{
						package: 'effect-surrealkit',
						extensions: ['.surql'],
						options: { schemas: { gauges: 'binds.json' } },
					},
				],
			}),
		);
		cpSync(fixture('statements-binds'), root, { recursive: true });
		const file = join(root, 'gauges', 'readings.surql');
		const session = mapper();
		await session.call('openProject', {
			configFileName: join(root, 'tsconfig.json'),
			projectHandle: 'h',
			options: { schemas: { gauges: 'binds.json' } },
		});
		const described = await session.call('transform', {
			fileName: file,
			content: readFileSync(file, 'utf8'),
			projectHandle: 'h',
		});
		await session.close();
		expect(loadSurqlModule(file)).toBe(described.result.text);
		expect(described.result.text).toBe(
			await commandOutput(
				surqlFileAt(file, readFileSync(file, 'utf8')),
				join(root, 'binds.json'),
			),
		);
		let loaded: { contents: string; loader: string } | undefined;
		surqlBunPlugin().setup({
			onLoad: (options, load) => {
				expect(options.filter.test(file)).toBe(true);
				loaded = load({ path: file });
			},
		});
		expect(loaded).toEqual({
			contents: described.result.text,
			loader: 'ts',
		});
		writeFileSync(join(root, 'gauges', 'broken.surql'), "RETURN 'open;");
		expect(() =>
			loadSurqlModule(join(root, 'gauges', 'broken.surql')),
		).toThrow(SurqlModuleError);
		expect(() =>
			loadSurqlModule(join(root, 'gauges', 'broken.surql')),
		).toThrow(/gauges\/broken\.surql:1:\d+: /u);
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
		const text = Buffer.concat(chunks).toString();
		const replies = text
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
