import { spawnSync } from 'node:child_process';
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import { Effect, Exit, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import {
	decodeDocument,
	generate,
	semanticDigest,
} from '../src/emitter/index.js';
import { refine, type TableDescriptor } from '../src/index.js';
import {
	at,
	compiledPath,
	corpusOptions,
	examplePath,
	goldenDir,
	goldens,
	jsonSchemaPath,
	readJson,
	sha256,
	withoutToolVersions,
} from './documents.js';

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
const failure = async <A, E>(effect: Effect.Effect<A, E>): Promise<E> => {
	const exit = await Effect.runPromiseExit(effect);
	if (Exit.isSuccess(exit)) throw new Error('expected a failure');
	const error = exit.cause.reasons.find((reason) => reason._tag === 'Fail');
	if (error === undefined || error._tag !== 'Fail')
		throw new Error('expected a typed failure');
	return error.error;
};

describe('input documents', () => {
	const ajv = new Ajv2020({ allErrors: true, strict: false });
	const validate = ajv.compile(readJson(jsonSchemaPath));

	for (const [name, path] of [
		['example.v2.json', examplePath],
		['compiled corpus', compiledPath('corpus')],
		['compiled field-presence tables', compiledPath('field-presence')],
	] as const) {
		it(`${name} validates against the published JSON Schema`, () => {
			const valid = validate(readJson(path));
			expect(validate.errors ?? []).toEqual([]);
			expect(valid).toBe(true);
		});

		it(`${name} decodes strictly and its semanticDigest verifies`, async () => {
			const document = readJson(path);
			await run(decodeDocument(document));
			expect(semanticDigest(document, sha256)).toBe(
				document.semanticDigest,
			);
		});
	}
});

describe('access authentication policy', () => {
	const withAuthenticate = (authenticate: unknown) => {
		const document = readJson(examplePath);
		return {
			...document,
			accesses: [
				{
					name: 'sample',
					method: 'record',
					signin: 'RETURN principal:one',
					authenticate,
					durations: { token: '15m', session: '1h' },
				},
			],
		};
	};

	it('accepts AUTHENTICATE and carries its distinction into the generated manifest', async () => {
		const digests: string[] = [];
		for (const expression of ['RETURN $auth', 'RETURN NONE']) {
			const input = withAuthenticate(expression);
			const document = {
				...input,
				semanticDigest: semanticDigest(input, sha256),
			};
			const decoded = await run(decodeDocument(document));
			expect(decoded.accesses[0]?.authenticate).toBe(expression);
			const generated = await run(
				generate(document, { ...corpusOptions }, { sha256 }),
			);
			expect(
				generated.files.find((file) => file.path === 'manifest.ts')
					?.content,
			).toContain(document.semanticDigest);
			digests.push(document.semanticDigest);
		}
		expect(digests[0]).not.toBe(digests[1]);
	});

	it('keeps legacy absent-field documents and their digests unchanged', async () => {
		const document = readJson(examplePath);
		const decoded = await run(decodeDocument(document));
		expect(
			decoded.accesses.every(
				(access) => access.authenticate === undefined,
			),
		).toBe(true);
		expect(decoded).toEqual(document);
		expect(semanticDigest(decoded, sha256)).toBe(document.semanticDigest);
	});

	it.each([
		null,
		1,
		true,
	])('rejects a malformed authenticate value: %s', async (value) => {
		const error = await failure(decodeDocument(withAuthenticate(value)));
		expect(error.message).toContain('authenticate');
	});
});

describe('golden generated output', () => {
	for (const golden of goldens) {
		it(`${golden.name} matches test/golden/${golden.name}`, async () => {
			const result = await run(
				generate(golden.document(), golden.options, { sha256 }),
			);
			const dir = goldenDir(golden.name);
			if (process.env.UPDATE_GOLDEN === '1') {
				rmSync(dir, { recursive: true, force: true });
				mkdirSync(dir, { recursive: true });
				for (const file of result.files)
					writeFileSync(join(dir, file.path), file.content);
			}
			const onDisk = readdirSync(dir).sort();
			expect(onDisk).toEqual(result.files.map((file) => file.path));
			for (const file of result.files) {
				expect(withoutToolVersions(file.content), file.path).toBe(
					withoutToolVersions(
						readFileSync(join(dir, file.path), 'utf8'),
					),
				);
			}
		});
	}

	it('the whole corpus in the bigint profile differs from the corpus golden only in its integers', async () => {
		const result = await run(
			generate(readJson(compiledPath('corpus')), {
				...corpusOptions,
				int: 'bigint',
			}),
		);
		const dir = goldenDir('corpus');
		expect(result.files.map((f) => f.path)).toEqual(
			readdirSync(dir).sort(),
		);
		let changed = 0;
		for (const file of result.files) {
			const big = withoutToolVersions(file.content).split('\n');
			const safe = withoutToolVersions(
				readFileSync(join(dir, file.path), 'utf8'),
			).split('\n');
			expect(big.length, file.path).toBe(safe.length);
			for (const [i, line] of big.entries()) {
				if (line === safe[i]) continue;
				changed++;
				// Back to the safe-number spelling of every integer position.
				const asSafe = line
					.replace(
						/SurrealSchema\.Int64FromJson\.pipe\(Schema\.decodeTo\(SurrealSchema\.Int64Literal\("(-?\d+)"\)\)\)/g,
						'Schema.Literal($1)',
					)
					.replace(
						/SurrealSchema\.Int64Literal\("(-?\d+)"\)/g,
						'Schema.Literal($1)',
					)
					.replace(
						/SurrealSchema\.Int64FromJson\b/g,
						'SurrealSchema.Int',
					)
					.replace(
						/Schema\.(is\w+?)BigInt\((-?\d+)n\)/g,
						'Schema.$1($2)',
					)
					.replace(/SurrealSchema\.Int64\b/g, 'SurrealSchema.Int')
					.replace('int: "bigint"', 'int: "safe-number"');
				expect(asSafe, `${file.path}:${i + 1}`).toBe(safe[i]);
			}
		}
		// Every integer position of the corpus: ids, fields, literals, checks,
		// function arguments and returns, and the manifest's profile (20), and
		// the fields of the JSON world (11).
		expect(changed).toBe(31);
	});

	it('ignores only tool versions when comparing goldens', () => {
		const manifest = readFileSync(
			join(goldenDir('example'), 'manifest.ts'),
			'utf8',
		);
		const bumped = manifest
			.replace(
				'name: "effect-surrealkit", version: "0.1.0"',
				'name: "effect-surrealkit", version: "0.2.0"',
			)
			.replace('surrealkit: "1.0.0-beta.6"', 'surrealkit: "1.0.0"')
			.replace('commit: "fd7b075"', 'commit: "0123456789abcdef"')
			.replace('engine: "3.3.0"', 'engine: "3.3.9"');
		expect(bumped).not.toBe(manifest);
		expect(withoutToolVersions(bumped)).toBe(withoutToolVersions(manifest));
		const unpinnedCommit = manifest.replace(/\t\tcommit: "[^"]*",\n/, '');
		expect(withoutToolVersions(unpinnedCommit)).toBe(
			withoutToolVersions(manifest),
		);
		for (const [from, to] of [
			['name: "effect-surrealkit"', 'name: "other"'],
			['adapter: "structure-3.3"', 'adapter: "structure-3.2"'],
			['store: "corpus"', 'store: "other"'],
			['effect: "4.0.6"', 'effect: "4.0.7"'],
		] as const) {
			expect(
				withoutToolVersions(manifest.replace(from, to)),
				to,
			).not.toBe(withoutToolVersions(manifest));
		}
	});

	it('is deterministic: the same input twice gives identical bytes', async () => {
		const document = readJson(compiledPath('corpus'));
		const first = await run(generate(document, { ...corpusOptions }));
		const second = await run(
			generate(structuredClone(document), { ...corpusOptions }),
		);
		expect(second.files).toEqual(first.files);
	});
});

describe('generation rules', () => {
	const example = () =>
		readJson(examplePath) as Record<string, unknown> & {
			tables: Array<Record<string, unknown>>;
			diagnostics: Array<Record<string, unknown>>;
		};
	const withDigest = (document: Record<string, unknown>) => ({
		...document,
		semanticDigest: semanticDigest(document, sha256),
	});

	it('rejects unknown properties anywhere in the document', async () => {
		const document = example();
		at(
			at(document.tables, 0).fields as Array<Record<string, unknown>>,
			0,
		).surprise = true;
		const error = await failure(generate(document, { ...corpusOptions }));
		expect(error.message).toMatch(/Expected no excess property/);
		expect(error.message).toMatch(/surprise/);
	});

	it('rejects a document whose semanticDigest does not match its content', async () => {
		const document = example();
		at(document.tables, 0).comment = 'edited by hand';
		const error = await failure(
			generate(document, { ...corpusOptions }, { sha256 }),
		);
		expect(error.message).toMatch(/semanticDigest mismatch/);
	});

	it('refuses error diagnostics unless the code is acknowledged with a reason', async () => {
		const document = example();
		document.diagnostics.push({
			severity: 'error',
			code: 'kind-unrecognized',
			table: 'doc',
			field: ['body'],
			source: 'mystery',
			reason: 'A kind string could not be parsed.',
		});
		const error = await failure(generate(document, { ...corpusOptions }));
		expect(error.message).toMatch(/kind-unrecognized \(doc body\)/);
		const acknowledged = await run(
			generate(document, {
				...corpusOptions,
				acknowledge: { 'kind-unrecognized': 'tracked upstream' },
			}),
		);
		expect(acknowledged.files.length).toBeGreaterThan(0);
	});

	it('rejects unknown emitter options and a missing store', async () => {
		expect(
			(
				await failure(
					generate(example(), { ...corpusOptions, ints: 'bigint' }),
				)
			).message,
		).toMatch(/ints/);
		expect((await failure(generate(example(), {}))).message).toMatch(
			/store/,
		);
	});

	it('refuses names that would collide, and accepts an explicit rename', async () => {
		const document = example();
		const person = document.tables.find(
			(t) => t.name === 'person',
		) as Record<string, unknown>;
		document.tables.push({ ...structuredClone(person), name: 'Person' });
		document.tables.sort((a, b) =>
			(a.name as string) < (b.name as string) ? -1 : 1,
		);
		const error = await failure(
			generate(withDigest(document), { ...corpusOptions }),
		);
		expect(error.message).toMatch(/would be generated for both/);
		const renamed = await run(
			generate(withDigest(document), {
				...corpusOptions,
				names: { Person: 'PersonUpper' },
			}),
		);
		expect(renamed.files.map((f) => f.path)).toContain('person.ts');
	});

	it('applies per-field presence overrides', async () => {
		const result = await run(
			generate(example(), {
				...corpusOptions,
				fields: { 'person.email': { create: 'optional' } },
			}),
		);
		const person =
			result.files.find((f) => f.path === 'person.ts')?.content ?? '';
		expect(person).toMatch(
			/email: \{ select: "required", create: "optional"/,
		);
	});

	it('reports kinds without an SDK value representation, and document warnings', async () => {
		const result = await run(
			generate(readJson(compiledPath('corpus')), { ...corpusOptions }),
		);
		expect(result.warnings).toEqual([
			'document: definition-duplicate (person flagged): defined 2 times (schema/04_fields.surql, schema/04_fields.surql); SurrealKit applies every DEFINE but a sequence as OVERWRITE, so the last one wins, which raw SurrealQL would not do for IF NOT EXISTS',
			'document: field-computed-kind-undeclared (person display): COMPUTED field has no TYPE clause; its value kind is unknown and is typed as `any`.',
			'document: field-computed-kind-undeclared (person referenced_by): COMPUTED field has no TYPE clause; its value kind is unknown and is typed as `any`.',
			'document: field-computed-kind-undeclared (team members): COMPUTED field has no TYPE clause; its value kind is unknown and is typed as `any`.',
			'document: function-optional-arg-not-trailing (fn::optional_first): Optional argument `$a` precedes the required argument `$b`; callers cannot omit it and must pass NONE explicitly.',
			'kinds.k_function: kind `function` has no SDK value representation; generated as SurrealSchema.Unsupported("function"), which rejects every value',
			'kinds.k_regex: kind `regex` has no SDK value representation; generated as SurrealSchema.Unsupported("regex"), which rejects every value',
		]);
	});

	it('refuses an id of a referenced table that collides with a defined table', async () => {
		// `org` is referenced by record<org | team> but not defined: its id
		// `OrgId` lives in ids.ts, and table `org_id` would export `OrgId` too.
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as Record<
			string,
			unknown
		>;
		document.tables.push({ ...structuredClone(team), name: 'org_id' });
		const error = await failure(
			generate(withDigest(document), { ...corpusOptions }),
		);
		expect(error.message).toMatch(
			/`OrgId` would be generated for both table `org_id` and the referenced table `org`/,
		);
		const renamed = await run(
			generate(withDigest(document), {
				...corpusOptions,
				names: { org_id: 'OrgKey' },
			}),
		);
		expect(renamed.files.map((f) => f.path)).toContain('org-key.ts');
	});

	it('fails with tagged errors', async () => {
		const document = example();
		at(document.tables, 0).surprise = true;
		expect(
			(await failure(generate(document, { ...corpusOptions })))._tag,
		).toBe('DocumentError');
		expect((await failure(generate(example(), {})))._tag).toBe(
			'OptionsError',
		);
		const edited = example();
		at(edited.tables, 0).comment = 'edited by hand';
		expect(
			(await failure(generate(edited, { ...corpusOptions }, { sha256 })))
				._tag,
		).toBe('GenerateError');
	});

	it('prints a parent without TYPE as the object its required children make it', async () => {
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as {
			fields: Array<Record<string, unknown>>;
		};
		const name = team.fields.find(
			(f) => (f.path as string[])[0] === 'name',
		) as Record<string, unknown>;
		const untyped: Record<string, unknown> = {
			...structuredClone(name),
			path: ['obj'],
		};
		delete untyped.kind;
		const child = {
			...structuredClone(name),
			path: ['obj', 'a'],
			kind: { kind: 'int' },
		};
		const loose = { ...structuredClone(untyped), path: ['loose'] };
		const looseChild = {
			...structuredClone(child),
			path: ['loose', 'b'],
			optional: true,
		};
		team.fields.push(untyped, child, loose, looseChild);
		team.fields.sort((x, y) =>
			JSON.stringify(x.path) < JSON.stringify(y.path) ? -1 : 1,
		);
		const result = await run(
			generate(withDigest(document), { ...corpusOptions }),
		);
		const module =
			result.files.find((f) => f.path === 'team.ts')?.content ?? '';
		// The engine rejects a missing or non-object `obj`, and `obj.b`.
		expect(module).toMatch(
			/obj: \{ select: "required", create: "required", merge: "optional", content: "required" \}/,
		);
		expect(module).toMatch(
			/obj: Schema\.Struct\(\{\n\t*a: SurrealSchema\.Int,/,
		);
		// Without a required child any value is accepted, and the engine
		// materialises it like an implicit parent.
		expect(module).toMatch(
			/loose: \{ select: "required", create: "optional", merge: "resettable", content: "optional" \}/,
		);
		expect(module).toMatch(/loose: Schema\.Unknown,/);
	});

	it('refuses a record<a | b> union name that collides with a table', async () => {
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as Record<
			string,
			unknown
		>;
		document.tables.push({ ...structuredClone(team), name: 'org_or_team' });
		const error = await failure(
			generate(withDigest(document), { ...corpusOptions }),
		);
		expect(error.message).toMatch(
			/`OrgOrTeamId` would be generated for both record<org \| team> and table `org_or_team`/,
		);
	});

	it('refuses two record unions that spell the same name', async () => {
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as Record<
			string,
			unknown
		>;
		const link = (name: string, tables: string[]) => ({
			path: [name],
			kind: { kind: 'record', tables },
			optional: false,
			flexible: false,
			readonly: false,
			inline: false,
			permissions: { select: true, create: true, update: true },
		});
		for (const name of ['a', 'a_or_b', 'b_or_c', 'c']) {
			document.tables.push({ ...structuredClone(team), name });
		}
		const a = document.tables.find((t) => t.name === 'a') as {
			fields: unknown[];
		};
		// AOrB + Or + C and A + Or + BOrC: both `AOrBOrCId`.
		a.fields.push(link('p', ['a_or_b', 'c']), link('q', ['a', 'b_or_c']));
		document.tables.sort((x, y) =>
			(x.name as string) < (y.name as string) ? -1 : 1,
		);
		const error = await failure(
			generate(withDigest(document), { ...corpusOptions }),
		);
		expect(error.message).toMatch(
			/`AOrBOrCId` would be generated for both record<a_or_b \| c> and record<a \| b_or_c>/,
		);
		expect(error.message).toMatch(/emitter options \(names\)/);
	});

	it('keeps datetimes in record keys as SDK values under the effect-datetime profile', async () => {
		const result = await run(
			generate(readJson(compiledPath('corpus')), {
				...corpusOptions,
				datetime: 'effect-datetime',
			}),
		);
		const ids =
			result.files.find((f) => f.path === 'ids.ts')?.content ?? '';
		expect(ids).toMatch(
			/IdArrayId = SurrealSchema\.RecordIdFor\("id_array", \{ key: Schema\.Tuple\(\[SurrealSchema\.String, SurrealSchema\.DateTime\]\) \}\)/,
		);
		expect(ids).not.toMatch(/DateTimeEffect/);
		// Fields still follow the profile.
		const modules = result.files.map((f) => f.content).join('\n');
		expect(modules).toMatch(/DateTimeEffect/);
	});

	it('generates a module set that type-checks for a schema without tables', async () => {
		const document = example();
		document.tables = [];
		document.functions = [];
		const result = await run(
			generate(withDigest(document), { store: 'empty' }),
		);
		const ids =
			result.files.find((f) => f.path === 'ids.ts')?.content ?? '';
		expect(ids.trimEnd().endsWith('export {};')).toBe(true);
		const dir = mkdtempSync(join(import.meta.dirname, 'tmp-empty-'));
		try {
			for (const file of result.files)
				writeFileSync(join(dir, file.path), file.content);
			writeFileSync(
				join(dir, 'tsconfig.json'),
				JSON.stringify({
					extends: '../types/tsconfig.json',
					include: ['./*.ts'],
				}),
			);
			const tsc = join(
				import.meta.dirname,
				'..',
				'node_modules',
				'typescript',
				'bin',
				'tsc',
			);
			const compiled = spawnSync(process.execPath, [tsc, '-p', dir], {
				encoding: 'utf8',
			});
			expect(`${compiled.stdout}${compiled.stderr}`).toBe('');
			expect(compiled.status).toBe(0);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('keeps `.js` imports by default and writes `.ts` ones on request', async () => {
		const document = readJson(compiledPath('corpus'));
		const plain = await run(generate(document, { ...corpusOptions }));
		const explicit = await run(
			generate(document, { ...corpusOptions, importExtension: '.js' }),
		);
		expect(explicit.files).toEqual(plain.files);
		const ts = await run(
			generate(document, { ...corpusOptions, importExtension: '.ts' }),
		);
		const index =
			ts.files.find((f) => f.path === 'index.ts')?.content ?? '';
		expect(index).toMatch(/export \* from "\.\/ids\.ts";/);
		const relative = ts.files.flatMap((f) =>
			[...f.content.matchAll(/from "(\.\/[^"]+)"/g)].map((m) => m[1]),
		);
		expect(relative.length).toBeGreaterThan(0);
		expect(relative.every((s) => s?.endsWith('.ts'))).toBe(true);
		expect(
			(
				await failure(
					generate(document, {
						...corpusOptions,
						importExtension: '.mjs',
					}),
				)
			).message,
		).toMatch(/importExtension/);
	});

	it('`.ts` imports typecheck with allowImportingTsExtensions and load under Node type stripping', async () => {
		const result = await run(
			generate(readJson(compiledPath('corpus')), {
				...corpusOptions,
				importExtension: '.ts',
			}),
		);
		const dir = mkdtempSync(join(import.meta.dirname, 'tmp-ts-ext-'));
		try {
			mkdirSync(join(dir, 'generated'));
			for (const file of result.files)
				writeFileSync(join(dir, 'generated', file.path), file.content);
			writeFileSync(
				join(dir, 'tsconfig.json'),
				JSON.stringify({
					extends: '../types/tsconfig.json',
					compilerOptions: { allowImportingTsExtensions: true },
					include: ['./generated/*.ts'],
				}),
			);
			const tsc = join(
				import.meta.dirname,
				'..',
				'node_modules',
				'typescript',
				'bin',
				'tsc',
			);
			const compiled = spawnSync('node', [tsc, '-p', dir], {
				encoding: 'utf8',
			});
			expect(`${compiled.stdout}${compiled.stderr}`).toBe('');
			expect(compiled.status).toBe(0);

			const [major = 0, minor = 0] = spawnSync(
				'node',
				['-p', 'process.versions.node'],
				{ encoding: 'utf8' },
			)
				.stdout.trim()
				.split('.')
				.map(Number);
			if (major < 22 || (major === 22 && minor < 6)) {
				// Node below 22.6 has no type stripping; nothing to load.
				return;
			}
			// The generated modules import the package by name: resolve it, as a
			// consumer does, to its built entry point.
			const pkg = join(import.meta.dirname, '..');
			const built = join(pkg, 'dist', 'index.js');
			const newest = (dir: string): number =>
				Math.max(
					...readdirSync(dir, { withFileTypes: true }).map((entry) =>
						entry.isDirectory()
							? newest(join(dir, entry.name))
							: statSync(join(dir, entry.name)).mtimeMs,
					),
				);
			if (
				!existsSync(built) ||
				statSync(built).mtimeMs < newest(join(pkg, 'src'))
			) {
				const built = spawnSync(
					'node',
					[
						join(
							pkg,
							'node_modules',
							'tsup',
							'dist',
							'cli-default.js',
						),
					],
					{ cwd: pkg, encoding: 'utf8' },
				);
				expect(built.status, built.stderr).toBe(0);
			}
			writeFileSync(
				join(dir, 'load.ts'),
				[
					"import { Schema } from 'effect';",
					"import { RecordId } from 'surrealdb';",
					"import { PersonId, manifest } from './generated/index.ts';",
					"const id: unknown = new RecordId('person', 'ann');",
					'Schema.decodeUnknownSync(PersonId)(id);',
					'console.log(manifest.store);',
				].join('\n'),
			);
			const flags =
				major === 22 || (major === 23 && minor < 6)
					? ['--experimental-strip-types']
					: [];
			const loaded = spawnSync(
				'node',
				[...flags, '--no-warnings', join(dir, 'load.ts')],
				{ encoding: 'utf8' },
			);
			expect(loaded.stderr).toBe('');
			expect(loaded.stdout.trim()).toBe('corpus');
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it('closes an object without declared children unless it is FLEXIBLE or its table SCHEMALESS', async () => {
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as {
			name: string;
			schemafull: boolean;
			fields: Array<Record<string, unknown>>;
		};
		const name = team.fields.find(
			(f) => (f.path as string[])[0] === 'name',
		) as Record<string, unknown>;
		team.fields.push(
			{
				...structuredClone(name),
				path: ['bare'],
				kind: { kind: 'object' },
			},
			{
				...structuredClone(name),
				path: ['flex'],
				kind: { kind: 'object' },
				flexible: true,
			},
		);
		team.fields.sort((x, y) =>
			JSON.stringify(x.path) < JSON.stringify(y.path) ? -1 : 1,
		);
		const loose = {
			...structuredClone(team),
			name: 'team_loose',
			schemafull: false,
		};
		document.tables.push(loose as unknown as Record<string, unknown>);
		document.tables.sort((x, y) =>
			(x.name as string) < (y.name as string) ? -1 : 1,
		);
		const result = await run(
			generate(withDigest(document), { ...corpusOptions }),
		);
		const module = (file: string) =>
			result.files.find((f) => f.path === file)?.content ?? '';
		expect(module('team.ts')).toMatch(
			/\tbare: Schema\.Record\(Schema\.String, Schema\.Never\),/,
		);
		expect(module('team.ts')).toMatch(
			/\tflex: Schema\.Record\(Schema\.String, Schema\.Unknown\),/,
		);
		expect(module('team.ts')).not.toMatch(
			/bare: Schema\.Record\(Schema\.String, Schema\.Unknown\)/,
		);
		expect(module('team-loose.ts')).toMatch(
			/\tbare: Schema\.Record\(Schema\.String, Schema\.Unknown\),/,
		);
		// The JSON world follows the same rule.
		expect(module('team.ts')).toMatch(
			/TeamJsonFields = \{[^}]*bare: Schema\.Record\(Schema\.String, Schema\.Never\)/,
		);
	});

	it('shares a nested object that is the same in every variant, and names the ones that are not', async () => {
		const document = example();
		const team = document.tables.find((t) => t.name === 'team') as {
			fields: Array<Record<string, unknown>>;
		};
		const name = team.fields.find(
			(f) => (f.path as string[])[0] === 'name',
		) as Record<string, unknown>;
		const field = (path: string[], extra: Record<string, unknown>) => ({
			...structuredClone(name),
			path,
			...extra,
		});
		team.fields.push(
			// READONLY: excluded from Merge and Content, so identical where used.
			field(['frozen'], { kind: { kind: 'object' }, readonly: true }),
			field(['frozen', 'kind'], { kind: { kind: 'string' } }),
			// Merge merges nested objects: its children are partial there.
			field(['after'], { kind: { kind: 'object' } }),
			field(['after', 'kind'], { kind: { kind: 'string' } }),
		);
		team.fields.sort((x, y) =>
			JSON.stringify(x.path) < JSON.stringify(y.path) ? -1 : 1,
		);
		const result = await run(
			generate(withDigest(document), { ...corpusOptions }),
		);
		const module =
			result.files.find((f) => f.path === 'team.ts')?.content ?? '';
		const fields = module.slice(
			module.indexOf('export const TeamFields'),
			module.indexOf('} as const;'),
		);
		expect(fields).toMatch(/frozen: Schema\.Struct\(\{/);
		expect(fields).not.toMatch(/after:/);
		expect(module).toMatch(
			/Not listed, because their schema differs by operation: `after` \(its children are partial in Merge\)\./,
		);

		// The shared nested object refines in every variant that uses it.
		const dir = mkdtempSync(join(import.meta.dirname, 'tmp-nested-'));
		try {
			for (const file of result.files)
				writeFileSync(join(dir, file.path), file.content);
			const { TeamTable } = (await import(join(dir, 'team.ts'))) as {
				TeamTable: TableDescriptor;
			};
			const Teams = refine(TeamTable, {
				frozen: (s: Schema.Top) =>
					(s as Schema.Struct<{ kind: Schema.String }>).check(
						Schema.makeFilter(({ kind }) => kind !== 'thawed'),
					),
			} as never) as TableDescriptor;
			const decodes = (schema: Schema.Top | undefined, value: unknown) =>
				Exit.isSuccess(
					Schema.decodeUnknownExit(
						schema as Schema.Codec<unknown, unknown>,
					)(value),
				);
			const create = (kind: string) => ({
				name: 'Core',
				frozen: { kind },
				after: { kind: 'x' },
			});
			const row = (kind: string) => ({
				id: new RecordId('team', 't'),
				...create(kind),
			});
			expect(decodes(Teams.select, row('frozen'))).toBe(true);
			expect(decodes(Teams.select, row('thawed'))).toBe(false);
			expect(decodes(Teams.create, create('thawed'))).toBe(false);
			expect(decodes(TeamTable.create, create('thawed'))).toBe(true);
			expect(() =>
				refine(TeamTable, { after: (s: Schema.Top) => s } as never),
			).toThrow(/`after` is not a shared field/);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});
});
