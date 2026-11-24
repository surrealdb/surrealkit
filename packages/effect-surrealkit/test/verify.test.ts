/**
 * `verifyStatements` and `statements --verify`: every complete text is sent
 * once, with no bindings, to a discarded in-memory engine, and a parse error,
 * a result count other than the analyzer's `slots` or a text that does not
 * finish within the timeout is a problem.
 *
 * The check always uses the embedded engine of `@surrealdb/node`, whatever
 * engine lane the suite runs on.
 */
import {
	cpSync,
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Effect, Exit } from 'effect';
import { afterEach, describe, expect, it } from 'vitest';

import { run } from '../src/cli/run.js';
import { STATEMENTS_USAGE } from '../src/cli/statements.js';
import {
	StatementVerificationError,
	type Verification,
	type VerifyInput,
	verifyStatements,
} from '../src/testing/index.js';

const verify = (input: VerifyInput) =>
	Effect.runPromise(Effect.result(verifyStatements(input)));

const passed = async (input: VerifyInput): Promise<Verification> => {
	const result = await verify(input);
	if (result._tag === 'Failure') throw result.failure;
	return result.success;
};

const problemsOf = async (input: VerifyInput) => {
	const result = await verify(input);
	if (result._tag === 'Success')
		throw new Error('expected the verification to fail');
	expect(result.failure).toBeInstanceOf(StatementVerificationError);
	return (result.failure as StatementVerificationError).problems;
};

const scope = (files: Record<string, string>) => ({
	scopes: [
		{
			name: 'people',
			files: Object.entries(files).map(([name, sql]) => ({ name, sql })),
		},
	],
});

/** Whether the embedded engine starts here; the suite skips without it. */
const embedded = await Effect.runPromise(
	Effect.exit(verifyStatements({ scopes: [] })),
).then(Exit.isSuccess);

if (!embedded) {
	console.warn(
		'[effect-surrealkit] verify suite skipped: the embedded engine of `@surrealdb/node` did not start',
	);
}

describe.skipIf(!embedded)('verifyStatements', () => {
	it('sends each complete text and reports the engine it used', async () => {
		const verification = await passed(
			scope({
				adults: 'SELECT * FROM person WHERE age >= $minimum;',
				rename: 'LET $p = SELECT * FROM ONLY $id;\nUPDATE $id SET name = $name;\nRETURN $p;',
			}),
		);
		expect(verification.sent).toEqual([
			'people/adults.surql',
			'people/rename.surql',
		]);
		expect(verification.notSent).toEqual([]);
		expect(verification.engine).toMatch(/^\d+\.\d+\.\d+/u);
	});

	it('reports a parse error with the file, line and column', async () => {
		const problems = await problemsOf(
			scope({
				fine: 'SELECT * FROM person;',
				typo: 'SELECT * FORM person;',
				later: 'SELECT * FROM person;\nSELECT name FROM person WHERE;',
			}),
		);
		expect(problems.map((problem) => problem.message)).toEqual([
			expect.stringMatching(
				/^people\/later\.surql:2:30: Parse error: Unexpected token `;`/u,
			),
			expect.stringMatching(
				/^people\/typo\.surql:1:10: Parse error: Unexpected token .*expected FROM$/u,
			),
		]);
		expect(problems[1]).toMatchObject({
			file: 'people/typo.surql',
			line: 1,
			column: 10,
		});
	});

	it('is not a problem when a statement errors for lack of bindings', async () => {
		await passed(
			scope({
				create: 'CREATE $id SET name = $name;\nRETURN fn::missing($name);',
				throws: "IF $x = NONE { THROW 'no x' };\nRETURN $x;",
			}),
		);
	});

	it('reports a result count other than the derived slots', async () => {
		const { verifyAnalysed } = await import('../src/testing/verify.js');
		const problems = await Effect.runPromise(
			Effect.scoped(
				verifyAnalysed(
					[
						{
							file: 'people/three.surql',
							sql: 'RETURN 1; RETURN 2; RETURN 3;',
							slots: 2,
						},
					],
					{},
				),
			),
		);
		expect(problems.problems.map((problem) => problem.message)).toEqual([
			'people/three.surql: the engine returned 3 results, the analyzer derived 2',
		]);
	});

	it('names the fragments and templates it does not send', async () => {
		const verification = await passed(
			scope({
				filter: 'age >= $minimum',
				list: 'SELECT * FROM person WHERE __FILTER__;',
				all: 'SELECT * FROM person;',
			}),
		);
		expect(verification.sent).toEqual(['people/all.surql']);
		expect(verification.notSent).toEqual([
			'people/filter.surql',
			'people/list.surql',
		]);
	});

	it('loads the schema files first, in order, and reports one that fails', async () => {
		await passed({
			...scope({ older: 'RETURN fn::older($a, $b);' }),
			schema: [
				{
					name: 'person.surql',
					sql: 'DEFINE TABLE person SCHEMAFULL;\nDEFINE FIELD age ON person TYPE int;',
				},
				{
					name: 'functions.surql',
					sql: 'DEFINE FUNCTION fn::older($a: int, $b: int) { RETURN $a > $b; };',
				},
			],
		});
		const parse = await problemsOf({
			...scope({ all: 'SELECT * FROM person;' }),
			schema: [{ name: 'broken.surql', sql: 'DEFINE TABLE;' }],
		});
		expect(parse.map((problem) => problem.message)).toEqual([
			expect.stringMatching(/^broken\.surql:1:13: Parse error: /u),
		]);
		const applied = await problemsOf({
			...scope({ all: 'SELECT * FROM person;' }),
			schema: [
				{
					name: 'twice.surql',
					sql: 'DEFINE TABLE person;\nDEFINE TABLE person;',
				},
			],
		});
		expect(applied.map((problem) => problem.message)).toEqual([
			expect.stringMatching(
				/^twice\.surql: statement 2 failed: The table 'person' already exists$/u,
			),
		]);
	});

	it('denies the network: an http:: call reaches nothing and still counts', async () => {
		let requests = 0;
		const server = createServer((_request, response) => {
			requests++;
			response.end('{}');
		});
		await new Promise<void>((done) =>
			server.listen(0, '127.0.0.1', () => done()),
		);
		try {
			const address = server.address();
			if (address === null || typeof address === 'string')
				throw new Error('no port');
			const url = `http://127.0.0.1:${address.port}/`;
			await passed(
				scope({
					post: `RETURN http::post('${url}', {});\nRETURN http::get('${url}');\nRETURN 1;`,
				}),
			);
			expect(requests).toBe(0);
		} finally {
			await new Promise((done) => server.close(done));
		}
	});

	/** The messages of a schema file the engine refused. */
	const schemaRefusal = async (sql: string) =>
		(
			await problemsOf({ scopes: [], schema: [{ name: 's.surql', sql }] })
		).map((problem) => problem.message);

	// The @surrealdb/node build has no script runtime, so this holds whatever
	// the scripting capability says; the capability is denied as well.
	it('denies embedded scripts: a script function is refused and still counts', async () => {
		await passed(
			scope({ script: 'RETURN function() { return 1; };\nRETURN 1;' }),
		);
		expect(await schemaRefusal('RETURN function() { return 1; };')).toEqual(
			[
				expect.stringMatching(
					/^s\.surql: .*Embedded functions are not enabled/u,
				),
			],
		);
	});

	it('refuses a file bucket: nothing is written to the folder, and it still counts', async () => {
		const folder = mkdtempSync(join(tmpdir(), 'effect-surrealkit-bucket-'));
		try {
			const bucket = `DEFINE BUCKET files BACKEND "file:${folder}";`;
			await passed(
				scope({
					bucket: `${bucket}\nRETURN f"files:/x.txt".put("hi");\nRETURN 1;`,
				}),
			);
			expect(await schemaRefusal(bucket)).toEqual([
				expect.stringMatching(/^s\.surql: .*File access denied/u),
			]);
			expect(readdirSync(folder)).toEqual([]);
		} finally {
			rmSync(folder, { recursive: true, force: true });
		}
	});

	it('reports a text that does not finish within the timeout as a timeout problem', async () => {
		const timeout = (file: string) => ({
			kind: 'timeout',
			file,
			text: 'the text did not finish within 1s on an empty database with no bindings, so its result count was not verified',
			message: `${file}: the text did not finish within 1s on an empty database with no bindings, so its result count was not verified`,
		});
		// The engine reports this text's timeout either per statement or as
		// a refusal of the whole text; the outcome is the same either way.
		const problems = await problemsOf({
			...scope({
				fine: 'RETURN 1;',
				slow: 'RETURN 1;\nsleep(5s);\nRETURN 2;',
			}),
			timeoutSeconds: 1,
		});
		expect(problems).toEqual([timeout('people/slow.surql')]);
		const { verifyAnalysed } = await import('../src/testing/verify.js');
		const { problems: miscounted } = await Effect.runPromise(
			Effect.scoped(
				verifyAnalysed(
					[
						{
							file: 'people/slow.surql',
							sql: 'sleep(5s);',
							slots: 2,
						},
					],
					{ timeoutSeconds: 1 },
				),
			),
		);
		expect(miscounted).toEqual([timeout('people/slow.surql')]);
	});
});

describe('judgeText', () => {
	/** The error the embedded engine reports for a query timeout. */
	const timedOut = (code: number) =>
		Object.assign(
			new Error(
				'The query was not executed because it exceeded the timeout: 1s',
			),
			{
				name: 'QueryError',
				kind: 'Query',
				code,
				details: { kind: 'TimedOut', details: { duration: '1s' } },
			},
		);
	const text = { file: 'people/slow.surql', slots: 3 };
	const expected = {
		kind: 'timeout',
		file: 'people/slow.surql',
		message:
			'people/slow.surql: the text did not finish within 1s on an empty database with no bindings, so its result count was not verified',
	};

	it('maps both ways the engine reports one timeout to one timeout problem', async () => {
		const { judgeText } = await import('../src/testing/verify.js');
		const perStatement = judgeText(
			text,
			{
				responses: [
					{ success: true },
					{ success: false, error: timedOut(0) },
					{ success: false, error: timedOut(0) },
				],
			},
			1,
		);
		const wholeText = judgeText(text, { refused: timedOut(-32004) }, 1);
		const wrapped = judgeText(
			text,
			{
				refused: new Error('request failed', {
					cause: timedOut(-32004),
				}),
			},
			1,
		);
		expect(perStatement).toMatchObject(expected);
		expect(wholeText).toEqual(perStatement);
		expect(wrapped).toEqual(perStatement);
	});

	it('reports a timeout even when the count would not match', async () => {
		const { judgeText } = await import('../src/testing/verify.js');
		expect(
			judgeText(
				text,
				{ responses: [{ success: false, error: timedOut(0) }] },
				1,
			),
		).toMatchObject(expected);
	});

	it('reads the structure, not the message', async () => {
		const { judgeText } = await import('../src/testing/verify.js');
		const message = new Error(
			'The query was not executed because it exceeded the timeout: 1s',
		);
		expect(judgeText(text, { refused: message }, 1)).toMatchObject({
			kind: 'refused',
		});
		expect(
			judgeText(
				{ ...text, slots: 1 },
				{ responses: [{ success: false, error: message }] },
				1,
			),
		).toBeUndefined();
		expect(
			judgeText(
				text,
				{ refused: new Error('Parse error: x --> [1:2]') },
				1,
			),
		).toMatchObject({ kind: 'parse', line: 1, column: 2 });
	});
});

const fixtures = join(import.meta.dirname, 'fixtures', 'statements');

const dirs: string[] = [];
const tempDir = () => {
	const dir = mkdtempSync(join(tmpdir(), 'effect-surrealkit-verify-'));
	dirs.push(dir);
	return dir;
};
afterEach(() => {
	for (const dir of dirs.splice(0))
		rmSync(dir, { recursive: true, force: true });
});

const cli = async (argv: string[], cwd: string) => {
	let stdout = '';
	let stderr = '';
	const code = await run(['statements', ...argv], {
		cwd,
		stdin: async () => '',
		stdout: (text) => {
			stdout += text;
		},
		stderr: (text) => {
			stderr += text;
		},
	});
	return { code, stdout, stderr };
};

/** The fixtures plus one scope whose only file does not parse. */
const brokenInput = () => {
	const dir = tempDir();
	cpSync(fixtures, join(dir, 'queries'), { recursive: true });
	mkdirSync(join(dir, 'queries', 'broken'));
	writeFileSync(
		join(dir, 'queries', 'broken', 'typo.surql'),
		'SELECT * FORM person;\n',
	);
	return dir;
};

describe('statements --verify', () => {
	it('is in the usage text', () => {
		expect(STATEMENTS_USAGE).toContain('--verify ');
		expect(STATEMENTS_USAGE).toContain('--verify-schema <file>');
	});

	it('refuses --verify-schema without --verify', async () => {
		const dir = tempDir();
		const result = await cli(
			[
				'--input',
				fixtures,
				'--out',
				join(dir, 'out'),
				'--verify-schema',
				'schema.surql',
			],
			dir,
		);
		expect(result.code).toBe(2);
		expect(result.stderr).toContain('--verify-schema needs --verify');
	});

	it.skipIf(!embedded)('generates when every text passes', async () => {
		const dir = tempDir();
		const result = await cli(
			['--input', fixtures, '--out', join(dir, 'out'), '--verify'],
			dir,
		);
		expect(result.code, result.stderr).toBe(0);
		expect(result.stderr).toMatch(
			/verified \d+ texts on SurrealDB \d+\.\d+\.\d+ \(\d+ fragments and templates not sent\)/u,
		);
		expect(existsSync(join(dir, 'out', 'index.ts'))).toBe(true);
	});

	it.skipIf(!embedded)(
		'writes nothing when a text does not parse',
		async () => {
			const dir = brokenInput();
			const result = await cli(
				['--input', 'queries', '--out', 'out', '--verify'],
				dir,
			);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('statements not verified');
			expect(result.stderr).toMatch(
				/ {2}broken\/typo\.surql:1:10: Parse error: /u,
			);
			expect(existsSync(join(dir, 'out'))).toBe(false);
		},
	);

	it.skipIf(!embedded)(
		'combines with --check and writes nothing',
		async () => {
			const dir = brokenInput();
			const broken = await cli(
				['--input', 'queries', '--out', 'out', '--check', '--verify'],
				dir,
			);
			expect(broken.code).toBe(1);
			expect(broken.stderr).toMatch(/broken\/typo\.surql:1:10: /u);
			rmSync(join(dir, 'queries', 'broken'), { recursive: true });
			const fresh = await cli(
				['--input', 'queries', '--out', 'out', '--check', '--verify'],
				dir,
			);
			expect(fresh.code).toBe(1);
			expect(fresh.stderr).toContain('is not up to date');
			expect(existsSync(join(dir, 'out'))).toBe(false);
			expect(
				(await cli(['--input', 'queries', '--out', 'out'], dir)).code,
			).toBe(0);
			const current = await cli(
				['--input', 'queries', '--out', 'out', '--check', '--verify'],
				dir,
			);
			expect(current.code, current.stderr).toBe(0);
			expect(current.stderr).toMatch(/verified \d+ texts/u);
		},
	);

	it.skipIf(!embedded)(
		'loads --verify-schema files in the order given',
		async () => {
			const dir = tempDir();
			mkdirSync(join(dir, 'queries', 'people'), { recursive: true });
			writeFileSync(
				join(dir, 'queries', 'people', 'older.surql'),
				'RETURN fn::older($a, $b);\n',
			);
			writeFileSync(
				join(dir, 'tables.surql'),
				'DEFINE TABLE person SCHEMAFULL;\n',
			);
			writeFileSync(join(dir, 'bad.surql'), 'DEFINE TABLE;\n');
			const ok = await cli(
				[
					'--input',
					'queries',
					'--out',
					'out',
					'--verify',
					'--verify-schema',
					'tables.surql',
				],
				dir,
			);
			expect(ok.code, ok.stderr).toBe(0);
			const bad = await cli(
				[
					'--input',
					'queries',
					'--out',
					'out2',
					'--verify',
					'--verify-schema',
					'tables.surql',
					'--verify-schema',
					'bad.surql',
				],
				dir,
			);
			expect(bad.code).toBe(1);
			expect(bad.stderr).toMatch(/ {2}bad\.surql:1:13: Parse error: /u);
			expect(existsSync(join(dir, 'out2'))).toBe(false);
		},
	);
});

describe('the analyze entry stays host-neutral', () => {
	interface Built {
		readonly outputFiles: ReadonlyArray<{ readonly text: string }>;
	}
	// esbuild, as tsup resolves it, with the options tsup.config.ts builds
	// `dist/analyze.js` with.
	const fromTsup = createRequire(
		createRequire(import.meta.url).resolve('tsup/package.json'),
	);
	const { build } = fromTsup('esbuild') as {
		readonly build: (options: Record<string, unknown>) => Promise<Built>;
	};

	it('does not import @surrealdb/node or a Node built-in', async () => {
		const result = await build({
			entryPoints: [
				join(import.meta.dirname, '..', 'src', 'analyze', 'index.ts'),
			],
			bundle: true,
			write: false,
			format: 'esm',
			platform: 'neutral',
			target: 'es2022',
			external: ['effect', 'surrealdb', '@surrealdb/node'],
			logLevel: 'silent',
		});
		const text = result.outputFiles.map((file) => file.text).join('\n');
		expect(text).not.toContain('@surrealdb/node');
		expect(text).not.toMatch(/from\s*["']node:|import\(\s*["']node:/u);
		expect(text).not.toContain('verifyStatements');
	});
});
