/**
 * `verifyStatements` and `statements --verify`: every complete text is sent
 * once, with no bindings, to a discarded in-memory engine, and a parse error
 * or a result count other than the analyzer's `slots` is a problem.
 *
 * The check always uses the embedded engine of `@surrealdb/node`, whatever
 * engine lane the suite runs on.
 */
import {
	cpSync,
	existsSync,
	mkdirSync,
	mkdtempSync,
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

	it('stops a slow text at the timeout and still counts its results', async () => {
		const started = performance.now();
		await passed({
			...scope({ slow: 'RETURN 1;\nsleep(5s);\nRETURN 2;' }),
			timeoutSeconds: 1,
		});
		expect(performance.now() - started).toBeLessThan(4_000);
		const { verifyAnalysed } = await import('../src/testing/verify.js');
		const { problems } = await Effect.runPromise(
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
		expect(problems.map((problem) => problem.message)).toEqual([
			'people/slow.surql: the engine returned 1 results, the analyzer derived 2',
		]);
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
