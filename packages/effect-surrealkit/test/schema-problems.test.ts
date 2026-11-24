/**
 * `schemaProblems`: the checks `renderStatementModules` makes against a
 * scope's `declared` names, over named texts, rendering nothing. The rules
 * themselves are pinned in `test/projections.test.ts`, `test/fields.test.ts`
 * and `test/uses.test.ts`; this suite holds the function to the same check
 * and to its own contract (any non-empty name, refusals as problems, a
 * stable order).
 */
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import {
	type DeclaredNames,
	renderStatementModulesSync,
	type SchemaProblem,
	StatementModulesError,
	type StatementScope,
	schemaProblems,
} from '../src/analyze/index.js';
import { declaredNames, decodeDocument } from '../src/emitter/index.js';
import { readJson } from './documents.js';

const bindsDocument = join(import.meta.dirname, 'fixtures', 'binds.v2.json');
const declared = async (): Promise<DeclaredNames> =>
	declaredNames(
		await Effect.runPromise(decodeDocument(readJson(bindsDocument))),
	);

const texts = (problems: ReadonlyArray<SchemaProblem>): ReadonlyArray<string> =>
	problems.map((problem) => problem.text);

describe('schemaProblems', () => {
	it('reports a misspelt projected field, with the text name and line', async () => {
		const problems = schemaProblems(
			[{ name: 'q', sql: 'SELECT station,\n\tstaton FROM gauge;' }],
			await declared(),
		);
		expect(problems).toEqual([
			{
				name: 'q',
				line: 2,
				code: 'undeclaredField',
				message:
					'selects `staton` of `gauge`, which declares no such field',
				text: 'q:2: selects `staton` of `gauge`, which declares no such field (undeclaredField)',
			},
		]);
	});

	it('reports a misspelt compared field', async () => {
		expect(
			texts(
				schemaProblems(
					[
						{
							name: 'q',
							sql: "SELECT * FROM gauge WHERE statoin = 'a';",
						},
					],
					await declared(),
				),
			),
		).toEqual([
			'q:1: compares `statoin` of `gauge`, which declares no such field (undeclaredField)',
		]);
	});

	it('reports a binding used as a field or argument that does not resolve', async () => {
		expect(
			texts(
				schemaProblems(
					[
						{
							name: 'q',
							sql: 'SELECT * FROM gauge WHERE statoin = $s;\nRETURN fn::scaled($a, $b);',
						},
					],
					await declared(),
				),
			),
		).toEqual([
			'q:1: compares `statoin` of `gauge`, which declares no such field (undeclaredField)',
			'q:2: passes argument 2 to `fn::scaled`, which takes 1 argument (extraArgument)',
		]);
	});

	it('reports an undeclared table and function', async () => {
		expect(
			texts(
				schemaProblems(
					[
						{
							name: 'q',
							sql: 'SELECT * FROM shipment;\nRETURN fn::missing();',
						},
					],
					await declared(),
				),
			),
		).toEqual([
			'q:1: reads table `shipment`, which the schema does not declare (undeclaredTable)',
			'q:2: calls function `fn::missing`, which the schema does not declare (undeclaredFunction)',
		]);
	});

	it('checks names alone without schema detail', async () => {
		const { detail: _, ...names } = await declared();
		expect(
			texts(
				schemaProblems(
					[
						{
							name: 'q',
							sql: 'SELECT nope FROM gauge;\nSELECT * FROM shipment;',
						},
					],
					names,
				),
			),
		).toEqual([
			'q:2: reads table `shipment`, which the schema does not declare (undeclaredTable)',
		]);
	});

	it('takes any non-empty name, an identifier or not', async () => {
		const names = await declared();
		const sql = 'SELECT staton FROM gauge;';
		const problems = schemaProblems(
			[
				{ name: 'billing.openInvoices', sql },
				{ name: 'retrieval/graph/depth-1', sql },
				{ name: 'class', sql },
				{ name: 'a b ü', sql },
			],
			names,
		);
		expect(problems.map((problem) => problem.name)).toEqual([
			'a b ü',
			'billing.openInvoices',
			'class',
			'retrieval/graph/depth-1',
		]);
		// The renderer refuses three of these names, and checks nothing in them.
		expect(() =>
			renderStatementModulesSync({
				scopes: [
					{
						name: 's',
						files: [{ name: 'billing.openInvoices', sql }],
						declared: names,
					},
				],
			}),
		).toThrow(/not a JavaScript identifier/u);
	});

	it('reports an empty name as a problem and checks the other texts', async () => {
		expect(
			texts(
				schemaProblems(
					[
						{ name: '', sql: 'SELECT * FROM shipment;' },
						{ name: 'q', sql: 'SELECT * FROM shipment;' },
					],
					await declared(),
				),
			),
		).toEqual([
			':1: a text has no name (unnamed)',
			'q:1: reads table `shipment`, which the schema does not declare (undeclaredTable)',
		]);
	});

	it('returns a text the analyzer refuses as a problem, without hiding the others', async () => {
		const problems = schemaProblems(
			[
				{ name: 'broken', sql: "SELECT 'unterminated FROM gauge;" },
				{ name: 'empty', sql: '-- nothing\n' },
				{ name: 'q', sql: 'SELECT staton FROM gauge;' },
			],
			await declared(),
		);
		expect(problems.map(({ name, code }) => [name, code])).toEqual([
			['broken', expect.any(String)],
			['empty', 'empty'],
			['q', 'undeclaredField'],
		]);
		const [broken] = problems;
		expect(broken?.line).toBe(1);
		expect(broken?.column).toBeTypeOf('number');
		expect(broken?.text).toMatch(/^broken:1:\d+: /u);
	});

	it('reports an error diagnostic as the renderer does', async () => {
		expect(
			schemaProblems(
				[{ name: 'q', sql: 'LET $auth = 1;' }],
				await declared(),
			).map(({ name, line, column, code }) => ({
				name,
				line,
				column,
				code,
			})),
		).toEqual([
			{ name: 'q', line: 1, column: 5, code: expect.any(String) },
		]);
	});

	describe('a template', () => {
		it('is checked as written, never filled or guessed', async () => {
			const names = await declared();
			expect(
				texts(
					schemaProblems(
						[
							{
								name: 'q',
								sql: 'SELECT nope FROM gauge WHERE __FILTER__;',
							},
						],
						names,
					),
				),
			).toEqual([
				'q:1: selects `nope` of `gauge`, which declares no such field (undeclaredField)',
			]);
			// A hole that starts a statement can be filled with one that
			// defines a field: no field is checked, the names still are.
			expect(
				texts(
					schemaProblems(
						[
							{
								name: 'q',
								sql: '__SETUP__;\nSELECT nope FROM gauge;\nSELECT * FROM shipment;',
							},
						],
						names,
					),
				),
			).toEqual([
				'q:3: reads table `shipment`, which the schema does not declare (undeclaredTable)',
			]);
			// A table behind a hole is an open target: nothing is recorded.
			expect(
				schemaProblems(
					[{ name: 'q', sql: 'SELECT nope FROM __TABLE__;' }],
					names,
				),
			).toEqual([]);
		});

		it('takes other hole delimiters', async () => {
			expect(
				texts(
					schemaProblems(
						[
							{
								name: 'q',
								sql: '{{SETUP}};\nSELECT nope FROM gauge;',
							},
						],
						await declared(),
						{ holes: { open: '{{', close: '}}' } },
					),
				),
			).toEqual([]);
		});
	});

	it('returns nothing for no texts, and for texts that hold', async () => {
		expect(schemaProblems([], await declared())).toEqual([]);
		expect(
			schemaProblems(
				[{ name: 'q', sql: 'SELECT station, reading FROM gauge;' }],
				await declared(),
			),
		).toEqual([]);
	});

	it('orders problems by name, line and column, without duplicates, whatever the input order', async () => {
		const names = await declared();
		const input = [
			{
				name: 'b',
				sql: 'SELECT staton FROM gauge;\nSELECT * FROM shipment;',
			},
			{ name: 'a', sql: 'SELECT * FROM shipment;' },
			{ name: 'a', sql: 'SELECT * FROM shipment;' },
		];
		const forward = schemaProblems(input, names);
		expect(texts(forward)).toEqual([
			'a:1: reads table `shipment`, which the schema does not declare (undeclaredTable)',
			'b:1: selects `staton` of `gauge`, which declares no such field (undeclaredField)',
			'b:2: reads table `shipment`, which the schema does not declare (undeclaredTable)',
		]);
		expect(schemaProblems([...input].reverse(), names)).toEqual(forward);
	});
});

/** Every fixture scope of the renderer's suites, as the renderer reads it. */
const fixtureScopes = (): ReadonlyArray<StatementScope> =>
	['statements', 'statements-binds', 'statements-presence'].flatMap((set) => {
		const root = join(import.meta.dirname, 'fixtures', set);
		return readdirSync(root).map((scope) => ({
			name: scope,
			files: readdirSync(join(root, scope)).map((file) => ({
				name: file.slice(0, -'.surql'.length),
				sql: readFileSync(join(root, scope, file), 'utf8'),
			})),
		}));
	});

/** Texts whose problems exercise every check, as one extra scope. */
const probes: StatementScope = {
	name: 'probes',
	files: [
		{ name: 'projected', sql: 'SELECT station,\n\tstaton FROM gauge;' },
		{ name: 'compared', sql: "SELECT * FROM gauge WHERE statoin = 'a';" },
		{ name: 'bound', sql: 'SELECT * FROM gauge WHERE statoin = $s;' },
		{ name: 'argument', sql: 'RETURN fn::scaled($a, $b);' },
		{ name: 'assigned', sql: "UPDATE gauge SET nte = 'x';" },
		{
			name: 'names',
			sql: 'SELECT * FROM shipment;\nRETURN fn::missing();',
		},
		{ name: 'template', sql: 'SELECT nope FROM gauge WHERE __FILTER__;' },
		{ name: 'setup', sql: '__SETUP__;\nSELECT nope FROM gauge;' },
		{
			name: 'defines',
			sql: 'DEFINE FIELD extra ON gauge TYPE string;\nSELECT extra FROM gauge;',
		},
		{ name: 'refused', sql: "SELECT 'unterminated FROM gauge;" },
		{ name: 'diagnostic', sql: 'LET $auth = 1;' },
	],
};

const rendererProblems = (
	scopes: ReadonlyArray<StatementScope>,
): ReadonlyArray<string> => {
	try {
		renderStatementModulesSync({ scopes });
	} catch (error) {
		if (error instanceof StatementModulesError)
			return error.problems
				.map(
					(problem) =>
						`${problem.scope}/${problem.file}:${problem.line}:${problem.code === undefined ? '' : problem.column}: ${problem.message}`,
				)
				.sort();
		throw error;
	}
	return [];
};

const functionProblems = (
	scopes: ReadonlyArray<StatementScope>,
	names: DeclaredNames,
): ReadonlyArray<string> =>
	schemaProblems(
		scopes.flatMap((scope) =>
			scope.files.map((file) => ({
				name: `${scope.name}/${file.name}`,
				sql: file.sql,
			})),
		),
		names,
	)
		.map(
			(problem) =>
				`${problem.name}:${problem.line}:${problem.column ?? ''}: ${problem.message}`,
		)
		.sort();

describe('schemaProblems and renderStatementModules', () => {
	it('report the same problems over every fixture, with each declared value', async () => {
		const full = await declared();
		const { detail: _, ...namesOnly } = full;
		const nothing: DeclaredNames = { tables: [], functions: [] };
		const scopes = [...fixtureScopes(), probes];
		for (const names of [full, namesOnly, nothing]) {
			const expected = rendererProblems(
				scopes.map((scope) => ({ ...scope, declared: names })),
			);
			expect(expected.length).toBeGreaterThan(0);
			expect(functionProblems(scopes, names)).toEqual(expected);
		}
	});
});
