import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Effect, Exit } from 'effect';
import { describe, expect, it } from 'vitest';

import {
	type RenderStatementModulesInput,
	renderStatementModules,
	renderStatementModulesSync,
	StatementModulesError,
	type StatementScope,
} from '../src/analyze/index.js';
import { goldenDir } from './documents.js';

const fixtures = join(import.meta.dirname, 'fixtures', 'statements');
const golden = goldenDir('statements');

const fixtureScopes = (): ReadonlyArray<StatementScope> =>
	readdirSync(fixtures).map((scope) => ({
		name: scope,
		files: readdirSync(join(fixtures, scope)).map((file) => ({
			name: file.slice(0, -'.surql'.length),
			sql: readFileSync(join(fixtures, scope, file), 'utf8'),
		})),
	}));

const failure = (input: RenderStatementModulesInput): StatementModulesError => {
	try {
		renderStatementModulesSync(input);
	} catch (error) {
		if (error instanceof StatementModulesError) return error;
		throw error;
	}
	throw new Error('expected a StatementModulesError');
};

const one = (scope: string, name: string, sql = 'RETURN 1;') => ({
	scopes: [{ name: scope, files: [{ name, sql }] }],
});

describe('renderStatementModules', () => {
	it('renders the golden modules from the fixtures in memory', async () => {
		const { files, warnings } = await Effect.runPromise(
			renderStatementModules({ scopes: fixtureScopes() }),
		);
		expect(files.map((file) => file.path)).toEqual([
			'accounts/statements.ts',
			'counter/statements.ts',
			'index.ts',
		]);
		for (const file of files)
			expect(file.content).toBe(
				readFileSync(join(golden, file.path), 'utf8'),
			);
		expect(warnings).toEqual([]);
	});

	it('changes the index imports with importExtension', () => {
		const { files } = renderStatementModulesSync({
			scopes: fixtureScopes(),
			importExtension: '.ts',
		});
		expect(files.at(-1)?.content).toContain(
			'export * as counter from "./counter/statements.ts";',
		);
	});

	it('takes other hole delimiters', () => {
		const { files } = renderStatementModulesSync({
			...one('a', 'filter', 'SELECT * FROM t WHERE {{WHERE}};'),
			holes: { open: '{{', close: '}}' },
		});
		expect(files[0]?.content).toContain('holes: ["{{WHERE}}"]');
	});

	it('returns warnings with scope, file, line and column', () => {
		const { warnings } = renderStatementModulesSync(
			one('counter', 'dynamic', "THROW string::concat('x', $y);\n"),
		);
		expect(warnings).toMatchObject([
			{
				scope: 'counter',
				file: 'dynamic',
				line: 1,
				column: 1,
				code: 'dynamicThrow',
			},
		]);
		expect(warnings[0]?.text).toMatch(
			/^counter\/dynamic\.surql:1:1: .*\(dynamicThrow\)$/,
		);
	});

	it('fails in the error channel with a file the analyzer rejects', async () => {
		const exit = await Effect.runPromiseExit(
			renderStatementModules(one('counter', 'broken', "RETURN 'open;\n")),
		);
		expect(Exit.isFailure(exit)).toBe(true);
		const error = failure(one('counter', 'broken', "RETURN 'open;\n"));
		expect(error._tag).toBe('StatementModulesError');
		expect(error.problems).toMatchObject([
			{
				kind: 'analysis',
				scope: 'counter',
				file: 'broken',
				line: 1,
				column: 8,
			},
		]);
		expect(error.message).toMatch(
			/^counter\/broken\.surql:1:8: unterminated string/,
		);
	});

	it('fails on an error diagnostic', () => {
		const error = failure(one('counter', 'spoof', 'LET $auth = 1;\n'));
		expect(error.problems).toMatchObject([
			{ kind: 'analysis', code: 'protectedLet', line: 1, column: 5 },
		]);
	});

	it.each([
		['class', /reserved word/],
		['not-a-name', /not a JavaScript identifier/],
	])('fails on the file name %s', (name, reason) => {
		const error = failure(one('counter', name));
		expect(error.problems).toMatchObject([
			{ kind: 'name', scope: 'counter', file: name },
		]);
		expect(error.problems[0]?.message).toMatch(reason);
	});

	it('fails on names that differ only in case within a scope', () => {
		const error = failure({
			scopes: [
				{
					name: 'a',
					files: [
						{ name: 'step', sql: 'RETURN 1;' },
						{ name: 'Step', sql: 'RETURN 1;' },
					],
				},
				{ name: 'b', files: [{ name: 'step', sql: 'RETURN 1;' }] },
			],
		});
		expect(error.message).toBe(
			'a/step.surql: file name "step" differs only in case from "Step"',
		);
		expect(error.problems).toMatchObject([
			{ kind: 'name', scope: 'a', file: 'step' },
		]);
	});

	it.each([
		['not-a-scope', /not a JavaScript identifier/],
		['default', /reserved word/],
	])('fails on the scope name %s', (scope, reason) => {
		const error = failure(one(scope, 'x'));
		expect(error.problems).toMatchObject([{ kind: 'scope', scope }]);
		expect(error.problems[0]?.file).toBeUndefined();
		expect(error.message).toMatch(reason);
	});

	it('reports every problem at once', () => {
		const error = failure({
			scopes: [
				{
					name: 'a',
					files: [
						{ name: 'class', sql: 'RETURN 1;' },
						{ name: 'broken', sql: "RETURN 'open;" },
					],
				},
			],
		});
		expect(error.problems.map((problem) => problem.kind)).toEqual([
			'analysis',
			'name',
		]);
	});
});
