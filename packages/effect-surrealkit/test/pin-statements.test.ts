import { Schema } from 'effect';
import { describe, expect, it } from 'vitest';
import {
	type PinnableStatement,
	PinnedStatementsError,
	pinnedStatementProblems,
	renderPinnedStatements,
} from '../src/analyze/index.js';
import * as SurrealSchema from '../src/index.js';
import { gte, param, part, select, sequence } from '../src/statement/index.js';
import { PersonTable } from './golden/corpus/index.js';

const failure = (
	statements: ReadonlyArray<PinnableStatement>,
): PinnedStatementsError => {
	try {
		renderPinnedStatements(statements);
	} catch (error) {
		if (error instanceof PinnedStatementsError) return error;
		throw error;
	}
	throw new Error('expected a PinnedStatementsError');
};

const ledger = [
	{ name: 'ledger.openEntries', sql: 'SELECT * FROM entry WHERE open;' },
	{ name: 'audit', sql: 'RETURN 1;' },
];

describe('renderPinnedStatements', () => {
	it('pins each statement at its dotted name with its sql and one newline', () => {
		expect(
			renderPinnedStatements([
				...ledger,
				{ name: 'ledger.v-2.close_all', sql: 'RETURN 2;\n' },
			]),
		).toEqual([
			{ path: 'audit.surql', content: 'RETURN 1;\n' },
			{
				path: 'ledger/openEntries.surql',
				content: 'SELECT * FROM entry WHERE open;\n',
			},
			{ path: 'ledger/v-2/close_all.surql', content: 'RETURN 2;\n\n' },
		]);
	});

	it('rejects a statement without a name', () => {
		const error = failure([{ name: '', sql: 'RETURN 1;' }]);
		expect(error._tag).toBe('PinnedStatementsError');
		expect(error.problems.map((problem) => problem.kind)).toEqual([
			'unnamed',
		]);
	});

	it('rejects two statements with the same name', () => {
		const error = failure([...ledger, ledger[1] as PinnableStatement]);
		expect(error.problems).toMatchObject([
			{ kind: 'duplicate', name: 'audit' },
		]);
	});

	it('rejects two names with the same path, ignoring case', () => {
		const error = failure([
			{ name: 'ledger.openEntries', sql: 'RETURN 1;' },
			{ name: 'Ledger.openentries', sql: 'RETURN 2;' },
		]);
		expect(error.problems).toMatchObject([
			{
				kind: 'path',
				name: 'Ledger.openentries',
				path: 'Ledger/openentries.surql',
			},
		]);
	});

	it('rejects empty and invalid segments, listing every problem', () => {
		const error = failure([
			{ name: 'ledger..open', sql: 'RETURN 1;' },
			{ name: 'ledger.open entries', sql: 'RETURN 1;' },
			{ name: '.audit', sql: 'RETURN 1;' },
			{ name: '', sql: 'RETURN 1;' },
		]);
		expect(error.problems.map((problem) => problem.kind)).toEqual([
			'segment',
			'segment',
			'segment',
			'unnamed',
		]);
		expect(error.message.split('\n')).toHaveLength(4);
	});
});

describe('pinnedStatementProblems', () => {
	const current = renderPinnedStatements(ledger);

	it('is empty when the pins are current, ignoring other files', () => {
		expect(
			pinnedStatementProblems(ledger, [
				...current,
				{ path: 'README.md', content: 'notes' },
			]),
		).toEqual([]);
	});

	it('reports missing, changed and stale pins, sorted by path', () => {
		expect(
			pinnedStatementProblems(ledger, [
				{ path: 'audit.surql', content: 'RETURN 1;' },
				{ path: 'ledger/closed.surql', content: 'RETURN 3;\n' },
			]),
		).toEqual([
			{ path: 'audit.surql', name: 'audit', kind: 'changed' },
			{ path: 'ledger/closed.surql', kind: 'stale' },
			{
				path: 'ledger/openEntries.surql',
				name: 'ledger.openEntries',
				kind: 'missing',
			},
		]);
	});

	it('compares content exactly, trailing newline included', () => {
		expect(
			pinnedStatementProblems(
				[{ name: 'audit', sql: 'RETURN 1;' }],
				[{ path: 'audit.surql', content: 'RETURN 1;\n\n' }],
			),
		).toEqual([{ path: 'audit.surql', name: 'audit', kind: 'changed' }]);
	});

	it('throws the render error for invalid statements', () => {
		expect(() =>
			pinnedStatementProblems([{ name: '', sql: 'RETURN 1;' }], []),
		).toThrow(PinnedStatementsError);
	});
});

describe('pinning built statements', () => {
	it('pins a builder statement and a sequence', () => {
		const adults = select(PersonTable)
			.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
			.build('people.adults');
		const touch = sequence(
			part({
				sql: 'LET $who = $boundWho;',
				free: ['boundWho'],
				defines: ['who'],
				slots: 1,
			}),
			part({ sql: 'RETURN $who;', free: ['who'], slots: 1 }),
		).define({
			name: 'people.touch',
			params: { boundWho: SurrealSchema.String },
			result: Schema.String,
		});
		const files = renderPinnedStatements([touch, adults]);
		expect(files).toEqual([
			{ path: 'people/adults.surql', content: `${adults.sql}\n` },
			{
				path: 'people/touch.surql',
				content: 'LET $who = $boundWho;\nRETURN $who;\n',
			},
		]);
		expect(pinnedStatementProblems([adults, touch], files)).toEqual([]);
	});
});
