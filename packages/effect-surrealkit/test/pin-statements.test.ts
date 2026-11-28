import { Schema } from 'effect';
import { describe, expect, it } from 'vitest';
import {
	type PinnableStatement,
	PinnedStatementsError,
	pinnedStatementProblems,
	renderPinnedStatements,
} from '../src/analyze/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	choose,
	compose,
	family,
	gte,
	param,
	part,
	select,
	sequence,
} from '../src/statement/index.js';
import { PersonTable } from './golden/corpus/index.js';
import { catalog } from './golden/statements/index.js';
import { listings, sized, summary } from './types/family.js';

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

describe('pinning a statement family', () => {
	it('pins every variant under the family name, one segment per axis', () => {
		const files = renderPinnedStatements(listings.variants());
		expect(files).toHaveLength(8);
		expect(files[0]?.path).toBe(
			'listings/price-false/region-false/vector-false.surql',
		);
		expect(files[7]?.path).toBe(
			'listings/price-true/region-true/vector-true.surql',
		);
		expect(files[0]?.content).toBe(
			`${listings.select({ price: false, region: false, vector: false }).sql}\n`,
		);
		expect(
			renderPinnedStatements(sized.variants()).map((f) => f.path),
		).toEqual(['sized/size-1.surql', 'sized/size-2.surql']);
		expect(renderPinnedStatements(summary.variants())).toEqual([
			{ path: 'summary.surql', content: `${summary.select({}).sql}\n` },
		]);
	});

	it('reports a variant whose text changed, one not yet pinned and one gone', () => {
		const [first, second, ...rest] = renderPinnedStatements(
			listings.variants(),
		);
		const onDisk = [
			...rest,
			{ path: first?.path ?? '', content: 'RETURN 0;\n' },
			{ path: 'listings/price-maybe.surql', content: 'RETURN 0;\n' },
		];
		expect(pinnedStatementProblems(listings.variants(), onDisk)).toEqual([
			{
				path: 'listings/price-false/region-false/vector-false.surql',
				name: 'listings.price-false.region-false.vector-false',
				kind: 'changed',
			},
			{
				path: 'listings/price-false/region-false/vector-true.surql',
				name: 'listings.price-false.region-false.vector-true',
				kind: 'missing',
			},
			{ path: 'listings/price-maybe.surql', kind: 'stale' },
		]);
		expect(second?.path).toBe(
			'listings/price-false/region-false/vector-true.surql',
		);
		expect(
			pinnedStatementProblems(
				listings.variants(),
				renderPinnedStatements(listings.variants()),
			),
		).toEqual([]);
	});

	it('refuses an axis value that is not a name segment', () => {
		const versioned = family(
			{ version: ['1.0', '2'] },
			compose(catalog.guarded, {
				__BODY__: choose('version', {
					'1.0': catalog.noRows,
					2: catalog.notReady,
				}),
			}),
		).define({
			name: 'versioned',
			params: { tenant: SurrealSchema.String },
			result: Schema.Unknown,
		});
		expect(failure(versioned.variants()).message).toBe(
			'statement "versioned.version-1%2E0" has a name segment that is empty or not letters, digits, _ and -',
		);
	});
});
