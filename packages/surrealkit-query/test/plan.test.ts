import { describe, expect, it } from 'vitest';

import { planLive } from '../src/index.js';

describe('planLive', () => {
	it('makes a SELECT with only FETCH keyed', () => {
		expect(planLive('SELECT * FROM user FETCH team;')).toEqual({
			mode: 'keyed',
			liveText: 'LIVE SELECT * FROM user FETCH team',
			snapshot: 'SELECT * FROM user FETCH team',
			windowed: [],
		});
	});

	it('windows a WHERE, watching the whole target', () => {
		expect(planLive('SELECT * FROM user WHERE age > $min')).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user',
			snapshot: 'SELECT * FROM user WHERE age > $min',
			windowed: ['WHERE'],
		});
	});

	it('keys a WHERE with trustWhere', () => {
		expect(
			planLive('SELECT * FROM user WHERE age > $min FETCH team', {
				trustWhere: true,
			}),
		).toEqual({
			mode: 'keyed',
			liveText: 'LIVE SELECT * FROM user WHERE age > $min FETCH team',
			snapshot: 'SELECT * FROM user WHERE age > $min FETCH team',
			windowed: [],
		});
	});

	it('windows a clause LIVE SELECT refuses, keeping a trusted WHERE', () => {
		const text =
			'SELECT * FROM user WHERE active = true ORDER BY name LIMIT $lim START $start';
		expect(planLive(text)).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user',
			snapshot: text,
			windowed: ['WHERE', 'ORDER BY', 'LIMIT', 'START'],
		});
		expect(planLive(text, { trustWhere: true })).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user WHERE active = true',
			snapshot: text,
			windowed: ['ORDER BY', 'LIMIT', 'START'],
		});
	});

	it.each([
		['SELECT VALUE name FROM user', ['VALUE']],
		['SELECT * OMIT secret FROM user', ['OMIT']],
		['SELECT * FROM user WITH NOINDEX', ['WITH']],
		['SELECT * FROM user SPLIT tags', ['SPLIT']],
		['SELECT count() FROM user GROUP ALL', ['GROUP BY']],
		['SELECT team, count() FROM user GROUP BY team', ['GROUP BY']],
		['SELECT * FROM user ORDER BY name', ['ORDER BY']],
		['SELECT * FROM user LIMIT 3', ['LIMIT']],
		['SELECT * FROM user START 3', ['START']],
		['SELECT * FROM user VERSION d"2026-01-01"', ['VERSION']],
		['SELECT * FROM user TIMEOUT 1s', ['TIMEOUT']],
		['SELECT * FROM user TEMPFILES', ['TEMPFILES']],
		['SELECT * FROM user EXPLAIN', ['EXPLAIN']],
		['SELECT * FROM user FOR UPDATE', ['FOR UPDATE']],
		['SELECT * FROM user PARALLEL', ['PARALLEL']],
	])('windows %s', (text, windowed) => {
		const plan = planLive(text, { trustWhere: true });
		expect(plan.mode).toBe('window');
		expect(plan.mode !== 'unsupported' && plan.windowed).toEqual(windowed);
	});

	it('stops a WHERE at the next clause', () => {
		const plan = planLive(
			'SELECT * FROM user WITH INDEX i WHERE a = 1 LIMIT 2',
			{ trustWhere: true },
		);
		expect(plan.mode !== 'unsupported' && plan.liveText).toBe(
			'LIVE SELECT id FROM user WHERE a = 1',
		);
	});

	it.each([
		["SELECT * FROM user WHERE name = 'ORDER BY x LIMIT 1'"],
		['SELECT * FROM user WHERE name = "a; LIMIT 1"'],
		['SELECT * FROM user -- LIMIT 1\nWHERE a = 1'],
		['SELECT * FROM user /* ORDER BY a */ WHERE a = 1'],
		['SELECT * FROM user # START 1\nWHERE a = 1'],
		['SELECT * FROM user // GROUP ALL\nWHERE a = 1'],
		['SELECT `limit`, ⟨order by⟩ FROM user'],
		[
			'SELECT * FROM user WHERE id IN (SELECT VALUE id FROM post ORDER BY at LIMIT 3)',
		],
		[
			'SELECT * FROM user WHERE tags CONTAINSANY [1, 2] AND meta = { limit: 3 }',
		],
		['SELECT * FROM user WHERE $limit > 3 AND settings.order = 1'],
		['SELECT * FROM user WHERE id = user:limit'],
	])('ignores keywords outside the top level: %s', (text) => {
		expect(planLive(text, { trustWhere: true }).mode).toBe('keyed');
	});

	it.each([
		['RETURN 1', 'a live query must be a SELECT'],
		[
			'SELECT * FROM a; SELECT * FROM b',
			'a live query must be a single statement',
		],
		['SELECT * FROM ONLY user:ada', 'a live query cannot select FROM ONLY'],
		['SELECT * FROM a, b', 'a live query must select FROM one target'],
		['SELECT 1', 'a live query must select FROM a table'],
	])('refuses %s', (text, reason) => {
		expect(planLive(text)).toEqual({ mode: 'unsupported', reason });
	});

	it.each([
		['SELECT * FROM $table'],
		['SELECT * FROM type::table($name)'],
		['SELECT * FROM user FETCH team'],
	])('keys %s, which LIVE SELECT accepts and reflects', (text) => {
		expect(planLive(text)).toMatchObject({
			mode: 'keyed',
			liveText: `LIVE ${text}`,
		});
	});

	it.each([
		[
			'SELECT * FROM user:ada',
			'a live query cannot select FROM a record id',
		],
		[
			'SELECT * FROM user:⟨a b⟩',
			'a live query cannot select FROM a record id',
		],
		[
			'SELECT * FROM `the user`:ada',
			'a live query cannot select FROM a record id',
		],
		[
			'SELECT * FROM r"user:ada"',
			'a live query cannot select FROM a record id',
		],
		[
			'SELECT * FROM user:1..5',
			'a live query cannot select FROM a record id',
		],
		[
			'SELECT * FROM [user:a, user:b]',
			'a live query cannot select FROM an array',
		],
		[
			'SELECT * FROM (SELECT * FROM user)',
			'a live query cannot select FROM a subquery',
		],
		['SELECT DIFF FROM user', 'DIFF live queries are not supported'],
	])('refuses %s, which re-running cannot serve', (text, reason) => {
		expect(planLive(text)).toEqual({ mode: 'unsupported', reason });
	});

	it('accepts a trailing semicolon and whitespace', () => {
		expect(planLive('  SELECT * FROM user ;  \n').mode).toBe('keyed');
	});
});

describe('planLive with an engine entry', () => {
	const PAGE = 'SELECT * FROM user WHERE active ORDER BY name LIMIT $lim';

	it('keys an accepted query and subscribes with the engine text', () => {
		expect(
			planLive('SELECT * FROM user WHERE active', {
				entry: {
					query: 'LIVE SELECT * FROM user WHERE active',
					exit: 'delete',
				},
			}),
		).toEqual({
			mode: 'keyed',
			liveText: 'LIVE SELECT * FROM user WHERE active',
			snapshot: 'SELECT * FROM user WHERE active',
			windowed: [],
		});
	});

	it('windows a silent exit, watching the whole target', () => {
		expect(
			planLive('SELECT * FROM user WHERE active', {
				entry: {
					query: 'LIVE SELECT * FROM user WHERE active',
					exit: 'silent',
				},
			}),
		).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user',
			snapshot: 'SELECT * FROM user WHERE active',
			windowed: ['WHERE'],
		});
	});

	it('windows a windowable refusal with the engine text when exits notify', () => {
		expect(
			planLive(PAGE, {
				entry: {
					query: 'LIVE SELECT * FROM user WHERE active',
					exit: 'delete',
					refusal: {
						clauses: ['ORDER BY', 'LIMIT'],
						windowable: true,
					},
				},
			}),
		).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user WHERE active',
			snapshot: PAGE,
			windowed: ['ORDER BY', 'LIMIT'],
		});
	});

	it('windows a windowable refusal unfiltered when exits are silent', () => {
		const plan = planLive(PAGE, {
			entry: {
				query: 'LIVE SELECT * FROM user WHERE active',
				exit: 'silent',
				refusal: { clauses: ['ORDER BY', 'LIMIT'], windowable: true },
			},
		});
		expect(plan).toMatchObject({
			mode: 'window',
			liveText: 'LIVE SELECT id FROM user',
			windowed: ['ORDER BY', 'LIMIT', 'WHERE'],
		});
	});

	it("watches with the engine's watch text when exits are silent", () => {
		const plan = planLive(PAGE, {
			entry: {
				query: 'LIVE SELECT * FROM user WHERE active',
				exit: 'silent',
				watch: 'LIVE SELECT id FROM user',
				refusal: { clauses: ['ORDER BY', 'LIMIT'], windowable: true },
			},
		});
		expect(plan.mode !== 'unsupported' && plan.liveText).toBe(
			'LIVE SELECT id FROM user',
		);
	});

	it('watches the plain target for a window, not the reduced projection', () => {
		const plan = planLive('SELECT count() AS total FROM person GROUP ALL', {
			entry: {
				query: 'LIVE SELECT count() AS total FROM person',
				refusal: { clauses: ['GROUP BY'], windowable: true },
			},
		});
		expect(plan.mode !== 'unsupported' && plan.liveText).toBe(
			'LIVE SELECT id FROM person',
		);
	});

	it('refuses what the engine says re-running cannot serve', () => {
		expect(
			planLive('SELECT * FROM a, b', {
				entry: {
					refusal: { clauses: ['FROM a, b'], windowable: false },
				},
			}),
		).toEqual({
			mode: 'unsupported',
			reason: 'LIVE SELECT refuses FROM a, b',
		});
	});

	it('follows the engine over trustWhere', () => {
		const plan = planLive('SELECT * FROM user WHERE active', {
			trustWhere: true,
			entry: {
				query: 'LIVE SELECT * FROM user WHERE active',
				exit: 'silent',
			},
		});
		expect(plan.mode !== 'unsupported' && plan.liveText).toBe(
			'LIVE SELECT id FROM user',
		);
	});
});
