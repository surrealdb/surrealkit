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
			liveText: 'LIVE SELECT * FROM user',
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
			liveText: 'LIVE SELECT * FROM user',
			snapshot: text,
			windowed: ['WHERE', 'ORDER', 'LIMIT', 'START'],
		});
		expect(planLive(text, { trustWhere: true })).toEqual({
			mode: 'window',
			liveText: 'LIVE SELECT * FROM user WHERE active = true',
			snapshot: text,
			windowed: ['ORDER', 'LIMIT', 'START'],
		});
	});

	it.each([
		['SELECT VALUE name FROM user', ['VALUE']],
		['SELECT * OMIT secret FROM user', ['OMIT']],
		['SELECT count() FROM user GROUP ALL', ['GROUP']],
		['SELECT * FROM user SPLIT tags', ['SPLIT']],
		['SELECT * FROM user WITH NOINDEX', ['WITH']],
		['SELECT * FROM user TIMEOUT 1s', ['TIMEOUT']],
		['SELECT * FROM user PARALLEL', ['PARALLEL']],
		['SELECT * FROM user TEMPFILES', ['TEMPFILES']],
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
			'LIVE SELECT * FROM user WHERE a = 1',
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
		['SELECT * FROM user:limit'],
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

	it('accepts a trailing semicolon and whitespace', () => {
		expect(planLive('  SELECT * FROM user ;  \n').mode).toBe('keyed');
	});
});
