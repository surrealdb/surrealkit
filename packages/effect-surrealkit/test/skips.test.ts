/**
 * The comparison behind `npm run test:skips`: which lane a run is on, the
 * full names of the skipped tests in vitest's JSON report, and what differs
 * from the committed list in either direction.
 */
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import {
	compareSkips,
	laneOf,
	parseList,
	renderList,
	skippedTests,
} from '../scripts/skips.mjs';

const root = '/work/package';

describe('laneOf', () => {
	it('names each lane by the variables the engine suites read', () => {
		expect(laneOf({})).toBe('embedded');
		expect(
			laneOf(
				{ SURREAL_BIN: '/bin/surreal' },
				() => '3.3.4 for macos on aarch64\n',
			),
		).toBe('binary-3.3');
		expect(
			laneOf(
				{ SURREAL_BIN: '/bin/surreal', SURREAL_TRANSPORT: 'http' },
				() => '4.0.1 for linux on x86_64',
			),
		).toBe('binary-4.0-http');
		expect(laneOf({ SURREAL_URL: 'ws://127.0.0.1:8000/rpc' })).toBe(
			'remote',
		);
		expect(
			laneOf({
				SURREAL_URL: 'http://127.0.0.1:8000/rpc',
				SURREAL_TRANSPORT: 'http',
			}),
		).toBe('remote-http');
		expect(laneOf({ SURREALKIT_EXEC_ENDPOINT: 'ws://x' })).toBe(
			'embedded-exec',
		);
	});

	it('refuses a binary whose version it cannot read', () => {
		expect(() => laneOf({ SURREAL_BIN: '/bin/surreal' }, () => '')).toThrow(
			/could not read the version/u,
		);
	});
});

describe('skippedTests', () => {
	it('names each skipped test by its file, describes and title, sorted', () => {
		const report = {
			testResults: [
				{
					name: `${root}/test/b.test.ts`,
					assertionResults: [
						{
							ancestorTitles: ['outer'],
							title: 'runs',
							status: 'passed',
						},
						{
							ancestorTitles: ['outer', 'inner'],
							title: 'needs HTTP',
							status: 'skipped',
						},
					],
				},
				{
					name: `${root}/test/a.test.ts`,
					assertionResults: [
						{ ancestorTitles: [], title: 'later', status: 'todo' },
						{
							ancestorTitles: [],
							title: 'failed',
							status: 'failed',
						},
						{
							ancestorTitles: [],
							title: 'pending',
							status: 'pending',
						},
					],
				},
			],
		};
		expect(skippedTests(report, root)).toEqual([
			'test/a.test.ts > later',
			'test/a.test.ts > pending',
			'test/b.test.ts > outer > inner > needs HTTP',
		]);
	});
});

describe('compareSkips', () => {
	it('reports nothing when the skipped tests are the list', () => {
		expect(compareSkips(['a', 'b'], ['b', 'a'])).toEqual({
			added: [],
			removed: [],
		});
	});

	it('reports a newly skipped test and a listed test that ran', () => {
		expect(compareSkips(['a', 'b'], ['a', 'c'])).toEqual({
			added: ['c'],
			removed: ['b'],
		});
	});

	it('counts a name that occurs more than once', () => {
		expect(compareSkips(['a'], ['a', 'a'])).toEqual({
			added: ['a'],
			removed: [],
		});
		expect(compareSkips(['a', 'a'], ['a'])).toEqual({
			added: [],
			removed: ['a'],
		});
	});
});

describe('the committed lists', () => {
	it('round-trip through their header', () => {
		expect(parseList(renderList('embedded', ['x > y', 'z']))).toEqual([
			'x > y',
			'z',
		]);
	});

	it.each([
		'embedded',
		'binary-3.3',
	])('pins the %s lane, sorted and as written', (lane) => {
		const path = join(import.meta.dirname, 'skips', `${lane}.txt`);
		expect(existsSync(path)).toBe(true);
		const text = readFileSync(path, 'utf8');
		const names = parseList(text);
		expect(text).toBe(renderList(lane, [...names].sort()));
	});
});
