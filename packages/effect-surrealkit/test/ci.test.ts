/**
 * The Node floor in `engines` is exercised: the CI jobs that run the suite
 * and the content mapper each run on it.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

const packageRoot = join(import.meta.dirname, '..');
const workflow = readFileSync(
	join(
		packageRoot,
		'..',
		'..',
		'.github',
		'workflows',
		'effect-surrealkit-ci.yml',
	),
	'utf8',
);
const floor = (
	JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8')) as {
		engines: { node: string };
	}
).engines.node.replace(/^>=/u, '');

/** The text of each job, by its key under `jobs:`. */
const jobs = new Map<string, string>();
{
	const lines = workflow.split('\n');
	let name: string | undefined;
	for (const line of lines) {
		const key = /^ {4}([\w-]+):\s*$/u.exec(line);
		if (key !== null) {
			name = key[1] as string;
			jobs.set(name, '');
		} else if (name !== undefined)
			jobs.set(name, `${jobs.get(name)}${line}\n`);
	}
}

/** The Node versions a job's matrix lists, or its single `node-version`. */
const nodeVersions = (job: string): ReadonlyArray<string> => {
	const matrix = /^\s+node: \[(.*)\]\s*$/mu.exec(job);
	if (matrix !== null)
		return (matrix[1] as string)
			.split(',')
			.map((v) => v.trim().replace(/^"|"$/gu, ''));
	const single = /^\s+node-version: (\S+)\s*$/mu.exec(job);
	return single === null ? [] : [single[1] as string];
};

describe('the CI workflow', () => {
	it(`states the floor ${floor} in engines`, () => {
		expect(floor).toMatch(/^\d+\.\d+\.\d+$/u);
	});

	it.each([
		'check',
		'mapper',
	])('runs the %s job on the Node floor and on Node 24', (name) => {
		const job = jobs.get(name);
		expect(job).toBeDefined();
		expect(nodeVersions(job as string)).toEqual([floor, '24']);
		expect(job).toMatch(/node-version: \$\{\{ matrix\.node \}\}/u);
	});
});
