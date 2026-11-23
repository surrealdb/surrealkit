#!/usr/bin/env node
/**
 * Runs the test suite on one engine lane and compares the tests it skipped
 * with the lane's committed list, `test/skips/<lane>.txt`.
 *
 * The lane comes from the same variables the engine suites read:
 *
 * - `embedded`: neither `SURREAL_URL` nor `SURREAL_BIN` is set.
 * - `binary-<major>.<minor>`: `SURREAL_BIN`, by the version the binary
 *   reports, e.g. `binary-3.3`.
 * - `remote`: `SURREAL_URL`, whose version is not known before the suite
 *   connects.
 *
 * `SURREAL_TRANSPORT=http` appends `-http`, and `SURREALKIT_EXEC_ENDPOINT`
 * (the round trips of `scripts/roundtrip.mjs`) appends `-exec`.
 *
 * Exits 1 when the suite fails, or when a test is skipped that the list does
 * not name or the list names one that was not skipped; both directions are
 * printed. A lane with no list is reported as unpinned and is not compared.
 * `UPDATE_SKIPS=1` writes the list from a passing run.
 */
import { spawnSync } from 'node:child_process';
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(import.meta.dirname, '..');
const listsRoot = join(packageRoot, 'test', 'skips');

/**
 * The lane's name. `binaryVersion` is what `surreal version` prints, read
 * only when `SURREAL_BIN` is set.
 */
export const laneOf = (env, binaryVersion = () => '') => {
	let lane;
	if (env.SURREAL_URL !== undefined) {
		lane = 'remote';
	} else if (env.SURREAL_BIN !== undefined) {
		const version = /(\d+)\.(\d+)\./u.exec(binaryVersion());
		if (version === null) {
			throw new Error(
				`could not read the version of SURREAL_BIN (${env.SURREAL_BIN})`,
			);
		}
		lane = `binary-${version[1]}.${version[2]}`;
	} else {
		lane = 'embedded';
	}
	if (env.SURREAL_TRANSPORT === 'http') lane += '-http';
	if (env.SURREALKIT_EXEC_ENDPOINT !== undefined) lane += '-exec';
	return lane;
};

/**
 * The full names of the skipped tests in a vitest JSON report, as
 * `<file> > <describe> > … > <test>` with the file relative to `root`,
 * sorted.
 */
export const skippedTests = (report, root) =>
	report.testResults
		.flatMap((file) =>
			file.assertionResults
				.filter(
					(test) =>
						test.status === 'skipped' ||
						test.status === 'pending' ||
						test.status === 'todo',
				)
				.map((test) =>
					[
						relative(root, file.name).split('\\').join('/'),
						...test.ancestorTitles,
						test.title,
					].join(' > '),
				),
		)
		.sort();

/** The names of a committed list: one per line, `#` starts a comment. */
export const parseList = (text) =>
	text
		.split('\n')
		.map((line) => line.trimEnd())
		.filter((line) => line !== '' && !line.startsWith('#'));

export const renderList = (lane, names) =>
	[
		`# The tests skipped on the ${lane} lane. Written by`,
		'# `UPDATE_SKIPS=1 npm run test:skips`; see scripts/skips.mjs.',
		...names,
		'',
	].join('\n');

/**
 * What differs between the committed list and the skipped tests, counting
 * a name that occurs twice twice: `added` were skipped and are not listed,
 * `removed` are listed and were not skipped.
 */
export const compareSkips = (expected, actual) => {
	const remaining = new Map();
	for (const name of expected) {
		remaining.set(name, (remaining.get(name) ?? 0) + 1);
	}
	const added = [];
	for (const name of actual) {
		const count = remaining.get(name) ?? 0;
		if (count === 0) added.push(name);
		else remaining.set(name, count - 1);
	}
	const removed = [...remaining].flatMap(([name, count]) =>
		Array.from({ length: count }, () => name),
	);
	return { added: added.sort(), removed: removed.sort() };
};

const binaryVersion = () => {
	const result = spawnSync(process.env.SURREAL_BIN, ['version'], {
		encoding: 'utf8',
	});
	if (result.error !== undefined) throw result.error;
	return result.stdout;
};

const main = () => {
	if (process.argv.length > 2) {
		console.error(
			'test:skips takes no arguments: it runs the whole suite, so the skipped tests can be compared.',
		);
		return 2;
	}
	const lane = laneOf(process.env, binaryVersion);
	const listPath = join(listsRoot, `${lane}.txt`);
	const shown = relative(packageRoot, listPath);
	const update = process.env.UPDATE_SKIPS === '1';

	const outDir = mkdtempSync(join(tmpdir(), 'effect-surrealkit-skips-'));
	const reportPath = join(outDir, 'report.json');
	let report;
	let status;
	try {
		const vitest = join(
			dirname(
				createRequire(import.meta.url).resolve('vitest/package.json'),
			),
			'vitest.mjs',
		);
		const run = spawnSync(
			process.execPath,
			[
				vitest,
				'run',
				'--reporter=default',
				'--reporter=json',
				`--outputFile.json=${reportPath}`,
			],
			{ cwd: packageRoot, stdio: 'inherit' },
		);
		status = run.status ?? 1;
		if (existsSync(reportPath)) {
			report = JSON.parse(readFileSync(reportPath, 'utf8'));
		}
	} finally {
		rmSync(outDir, { recursive: true, force: true });
	}
	if (report === undefined) {
		console.error(`[skips] vitest wrote no report on the ${lane} lane`);
		return status === 0 ? 1 : status;
	}
	if (status !== 0) {
		console.error(
			`[skips] the suite failed on the ${lane} lane; skipped tests not compared`,
		);
		return status;
	}

	const actual = skippedTests(report, packageRoot);
	if (update) {
		mkdirSync(listsRoot, { recursive: true });
		writeFileSync(listPath, renderList(lane, actual));
		console.log(
			`[skips] wrote ${shown}: ${actual.length} skipped on the ${lane} lane`,
		);
		return 0;
	}
	if (!existsSync(listPath)) {
		console.log(
			`[skips] the ${lane} lane is unpinned: ${shown} does not exist, so its ${actual.length} skipped tests were not compared. UPDATE_SKIPS=1 writes it.`,
		);
		return 0;
	}
	const { added, removed } = compareSkips(
		parseList(readFileSync(listPath, 'utf8')),
		actual,
	);
	if (added.length === 0 && removed.length === 0) {
		console.log(
			`[skips] ${actual.length} skipped on the ${lane} lane, as ${shown} lists`,
		);
		return 0;
	}
	console.error(
		`[skips] the skipped tests on the ${lane} lane differ from ${shown}:`,
	);
	for (const name of added)
		console.error(`  + ${name}  (skipped, not listed)`);
	for (const name of removed)
		console.error(`  - ${name}  (listed, not skipped)`);
	console.error(
		'[skips] if the change is intended, rewrite the list with UPDATE_SKIPS=1 npm run test:skips',
	);
	return 1;
};

if (
	process.argv[1] !== undefined &&
	resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
	process.exitCode = main();
}
