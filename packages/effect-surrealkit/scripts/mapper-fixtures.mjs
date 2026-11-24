#!/usr/bin/env node
/**
 * Runs the content-mapper fixtures (`test/mapper-fixtures`) on the
 * TypeScript they pin, after `npm run build` and `npm ci` in that directory:
 *
 * 1. The installed TypeScript is exactly the pinned version.
 * 2. `accepts` type-checks with `tsc --noEmit --runExternalCode` and no
 *    error, and without `--runExternalCode` it does not.
 * 3. `rejects` fails with exactly `rejects/expected.txt`: an analyzer error
 *    and a schema problem inside their `.surql` files at their line and
 *    column, a missing binding where the part is used, and a type error in
 *    another file, which the `.surql` errors do not hide.
 * 4. The texts the mapper answered the type check of `accepts` (read from
 *    `TS_CONTENT_MAPPER_DEBUG`) are written to `accepts/.described`, and
 *    `accepts/src/run.ts` runs under `node --import effect-surrealkit/register`
 *    and `bun --preload effect-surrealkit/register`: each loader's text is the
 *    mapper's, and each loaded module holds the same values.
 *
 * Exits 1 on the first step that fails. `bun` must be on PATH.
 */
import { spawnSync } from 'node:child_process';
import {
	existsSync,
	mkdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';

const packageRoot = resolve(import.meta.dirname, '..');
const fixtures = join(packageRoot, 'test', 'mapper-fixtures');

const fail = (message) => {
	console.error(`[mapper] ${message}`);
	process.exit(1);
};

const run = (command, args, env = {}) =>
	spawnSync(command, args, {
		cwd: fixtures,
		encoding: 'utf8',
		env: { ...process.env, ...env },
		maxBuffer: 64 * 1024 * 1024,
	});

if (!existsSync(join(packageRoot, 'dist', 'mapper-server.js')))
	fail('dist/mapper-server.js is missing; run npm run build first');
const pinned = JSON.parse(readFileSync(join(fixtures, 'package.json'), 'utf8'))
	.devDependencies.typescript;
const installed = join(fixtures, 'node_modules', 'typescript', 'package.json');
if (!existsSync(installed))
	fail(
		`TypeScript is not installed; run npm ci in ${relative(process.cwd(), fixtures)}`,
	);
const version = JSON.parse(readFileSync(installed, 'utf8')).version;
if (version !== pinned)
	fail(
		`TypeScript ${version} is installed, the fixtures pin ${pinned}; run npm ci`,
	);
const tsc = join(fixtures, 'node_modules', '.bin', 'tsc');
console.log(`[mapper] TypeScript ${version}`);

const accepts = run(
	tsc,
	['-p', 'accepts', '--noEmit', '--runExternalCode', '--pretty', 'false'],
	{
		TS_CONTENT_MAPPER_DEBUG: '1',
	},
);
if (accepts.status !== 0 || accepts.stdout !== '')
	fail(
		`accepts did not type-check (exit ${accepts.status}):\n${accepts.stdout}${accepts.stderr}`,
	);
console.log('[mapper] accepts type-checks with --runExternalCode');

const ungated = run(tsc, ['-p', 'accepts', '--noEmit', '--pretty', 'false']);
if (ungated.status === 0 || !ungated.stdout.includes('TS18068'))
	fail(
		`accepts without --runExternalCode did not report TS18068:\n${ungated.stdout}`,
	);
console.log('[mapper] accepts without --runExternalCode reports TS18068');

const rejects = run(tsc, [
	'-p',
	'rejects',
	'--noEmit',
	'--runExternalCode',
	'--pretty',
	'false',
]);
const expected = readFileSync(
	join(fixtures, 'rejects', 'expected.txt'),
	'utf8',
);
if (rejects.status !== 2 || rejects.stdout !== expected)
	fail(
		`rejects (exit ${rejects.status}) did not report rejects/expected.txt:\n--- expected\n${expected}--- reported\n${rejects.stdout}`,
	);
console.log(
	`[mapper] rejects reports ${expected.trimEnd().split('\n').length} errors, as rejects/expected.txt lists`,
);

// The debug log pairs each transform sent with the reply received.
const sent = new Map();
const texts = new Map();
for (const line of accepts.stderr.split('\n')) {
	const match =
		/^\[content mapper: effect-surrealkit\] (send|receive): (.*)$/u.exec(
			line,
		);
	if (match === null) continue;
	const message = JSON.parse(match[2]);
	if (match[1] === 'send' && message.method === 'transform')
		sent.set(message.id, message.params.fileName);
	else if (match[1] === 'receive' && sent.has(message.id))
		texts.set(sent.get(message.id), message.result.text);
}
const queries = join(fixtures, 'accepts', 'queries');
const described = join(fixtures, 'accepts', '.described');
rmSync(described, { recursive: true, force: true });
let written = 0;
for (const [file, text] of texts) {
	const at = relative(queries, resolve(file));
	if (at.startsWith('..')) continue;
	const target = join(described, at.replace(/\.surql$/u, '.ts'));
	mkdirSync(dirname(target), { recursive: true });
	writeFileSync(target, text);
	written++;
}
if (written === 0)
	fail(
		'the type check transformed no .surql file (no TS_CONTENT_MAPPER_DEBUG log?)',
	);
console.log(`[mapper] the mapper answered ${written} .surql files`);

// The texts are scratch: removed however the hosts end.
process.on('exit', () => rmSync(described, { recursive: true, force: true }));
for (const [host, command, args] of [
	[
		'node',
		process.execPath,
		['--import', 'effect-surrealkit/register', 'accepts/src/run.ts'],
	],
	[
		'bun',
		'bun',
		['--preload', 'effect-surrealkit/register', 'accepts/src/run.ts'],
	],
]) {
	const loaded = run(command, args);
	if (loaded.error !== undefined) fail(`${host}: ${loaded.error.message}`);
	if (loaded.status !== 0)
		fail(
			`${host} (exit ${loaded.status}):\n${loaded.stdout}${loaded.stderr}`,
		);
	console.log(`[mapper] ${loaded.stdout.trim()}`);
}
