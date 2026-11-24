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
import { spawn, spawnSync } from 'node:child_process';
import {
	cpSync,
	existsSync,
	mkdirSync,
	readdirSync,
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
const all = join(fixtures, '.all');
process.on('exit', () => {
	rmSync(described, { recursive: true, force: true });
	rmSync(all, { recursive: true, force: true });
});

/** `accepts/src/run.ts` under the Node hook and the Bun plugin. */
const hosts = (root) => {
	for (const [host, command, args] of [
		[process.execPath, ['--import', 'effect-surrealkit/register']],
		['bun', ['--preload', 'effect-surrealkit/register']],
	].map(([command, flags]) => [
		command === 'bun' ? 'bun' : 'node',
		command,
		[...flags, 'accepts/src/run.ts', ...(root === undefined ? [] : [root])],
	])) {
		const loaded = run(command, args);
		if (loaded.error !== undefined)
			fail(`${host}: ${loaded.error.message}`);
		if (loaded.status !== 0)
			fail(
				`${host} (exit ${loaded.status}):\n${loaded.stdout}${loaded.stderr}`,
			);
		console.log(`[mapper] ${loaded.stdout.trim()}`);
	}
};
hosts();

/** Asks the mapper process TypeScript starts for each file's text. */
const ask = async (config, options, files) => {
	const child = spawn(
		process.execPath,
		[join(packageRoot, 'dist', 'mapper-server.js')],
		{ stdio: ['pipe', 'pipe', 'inherit'] },
	);
	let buffer = Buffer.alloc(0);
	const waiting = new Map();
	child.stdout.on('data', (chunk) => {
		buffer = Buffer.concat([buffer, chunk]);
		for (;;) {
			const end = buffer.indexOf('\r\n\r\n');
			if (end === -1) return;
			const length = Number(
				/Content-Length: (\d+)/u.exec(
					buffer.subarray(0, end).toString(),
				)[1],
			);
			if (buffer.length < end + 4 + length) return;
			const reply = JSON.parse(
				buffer.subarray(end + 4, end + 4 + length).toString(),
			);
			buffer = buffer.subarray(end + 4 + length);
			waiting.get(reply.id)(reply);
		}
	});
	let next = 0;
	const call = (method, params) =>
		new Promise((done) => {
			const id = `api${++next}`;
			waiting.set(id, done);
			const body = Buffer.from(
				JSON.stringify({ jsonrpc: '2.0', id, method, params }),
			);
			child.stdin.write(`Content-Length: ${body.length}\r\n\r\n`);
			child.stdin.write(body);
		});
	await call('initialize', { positionEncodings: ['utf-8', 'utf-16'] });
	const opened = await call('openProject', {
		configFileName: config,
		projectHandle: 'all',
		options,
		compilerOptions: {},
	});
	if (opened.result.optionDiagnostics.length > 0)
		fail(`${config}: ${JSON.stringify(opened.result.optionDiagnostics)}`);
	const texts = new Map();
	for (const file of files) {
		const { result } = await call('transform', {
			fileName: file,
			content: readFileSync(file, 'utf8'),
			projectHandle: 'all',
		});
		if (result.diagnosticDirectives !== undefined)
			fail(`${file}: ${JSON.stringify(result.diagnosticDirectives)}`);
		texts.set(file, result.text);
	}
	child.stdin.end();
	return texts;
};

// Every authored fixture, with and without its schema: the mapper's text,
// the Node hook's module and the Bun plugin's module are the same module.
const authored = join(packageRoot, 'test', 'fixtures');
const variants = [
	['statements', undefined],
	['statements-binds', undefined],
	['statements-binds', join(authored, 'binds.v2.json')],
	['statements-presence', undefined],
	[
		'statements-presence',
		join(
			packageRoot,
			'..',
			'..',
			'crates',
			'surrealkit',
			'tests',
			'fixtures',
			'typegen_corpus',
			'compiled',
			'field-presence.v2.json',
		),
	],
];
let compared = 0;
for (const [corpus, schema] of variants) {
	const root = join(all, `${corpus}${schema === undefined ? '' : '-schema'}`);
	cpSync(join(authored, corpus), join(root, 'queries'), { recursive: true });
	const options = {
		input: 'queries',
		...(schema !== undefined && { schema }),
	};
	const config = join(root, 'tsconfig.json');
	writeFileSync(
		config,
		JSON.stringify({
			contentMappers: [
				{
					package: 'effect-surrealkit',
					extensions: ['.surql'],
					options,
				},
			],
		}),
	);
	const files = readdirSync(join(root, 'queries'))
		.sort()
		.flatMap((scope) =>
			readdirSync(join(root, 'queries', scope))
				.sort()
				.map((file) => join(root, 'queries', scope, file)),
		);
	const texts = await ask(config, options, files);
	for (const [file, text] of texts) {
		const target = join(
			root,
			'.described',
			relative(join(root, 'queries'), file).replace(/\.surql$/u, '.ts'),
		);
		mkdirSync(dirname(target), { recursive: true });
		writeFileSync(target, text);
	}
	hosts(root);
	compared += files.length;
}
console.log(
	`[mapper] all fixtures: ${compared} files in ${variants.length} variants, the same module from the mapper, Node and Bun`,
);
