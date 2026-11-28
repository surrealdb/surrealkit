import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { arch, cpus, platform, tmpdir, totalmem } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

import { Effect, Schema, Stream } from 'effect';
import { Int, RecordIdFor } from 'effect-surrealkit';
import { bind, make } from 'effect-surrealkit/client';
import { select, make as statement } from 'effect-surrealkit/statement';
import { RecordId, Surreal } from 'surrealdb';

const samples = 5;
const inserts = 5_000;
const chunk = 500;
const concurrency = 64;
const smallReads = 8;
const largeRows = 20_000;
const transactionCycles = 100;
const tasks = [
	'insertRunSequential',
	'insertRunConcurrent',
	'insertBulkIndependent',
	'insertBulkAtomic',
	'readRuns',
	'readPipeline',
	'readLargeRun',
	'readLargeStream',
	'strictDecode',
	'twoWriteTransaction',
	'twoWriteInteractive',
];

const itemId = RecordIdFor('item');
const item = (index) => ({ id: new RecordId('item', index), n: index });
const noRows = Schema.Tuple([]);
const create = statement({
	name: 'insert one item',
	sql: 'CREATE ONLY $id SET n = $n RETURN NONE;',
	params: { id: itemId, n: Int },
	result: Schema.Undefined,
	writes: true,
});
const insert = statement({
	name: 'insert many items',
	sql: 'INSERT INTO item $rows RETURN NONE;',
	params: { rows: Schema.Array(Schema.Struct({ id: itemId, n: Int })) },
	result: noRows,
	writes: true,
});
const read = statement({
	name: 'read item value',
	sql: 'SELECT VALUE n FROM ONLY $id;',
	params: { id: itemId },
	result: Int,
	writes: false,
});
const increment = statement({
	name: 'increment count',
	sql: 'UPDATE ONLY item:counter SET n += $amount RETURN NONE;',
	params: { amount: Int },
	result: Schema.Undefined,
	writes: true,
});

const total = (amount) => (amount * (amount - 1)) / 2;
const gc = () => globalThis.gc?.();

const measure = async (op) => {
	gc();
	const start = performance.now();
	const result = await op();
	return { result, elapsedMs: performance.now() - start };
};

const measureInserts = async (task, db, client) => {
	const rows = Array.from({ length: inserts }, (_, index) => item(index));
	const atomic = task === 'insertBulkAtomic';
	const concurrent = task === 'insertRunConcurrent';
	const { result, elapsedMs } = await measure(() => {
		const program =
			task === 'insertRunSequential' || concurrent
				? Effect.forEach(rows, (row) => client.run(create, row), {
						concurrency: concurrent ? concurrency : 1,
						discard: true,
					})
				: client.bulk(
						insert,
						{ rows },
						{ over: 'rows', chunk, atomic },
					);
		return Effect.runPromise(program);
	});
	if (result !== undefined) {
		assert.equal(result.rows, inserts);
		assert.equal(result.chunks, inserts / chunk);
	}
	const [count, sum] = await db.query(
		'count((SELECT VALUE id FROM item)); math::sum((SELECT VALUE n FROM item));',
	);
	assert.equal(count, inserts);
	assert.equal(sum, total(inserts));
	return { elapsedMs };
};

const measureSmallReads = async (task, db, client) => {
	const items = Array.from({ length: smallReads }, (_, index) => item(index));
	await db.query('INSERT INTO item $rows RETURN NONE;', { rows: items });
	const inputs = items.map(({ id }) => ({ id }));
	const { result, elapsedMs } = await measure(() =>
		Effect.runPromise(
			task === 'readRuns'
				? Effect.forEach(inputs, (input) => client.run(read, input))
				: Effect.map(
						client.pipeline(
							inputs.map((input) => bind(read, input)),
						),
						(results) =>
							results.map((result) => {
								if (result._tag === 'Failure')
									throw result.failure;
								return result.success;
							}),
					),
		),
	);
	assert.deepEqual(
		result,
		items.map(({ n }) => n),
	);
	return { elapsedMs };
};

const seedDocuments = async (db) => {
	const owner = new RecordId('person', 'owner');
	for (let offset = 0; offset < largeRows; offset += chunk) {
		const rows = Array.from({ length: chunk }, (_, index) => {
			const n = offset + index;
			return {
				id: new RecordId('doc', n),
				title: 'benchmark row',
				body: 'A synthetic document for the strict generated row codec.',
				code: String(n),
				embedding: [1.25, 2.5, 3.75, 4.125],
				owner,
				status: 'open',
				touched: false,
			};
		});
		await db.query('INSERT INTO doc $rows RETURN NONE;', { rows });
	}
};

const checkDocuments = (rows) => {
	assert.equal(rows.length, largeRows);
	assert.equal(
		rows.reduce((sum, row) => sum + Number(row.code), 0),
		total(largeRows),
	);
};

const measureDocuments = async (task, db, client, fixture) => {
	const { DocTable } = await import(fixture);
	const readDocs = select(DocTable).build('read generated doc rows');
	await seedDocuments(db);

	if (task === 'strictDecode') {
		const [raw] = await db.query(readDocs.sql);
		const strictDecode = Schema.decodeUnknownSync(readDocs.result);
		const { result, elapsedMs } = await measure(() =>
			strictDecode(raw, { onExcessProperty: 'error' }),
		);
		checkDocuments(result);
		return { elapsedMs, microsPerRow: (elapsedMs * 1_000) / largeRows };
	}

	if (task === 'readLargeRun') {
		const { result, elapsedMs } = await measure(() =>
			Effect.runPromise(client.run(readDocs)),
		);
		const peakProcessRssKiB = process.resourceUsage().maxRSS;
		checkDocuments(result);
		return { elapsedMs, peakProcessRssKiB };
	}

	let count = 0;
	let sum = 0;
	const { elapsedMs } = await measure(() =>
		Effect.runPromise(
			Stream.runForEach(client.stream(readDocs), (row) =>
				Effect.sync(() => {
					count++;
					sum += Number(row.code);
				}),
			),
		),
	);
	const peakProcessRssKiB = process.resourceUsage().maxRSS;
	assert.equal(count, largeRows);
	assert.equal(sum, total(largeRows));
	return { elapsedMs, peakProcessRssKiB };
};

const measureTransactions = async (task, db, client) => {
	await db.query('CREATE ONLY item:counter SET n = 0 RETURN NONE;');
	const atomic = client.transaction([
		bind(increment, { amount: 1 }),
		bind(increment, { amount: 1 }),
	]);
	const interactive = client.withTransaction((tx) =>
		Effect.andThen(
			tx.run(increment, { amount: 1 }),
			tx.run(increment, { amount: 1 }),
		),
	);
	const { elapsedMs } = await measure(async () => {
		for (let index = 0; index < transactionCycles; index++) {
			await Effect.runPromise(
				task === 'twoWriteTransaction' ? atomic : interactive,
			);
		}
	});
	assert.deepEqual(await db.query('SELECT VALUE n FROM ONLY item:counter;'), [
		transactionCycles * 2,
	]);
	return { elapsedMs, msPerTransaction: elapsedMs / transactionCycles };
};

const runWorker = async (task, fixture, wsUrl) => {
	assert(tasks.includes(task));
	let db;
	try {
		if (wsUrl === undefined) {
			const { createNodeEngines } = await import('@surrealdb/node');
			db = new Surreal({ engines: createNodeEngines() });
			await db.connect('mem://', {
				namespace: 'benchmark',
				database: 'benchmark',
			});
		} else {
			const endpoint = new URL(wsUrl);
			assert.equal(endpoint.protocol, 'ws:');
			assert.equal(endpoint.hostname, '127.0.0.1');
			const password = process.env[PASSWORD_ENV];
			assert(password, `${PASSWORD_ENV} is not set`);
			db = new Surreal();
			await db.connect(endpoint, {
				authentication: { username: 'root', password },
				reconnect: false,
			});
			await db.query(
				'DEFINE NAMESPACE benchmark; USE NS benchmark; DEFINE DATABASE benchmark;',
			);
			await db.use({ namespace: 'benchmark', database: 'benchmark' });
		}
		const { version } = await db.version();
		const client = make(db);
		let result;
		if (task.startsWith('insert')) {
			result = await measureInserts(task, db, client);
		} else if (task === 'readRuns' || task === 'readPipeline') {
			result = await measureSmallReads(task, db, client);
		} else if (task.startsWith('twoWrite')) {
			result = await measureTransactions(task, db, client);
		} else {
			result = await measureDocuments(task, db, client, fixture);
		}
		return { version, ...result };
	} finally {
		if (db !== undefined) {
			try {
				if (wsUrl !== undefined)
					await db.query('REMOVE NAMESPACE IF EXISTS benchmark;');
			} finally {
				await db.close();
			}
		}
	}
};

const prepareFixture = async (directory) => {
	const { default: ts } = await import('typescript');
	// The checked-in generated sources use ESM .js imports. Transpile their
	// two modules outside the checkout so each worker imports the real codec.
	await writeFile(join(directory, 'package.json'), '{"type":"module"}');
	const moduleUrls = new Map(
		['effect', 'effect-surrealkit'].map((name) => [
			name,
			import.meta.resolve(name),
		]),
	);
	for (const name of ['ids', 'doc']) {
		const url = new URL(
			`../test/golden/corpus/${name}.ts`,
			import.meta.url,
		);
		const source = await readFile(url, 'utf8');
		const { outputText } = ts.transpileModule(source, {
			compilerOptions: {
				target: ts.ScriptTarget.ES2022,
				module: ts.ModuleKind.ESNext,
			},
		});
		const relocated = outputText.replace(
			/from ["'](effect|effect-surrealkit)["']/gu,
			(_, name) => `from ${JSON.stringify(moduleUrls.get(name))}`,
		);
		await writeFile(join(directory, `${name}.js`), relocated);
	}
	return pathToFileURL(join(directory, 'doc.js')).href;
};

const findPort = async () => {
	const socket = createServer().listen(0, '127.0.0.1');
	await once(socket, 'listening');
	const { port } = socket.address();
	await promisify(socket.close.bind(socket))();
	return port;
};

// The benchmark server's root password is made for the run, and its workers
// read it from their environment.
const PASSWORD_ENV = 'SURREALKIT_BENCH_PASSWORD';

const withServer = async (binary, use) => {
	const port = await findPort();
	const host = `127.0.0.1:${port}`;
	const password = randomUUID();
	const child = spawn(
		binary,
		[
			'start',
			'--bind',
			host,
			'--user',
			'root',
			'--pass',
			password,
			'--log',
			'error',
			'memory',
		],
		{ stdio: ['ignore', 'ignore', 'inherit'] },
	);
	await once(child, 'spawn');
	const exited = once(child, 'exit');
	try {
		for (let attempt = 0; attempt < 100; attempt++) {
			if (child.exitCode !== null)
				throw new Error('benchmark server exited');
			const ready = await fetch(`http://${host}/health`, {
				signal: AbortSignal.timeout(250),
			}).then(
				(response) => response.ok,
				() => false,
			);
			if (ready) return await use(`ws://${host}/rpc`, password);
			await delay(100);
		}
		throw new Error('benchmark server did not start');
	} finally {
		if (child.exitCode === null && child.signalCode === null) child.kill();
		await exited;
	}
};

const execWorker = promisify(execFile);
const median = (values) =>
	[...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];

const collect = async (fixture, wsUrl, password) => {
	const results = {};
	let version;
	for (const task of tasks) {
		const runs = [];
		for (let index = 0; index < samples; index++) {
			const { stdout } = await execWorker(
				process.execPath,
				[
					'--expose-gc',
					fileURLToPath(import.meta.url),
					'--worker',
					task,
					fixture,
					...(wsUrl === undefined ? [] : [wsUrl]),
				],
				{
					timeout: 120_000,
					env:
						password === undefined
							? process.env
							: { ...process.env, [PASSWORD_ENV]: password },
				},
			);
			const { version: observedVersion, ...sample } = JSON.parse(stdout);
			version ??= observedVersion;
			assert.equal(observedVersion, version);
			runs.push(sample);
		}
		const fields = Object.keys(runs[0]);
		results[task] = {
			median: Object.fromEntries(
				fields.map((field) => [
					field,
					median(runs.map((run) => run[field])),
				]),
			),
			samples: runs,
		};
		console.error(
			`[effect-surrealkit] ${wsUrl === undefined ? 'embedded' : 'websocket'} ${task}: ${samples} validated samples`,
		);
	}
	return {
		transport: wsUrl === undefined ? 'embedded' : 'websocket',
		version,
		results,
	};
};

const main = async () => {
	const directory = await mkdtemp(join(tmpdir(), 'surrealkit-bench-'));
	try {
		const fixture = await prepareFixture(directory);
		const engines = [await collect(fixture)];
		if (process.env.SURREAL_BIN) {
			engines.push(
				await withServer(process.env.SURREAL_BIN, (url, password) =>
					collect(fixture, url, password),
				),
			);
		}
		const versionOf = async (name) =>
			JSON.parse(
				await readFile(
					new URL(
						`../node_modules/${name}/package.json`,
						import.meta.url,
					),
					'utf8',
				),
			).version;
		return {
			schemaVersion: 1,
			machine: {
				platform: platform(),
				architecture: arch(),
				cpu: cpus()[0]?.model,
				memoryGiB: Math.round(totalmem() / 2 ** 30),
			},
			runtime: {
				node: process.versions.node,
				effect: await versionOf('effect'),
				sdk: await versionOf('surrealdb'),
			},
			parameters: {
				samples,
				inserts,
				chunk,
				concurrency,
				smallReads,
				largeRows,
				transactionCycles,
			},
			engines,
		};
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
};

const [, , mode, ...args] = process.argv;
const output = mode === '--worker' ? await runWorker(...args) : await main();
process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
