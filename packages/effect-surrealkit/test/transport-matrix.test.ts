import {
	existsSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { inspect } from 'node:util';

import { Cause, Effect, Exit, Result, Schema, Stream } from 'effect';
import {
	CborCodec,
	DateTime,
	Duration,
	GeometryPoint,
	RecordId,
	UnavailableFeatureError,
	UnsupportedFeatureError,
	Uuid,
} from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { makeWith } from '../src/client/client.js';
import {
	bind,
	type Client,
	make as makeClient,
	StatementEncodeError,
	StatementThrown,
	StatementUnsupported,
	timed,
} from '../src/client/index.js';
import {
	type SlotAnswer,
	sdkTransport,
	type Transport,
} from '../src/client/internal/transport.js';
import * as S from '../src/index.js';
import { live, make } from '../src/statement/index.js';
import type { Codec } from '../src/statement/statement.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	interactiveTransactions,
	liveQueries,
	startEngine,
} from './engine.js';

const read = make({
	name: 'matrix read',
	sql: '42;',
	params: {},
	result: Schema.Number,
	writes: false,
});
const readList = make({
	name: 'matrix list',
	sql: '[1, 2];',
	params: {},
	result: Schema.Array(Schema.Number),
	writes: false,
});
const insert = make({
	name: 'matrix insert',
	sql: 'INSERT INTO matrix_bulk $rows RETURN NONE;',
	params: { rows: Schema.Array(Schema.Struct({ label: S.String })) },
	result: Schema.Tuple([]),
	writes: true,
});
const liveId = S.RecordIdFor('matrix_live', { key: S.String });
const liveTable = {
	name: 'matrix_live',
	id: liveId,
	select: Schema.Struct({ id: liveId, label: S.String }),
} as const;

const directory = new URL('./golden/transports/', import.meta.url);
const readme = new URL('../README.md', import.meta.url);
const updateGoldens = process.env.SURREAL_MATRIX_UPDATE === '1';
const printTable = process.env.SURREAL_MATRIX_PRINT === '1';

const operationNames = {
	run: '`run`',
	transaction: '`transaction`',
	pipelineStatement: '`pipeline`, each statement',
	pipelineTransaction: '`pipeline`, each transaction',
	stream: '`stream`',
	withTransaction: '`withTransaction`, one statement',
	live: '`liveScoped`, start and kill',
	bulkAtomic: '`bulk`, two chunks, atomic',
	bulkNonAtomic: '`bulk`, two chunks, non-atomic',
} as const;

type Operation = keyof typeof operationNames;
const operations = Object.keys(operationNames) as ReadonlyArray<Operation>;
const support = Schema.Union([
	Schema.Struct({
		supported: Schema.Literal(true),
		requests: Schema.Int,
		serverTiming: Schema.Boolean,
	}),
	Schema.Struct({ supported: Schema.Literal(false) }),
	Schema.Struct({ supported: Schema.Literal('pending') }),
]);

const matrix = Schema.Struct({
	sdk: Schema.String,
	engine: Schema.String,
	transport: Schema.Literals(['embedded', 'ws', 'http']),
	signedZero: Schema.Struct({
		checkedBinding: Schema.Literal('refused before dispatch'),
		unsafeBinding: Schema.Literal('returned -0'),
		engineLiteral: Schema.Literal('preserved'),
	}),
	operations: Schema.Record(Schema.Literals(operations), support),
});
type Matrix = typeof matrix.Type;
type Observation = typeof support.Type;

const readJson = (file: URL): unknown => JSON.parse(readFileSync(file, 'utf8'));
const readMatrix = Schema.decodeUnknownSync(matrix, {
	onExcessProperty: 'error',
});
const sdkVersion = () => {
	const entry = pathToFileURL(
		createRequire(import.meta.url).resolve('surrealdb'),
	);
	return Schema.decodeUnknownSync(
		Schema.Struct({
			name: Schema.Literal('surrealdb'),
			version: Schema.String,
		}),
	)(readJson(new URL('../package.json', entry))).version;
};

const observe = (engine: Engine) => {
	const sdk = sdkTransport(engine.db);
	const counts = { requests: 0, attempts: 0, serverTiming: false };
	const sent = () => {
		counts.requests++;
		counts.attempts++;
	};
	const slots = (answers: ReadonlyArray<SlotAnswer>) => {
		counts.serverTiming ||= answers.some(
			({ server }) => server !== undefined,
		);
		return answers;
	};
	const { begin, live } = sdk;
	const transport: Transport = {
		collect: (request, signal) => {
			sent();
			return sdk.collect(request, signal);
		},
		send: async (request, signal) => {
			sent();
			return slots(await sdk.send(request, signal));
		},
		stream: async function* (request, signal) {
			sent();
			for await (const frame of sdk.stream(request, signal)) {
				if (frame.kind === 'done' && frame.server !== undefined)
					counts.serverTiming = true;
				yield frame;
			}
		},
		...(begin === undefined
			? {}
			: {
					begin: async (signal) => {
						sent();
						try {
							const handle = await begin(signal);
							return {
								send: async (...args) => {
									sent();
									return slots(await handle.send(...args));
								},
								commit: () => {
									sent();
									return handle.commit();
								},
								cancel: () => {
									sent();
									return handle.cancel();
								},
							};
						} catch (cause) {
							// The SDK detects missing interactive support locally, before sending begin.
							if (
								cause instanceof UnsupportedFeatureError ||
								cause instanceof UnavailableFeatureError
							)
								counts.requests--;
							throw cause;
						}
					},
				}),
		...(live === undefined
			? {}
			: {
					live: async (request, signal) => {
						sent();
						const handle = await live(request, signal);
						return {
							messages: handle.messages,
							kill: () => {
								sent();
								return handle.kill();
							},
						};
					},
				}),
	};
	return { client: makeWith(transport), counts };
};

/**
 * The message a client operation failed with while it was still a
 * placeholder. Such an operation is observed as Pending, not as a failure;
 * no operation is a placeholder now, and the matrix test requires that none
 * is pending.
 */
const pending =
	/^effect-surrealkit: (bulk|stream|liveScoped|live builder) is not implemented yet\b/u;

const probe = async <A, E>(
	engine: Engine,
	name: Operation,
	operation: (client: Client) => Effect.Effect<A, E>,
	verify: (answer: A) => void | Promise<void>,
	supported = true,
): Promise<Observation> => {
	const { client, counts } = observe(engine);
	const [result, timings] = await Effect.runPromise(
		timed(
			Effect.suspend(() => operation(client)).pipe(
				Effect.timeout('10 seconds'),
				Effect.exit,
			),
		),
	);
	if (Exit.isFailure(result)) {
		const error = Cause.squash(result.cause);
		if (error instanceof Error && pending.test(error.message))
			return { supported: 'pending' };
		if (!supported && error instanceof StatementUnsupported) {
			if (name === 'withTransaction') expect(counts.requests).toBe(0);
			return { supported: false };
		}
		throw new Error(`${name} failed: ${String(error)}`, { cause: error });
	}
	expect(supported, name).toBe(true);
	await verify(result.value);
	const reportedSends = timings.reduce(
		(count, request) => count + request.sends,
		0,
	);
	expect(
		reportedSends,
		`${name}: client timing sends and observed transport attempts`,
	).toBe(counts.attempts);
	expect(
		timings.some(({ server }) => server !== undefined),
		`${name}: client server timing and observed transport durations`,
	).toBe(counts.serverTiming);
	return {
		supported: true,
		requests: counts.requests,
		serverTiming: counts.serverTiming,
	};
};

const heading = (golden: Matrix): string =>
	`${golden.engine} ${golden.transport === 'ws' ? 'WebSocket' : golden.transport === 'http' ? 'HTTP' : 'embedded'}`;

const cell = (row: Observation): string => {
	if (row.supported === 'pending') return 'Pending';
	if (!row.supported) return 'Unsupported';
	const timing = row.serverTiming ? 'server timing' : 'client timing only';
	return `${row.requests} ${row.requests === 1 ? 'request' : 'requests'}, ${timing}`;
};

const tableText = (goldens: ReadonlyArray<Matrix>): string => {
	const line = (label: string, cells: ReadonlyArray<string>) =>
		`| ${[label, ...cells].join(' | ')} |`;
	return [
		line('Capability', goldens.map(heading)),
		line(
			'---',
			goldens.map(() => '---'),
		),
		line(
			'JavaScript SDK',
			goldens.map(({ sdk }) => sdk),
		),
		...operations.map((name) =>
			line(
				operationNames[name],
				goldens.map((golden) => cell(golden.operations[name])),
			),
		),
		line(
			'Checked `-0` binding',
			goldens.map(({ signedZero }) => signedZero.checkedBinding),
		),
		line(
			'Unchecked `-0` binding',
			goldens.map(({ signedZero }) => signedZero.unsafeBinding),
		),
		line(
			'Engine float literal `-0.0f`',
			goldens.map(({ signedZero }) => signedZero.engineLiteral),
		),
	].join('\n');
};

const floats = Schema.Array(S.Float);
const signedZeroSample = [NaN, Infinity, -Infinity, -0, 0, Number.MIN_VALUE];
const inspectSignedZero = make({
	name: 'inspect signed zero',
	sql: 'RETURN $sample;',
	params: { sample: floats },
	result: floats,
	writes: false,
});
const negativeZeroLiteral = make({
	name: 'engine negative zero',
	sql: 'RETURN -0.0f;',
	params: {},
	result: S.Float,
	writes: false,
});

const observeSignedZero = async (
	engine: Engine,
): Promise<Matrix['signedZero']> => {
	const { client, counts } = observe(engine);
	const [error, timings] = await Effect.runPromise(
		timed(
			Effect.flip(
				client.run(inspectSignedZero, { sample: signedZeroSample }),
			),
		),
	);
	expect(error).toBeInstanceOf(StatementEncodeError);
	expect(error).toMatchObject({
		statement: inspectSignedZero.name,
		binding: 'sample',
		path: [3],
	});
	expect(error.message).toMatch(/^\$sample\[3\] /u);
	expect(counts.attempts).toBe(0);
	expect(timings).toEqual([]);

	const unchecked = await Effect.runPromise(
		client.run(
			inspectSignedZero,
			{ sample: signedZeroSample },
			{ unsafeSkipValueCheck: true },
		),
	);
	expect(unchecked).toEqual([
		NaN,
		Infinity,
		-Infinity,
		-0,
		0,
		Number.MIN_VALUE,
	]);
	expect(unchecked[3]).toBe(-0);
	expect(counts.requests).toBe(1);

	const literal = await Effect.runPromise(client.run(negativeZeroLiteral));
	expect(literal).toBe(-0);
	return {
		checkedBinding: 'refused before dispatch',
		unsafeBinding: 'returned -0',
		engineLiteral: 'preserved',
	};
};

const observeMatrix = async (
	engine: Engine,
	version: string,
): Promise<Matrix> => {
	const equal = (expected: unknown) => (value: unknown) => {
		expect(value).toEqual(expected);
	};
	const readPipeline = [bind(read), bind(readList)] as const;
	const pipelineValue = [Result.succeed(42), Result.succeed([1, 2])] as const;
	const run = await probe(
		engine,
		'run',
		(client) => client.run(read),
		equal(42),
	);
	const transaction = await probe(
		engine,
		'transaction',
		(client) => client.transaction(readPipeline),
		equal([42, [1, 2]] as const),
	);
	const pipelineStatement = await probe(
		engine,
		'pipelineStatement',
		(client) => client.pipeline(readPipeline, { each: 'statement' }),
		equal(pipelineValue),
	);
	const pipelineTransaction = await probe(
		engine,
		'pipelineTransaction',
		(client) => client.pipeline(readPipeline, { each: 'transaction' }),
		equal(pipelineValue),
	);
	const stream = await probe(
		engine,
		'stream',
		(client) => Stream.runCollect(client.stream(readList)),
		equal([1, 2]),
	);
	const withTransaction = await probe(
		engine,
		'withTransaction',
		(client) => client.withTransaction((tx) => tx.run(read)),
		equal(42),
		interactiveTransactions,
	);

	await engine.db.query(
		'DEFINE TABLE matrix_live SCHEMALESS; DEFINE TABLE matrix_bulk SCHEMALESS;',
	);
	const liveProbe = await probe(
		engine,
		'live',
		(client) =>
			Effect.scoped(
				Effect.gen(function* () {
					const events = yield* client.liveScoped(
						live(liveTable).build('watch matrix live'),
					);
					yield* Effect.promise(
						async () =>
							await engine.db.query(
								"CREATE ONLY matrix_live:row SET label = 'observed';",
							),
					);
					return yield* Stream.runCollect(Stream.take(events, 1));
				}),
			),
		(events) => {
			expect(events).toMatchObject([
				{ action: 'CREATE', value: { label: 'observed' } },
			]);
			expect(events[0]?.id.id).toBe('row');
		},
		liveQueries,
	);

	const rows = [{ label: 'a' }, { label: 'b' }, { label: 'c' }];
	const bulkProbe = (atomic: boolean) =>
		probe(
			engine,
			atomic ? 'bulkAtomic' : 'bulkNonAtomic',
			(client) =>
				client.bulk(
					insert,
					{ rows },
					{ over: 'rows', chunk: 2, atomic },
				),
			async (summary) => {
				expect(summary).toEqual({
					rows: 3,
					chunks: 2,
					results: [[], []],
				});
				expect(
					await engine.db.query(
						'SELECT VALUE label FROM matrix_bulk ORDER BY label;',
					),
				).toEqual([['a', 'b', 'c']]);
				await engine.db.query('DELETE matrix_bulk;');
			},
		);
	const bulkAtomic = await bulkProbe(true);
	const bulkNonAtomic = await bulkProbe(false);

	return {
		sdk: sdkVersion(),
		engine: version,
		transport: engine.transport,
		signedZero: await observeSignedZero(engine),
		operations: {
			run,
			transaction,
			pipelineStatement,
			pipelineTransaction,
			stream,
			withTransaction,
			live: liveProbe,
			bulkAtomic,
			bulkNonAtomic,
		},
	};
};

it('README transport table matches every committed golden', () => {
	const files = readdirSync(directory).filter((name) =>
		name.endsWith('.json'),
	);
	expect(files).toEqual(
		expect.arrayContaining([
			'4.0.1-embedded.json',
			'3.3.4-ws.json',
			'3.3.4-http.json',
		]),
	);
	const goldens = files.map((file) => {
		const golden = readMatrix(readJson(new URL(file, directory)));
		expect(file).toBe(`${golden.engine}-${golden.transport}.json`);
		return golden;
	});
	const order = { embedded: 0, ws: 1, http: 2 } as const;
	goldens.sort(
		(left, right) =>
			right.engine.localeCompare(left.engine, 'en', { numeric: true }) ||
			order[left.transport] - order[right.transport],
	);
	const table = tableText(goldens);
	if (printTable) {
		console.info(`\n${table}\n`);
		return;
	}
	const section = readFileSync(readme, 'utf8')
		.split('#### Transports\n')[1]
		?.split('\n#### ')[0];
	const actual = section
		?.split('\n')
		.filter((line) => line.startsWith('| '))
		.join('\n');
	expect(actual).toBe(table);
});

it('the SDK encoder preserves negative zero as a CBOR float', () => {
	const codec = CborCodec.DEFAULT;
	const negativeZero = codec.encode(-0);
	expect(negativeZero).toEqual(Uint8Array.of(0xf9, 0x80, 0));
	expect(codec.decode(negativeZero)).toBe(-0);
	expect(codec.encode(+0)).toEqual(Uint8Array.of(0));
	const negativeZeroFloat = Uint8Array.of(0xfb, 0x80, 0, 0, 0, 0, 0, 0, 0);
	expect(codec.decode(negativeZeroFloat)).toBe(-0);
});
if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] transport matrix skipped: ${engineSkipReason}`,
	);
}

describe.skipIf(!engineConfigured)('disposable engine transport matrix', () => {
	let engine: Engine;
	beforeAll(async () => {
		engine = await startEngine();
		console.info(
			`[effect-surrealkit] transport matrix: ${engine.version} ${engine.transport}`,
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const roundTrip = <C extends Codec>(
		kind: string,
		codec: C,
		sample: C['Type'],
	) => {
		const runtimeCodec: Codec = codec;
		const equivalent = Schema.toEquivalence(runtimeCodec);
		const statement = make({
			name: `return ${kind}`,
			sql: 'RETURN $sample;',
			params: { sample: runtimeCodec },
			result: runtimeCodec,
			writes: false,
		});
		it(`V8.1: preserves ${kind} through client.run`, async () => {
			const client = makeClient(engine.db);
			const actual = await Effect.runPromise(
				client
					.run(statement, { sample })
					.pipe(Effect.timeout('10 seconds')),
			);
			expect(
				equivalent(sample, actual),
				`${engine.version} ${engine.transport}: ${inspect(sample)} became ${inspect(actual)}`,
			).toBe(true);
		});
	};

	roundTrip('int64 beyond the safe integer range', Schema.Array(S.Int64), [
		S.int64(S.INT64_MIN),
		S.int64(-(2n ** 60n + 7n)),
		S.int64(2n ** 60n + 7n),
		S.int64(S.INT64_MAX),
	]);
	roundTrip(
		'decimal',
		S.Decimal,
		S.decimalFromString('12345678901234567890.01234567'),
	);
	roundTrip('float specials and positive zero', floats, [
		NaN,
		Infinity,
		-Infinity,
		+0,
		Number.MIN_VALUE,
	]);
	it('V8.1: refuses checked negative zero and pins unsafe binding and engine read behavior', async () => {
		await observeSignedZero(engine);
	});
	it('the engine preserves literal, string-cast, and SDK-bound negative float zero', async () => {
		const [literal, cast, reciprocal, arrivedAsFloat] =
			await engine.db.query<[number, number, number, boolean]>(
				"RETURN -0.0f; RETURN <float>'-0'; RETURN 1.0f / -0.0f; RETURN type::is_float($sample);",
				{ sample: -0 },
			);
		expect(literal).toBe(-0);
		expect(cast).toBe(-0);
		expect(reciprocal).toBe(-Infinity);
		expect(arrivedAsFloat).toBe(true);
	});
	roundTrip(
		'nanosecond datetime',
		S.DateTime,
		new DateTime('2024-01-02T03:04:05.123456789Z'),
	);
	roundTrip('nanosecond duration', S.Duration, new Duration('1h30m5ns'));
	roundTrip('UUID', S.Uuid, new Uuid('0189a8f6-0000-7000-8000-000000000000'));
	roundTrip('bytes', S.Bytes, Uint8Array.of(0, 1, 127, 128, 255));
	roundTrip('set', S.SetOf(S.String), new Set(['z', 'é', 'a']));
	roundTrip('geometry', S.GeometryPoint, new GeometryPoint([-1.25, 2.5]));
	roundTrip(
		'record ID with composite array key',
		S.RecordIdFor('matrix_record', {
			key: Schema.Tuple([S.Int64, S.String]),
		}),
		new RecordId('matrix_record', [S.int64(2n ** 60n + 7n), 'eu'] as const),
	);
	roundTrip(
		'record ID with composite object key',
		S.RecordIdFor('matrix_record', {
			key: Schema.Struct({ n: S.Int, area: S.String }),
		}),
		new RecordId('matrix_record', { n: 42, area: 'eu' }),
	);
	roundTrip('nested lists and objects', S.SurrealValue, {
		meta: { exact: 2n ** 60n + 7n, absent: null },
		values: [
			[1, 'é'],
			[
				{
					duration: new Duration('1ns'),
					point: new GeometryPoint([-1.25, 2.5]),
				},
			],
		],
	});

	it('V8.2: a wrapped pipeline rolls back only the failing member', async () => {
		await engine.db.query(`
			CREATE ONLY matrix_wrap:first SET hits = 0;
			CREATE ONLY matrix_wrap:middle SET hits = 0;
			CREATE ONLY matrix_wrap:last SET hits = 0;
		`);
		const first = make({
			name: 'first',
			sql: 'UPDATE ONLY matrix_wrap:first SET hits += 1 RETURN NONE;',
			params: {},
			result: Schema.Undefined,
			writes: true,
		});
		const middle = make({
			name: 'middle',
			sql: "UPDATE ONLY matrix_wrap:middle SET hits += 1 RETURN NONE; THROW 'matrix rollback';",
			params: {},
			result: Schema.Undefined,
			throws: ['matrix rollback'],
			slots: 2,
			writes: true,
		});
		const last = make({
			name: 'last',
			sql: 'UPDATE ONLY matrix_wrap:last SET hits += 1 RETURN NONE;',
			params: {},
			result: Schema.Undefined,
			writes: true,
		});
		const result = await Effect.runPromise(
			makeClient(engine.db).pipeline(
				[bind(first), bind(middle), bind(last)],
				{ each: 'transaction' },
			),
		);
		expect(result[0]).toEqual(Result.succeed(undefined));
		expect(result[2]).toEqual(Result.succeed(undefined));
		expect(Result.isFailure(result[1]) && result[1].failure).toBeInstanceOf(
			StatementThrown,
		);
		expect(Result.isFailure(result[1]) && result[1].failure).toMatchObject({
			statement: 'middle',
			reason: 'matrix rollback',
		});
		const [rows] = await engine.db.query<
			[{ id: RecordId; hits: number }[]]
		>('SELECT id, hits FROM matrix_wrap ORDER BY id;');
		expect(rows.map(({ id, hits }) => [id.id, hits])).toEqual([
			['first', 1],
			['last', 1],
			['middle', 0],
		]);
	});

	it('records observed execution capabilities against the transport golden', async ({
		skip,
	}) => {
		const version = engine.version.replace(/^surrealdb-/u, '');
		const file = new URL(
			`${encodeURIComponent(version)}-${engine.transport}.json`,
			directory,
		);
		if (!updateGoldens && !existsSync(file)) {
			const reason = `no transport golden for ${version} ${engine.transport}`;
			console.info(`[effect-surrealkit] ${reason}`);
			skip(reason);
			return;
		}
		const observation = await observeMatrix(engine, version);
		if (updateGoldens) {
			mkdirSync(directory, { recursive: true });
			writeFileSync(file, `${JSON.stringify(observation, null, '\t')}\n`);
			return;
		}
		expect(observation).toEqual(readMatrix(readJson(file)));
		expect(
			operations.filter(
				(name) => observation.operations[name].supported === 'pending',
			),
		).toEqual([]);
	});
});
