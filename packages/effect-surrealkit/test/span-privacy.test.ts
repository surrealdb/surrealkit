import { Cause, Effect, Exit, Result, Schema, Stream, Tracer } from 'effect';
import { expect, it } from 'vitest';
import { bind, StatementRequestError } from '../src/client/index.js';
import { RecordIdFor } from '../src/record-id.js';
import { live, make } from '../src/statement/index.js';
import { events, failure, fake } from '../src/testing/index.js';

const privateValue = 'private database value';
const privateError = 'private engine error';
const rows = make({
	name: 'rows',
	sql: 'SELECT VALUE name FROM item;',
	params: {},
	result: Schema.Array(Schema.String),
	writes: false,
});
const broken = make({ ...rows, name: 'broken' });
const write = make({
	name: 'write',
	sql: '$rows;',
	params: { rows: Schema.Array(Schema.String) },
	result: Schema.Array(Schema.String),
	writes: true,
});
const watch = live({
	name: 'item',
	id: RecordIdFor('item'),
	select: Schema.Struct({ name: Schema.String }),
}).build('watch');

const tracing = () => {
	const spans: Array<Tracer.NativeSpan> = [];
	const tracer = Tracer.make({
		span: (options) => {
			const span = new Tracer.NativeSpan(options);
			spans.push(span);
			return span;
		},
	});
	const span = (name: string) => {
		const observed = spans.find((span) => span.name === name);
		if (observed === undefined) throw new Error(`No span named ${name}`);
		return observed;
	};
	const exit = (name: string) => {
		const { status } = span(name);
		if (status._tag !== 'Ended') throw new Error(`Open span ${name}`);
		return status.exit;
	};
	const run = <A, E>(effect: Effect.Effect<A, E>) =>
		Effect.runPromise(effect.pipe(Effect.withTracer(tracer)));
	return { span, exit, run };
};

it('returns statement, stateless transaction, pipeline and bulk results without retaining them in spans', async () => {
	const trace = tracing();
	const client = fake({
		rows: [privateValue],
		broken: failure(privateError),
		write: [privateValue],
	});
	expect(await trace.run(client.run(rows))).toEqual([privateValue]);
	expect(await trace.run(client.transaction([bind(rows)]))).toEqual([
		[privateValue],
	]);
	const [first, second] = await trace.run(
		client.pipeline([bind(rows), bind(broken)]),
	);
	expect(first).toEqual(Result.succeed([privateValue]));
	expect(second).toMatchObject({
		_tag: 'Failure',
		failure: { cause: { message: privateError } },
	});
	expect(
		await trace.run(
			client.bulk(
				write,
				{ rows: [privateValue] },
				{ over: 'rows', atomic: false, chunk: 1 },
			),
		),
	).toEqual({ chunks: 1, rows: 1, results: [[privateValue]] });
	for (const name of [
		'rows',
		'transaction(rows)',
		'pipeline(rows, broken)',
		'bulk(write)',
	]) {
		expect(trace.exit(name)).toEqual(Exit.void);
	}
});

it.each([
	'run',
	'stream',
] as const)('preserves the caller error from %s and omits its cause from the span exit', async (operation) => {
	const trace = tracing();
	const client = fake({ broken: failure(privateError) });
	const request =
		operation === 'run'
			? client.run(broken)
			: Stream.runCollect(client.stream(broken));
	const error = await trace.run(Effect.flip(request));
	expect(error).toBeInstanceOf(StatementRequestError);
	expect(error).toMatchObject({ cause: { message: privateError } });
	expect(trace.span('broken').attributes.get('error.type')).toBe(
		'StatementRequestError',
	);
	expect(trace.exit('broken')).toEqual(Exit.fail('Failure'));
});

it('erases transaction and live handles and keeps child spans under the original parent', async () => {
	const trace = tracing();
	const client = fake({ rows: [privateValue], watch: events([]) });
	const result = await trace.run(
		Effect.gen(function* () {
			return yield* client.withTransaction((tx) => tx.run(rows));
		}).pipe(Effect.withSpan('caller')),
	);
	expect(result).toEqual([privateValue]);
	expect(trace.exit('caller')).toEqual(Exit.succeed([privateValue]));
	const transaction = trace.span('withTransaction');
	expect(transaction.parent).toMatchObject({
		_tag: 'Some',
		value: trace.span('caller'),
	});
	for (const name of [
		'withTransaction.begin',
		'rows',
		'withTransaction.commit',
	]) {
		expect(trace.span(name).parent).toMatchObject({
			_tag: 'Some',
			value: transaction,
		});
		expect(trace.exit(name)).toEqual(Exit.void);
	}
	expect(trace.exit('withTransaction')).toEqual(Exit.void);
	await trace.run(Effect.scoped(client.liveScoped(watch)));
	expect(trace.exit('live(watch)')).toEqual(Exit.void);
	expect(trace.exit('kill(watch)')).toEqual(Exit.void);
});

it.each([
	[Effect.fail, Cause.isFailReason, Exit.fail('Failure')],
	[Effect.die, Cause.isDieReason, Exit.die('Defect')],
] as const)('keeps transaction body failures and defects in the caller exit only', async (fail, isReason, expected) => {
	const trace = tracing();
	const client = fake({});
	const error = { _tag: privateError, payload: privateValue };
	const exit = await trace.run(
		client
			.withTransaction(() => fail(error))
			.pipe(Effect.withSpan('caller'), Effect.exit),
	);
	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isSuccess(exit)) throw new Error('Expected a failure');
	const reason = exit.cause.reasons[0];
	if (reason === undefined) throw new Error('Expected a cause reason');
	expect(isReason(reason)).toBe(true);
	expect(reason).toHaveProperty(
		Cause.isFailReason(reason) ? 'error' : 'defect',
		error,
	);
	expect(trace.exit('caller')).toBe(exit);
	expect(trace.exit('withTransaction')).toEqual(expected);
	expect(trace.exit('withTransaction.cancel')).toEqual(Exit.void);
});

it('records interruption without retaining the caller’s interrupt cause', async () => {
	const trace = tracing();
	const client = fake({});
	const exit = await trace.run(
		client
			.withTransaction(() => Effect.interrupt)
			.pipe(Effect.withSpan('caller'), Effect.exit),
	);
	expect(Exit.hasInterrupts(exit)).toBe(true);
	expect(trace.exit('caller')).toBe(exit);
	expect(trace.exit('withTransaction')).toEqual(Exit.interrupt());
	expect(trace.exit('withTransaction.cancel')).toEqual(Exit.void);
});

it('records a successful stream exit without downstream values', async () => {
	const trace = tracing();
	const client = fake({ rows: [privateValue] });
	expect(await trace.run(Stream.runCollect(client.stream(rows)))).toEqual([
		privateValue,
	]);
	expect(trace.exit('rows')).toEqual(Exit.void);
});
