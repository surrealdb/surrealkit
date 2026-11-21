import {
	type Duration,
	Effect,
	type Result,
	Schema,
	type Scope,
	Stream,
} from 'effect';

import {
	type BulkOptions,
	type BulkProgress,
	type BulkSummary,
	type BulkWriteError,
	bind,
	type Client,
	type ListBindingsOf,
	type LiveError,
	type LiveEvent,
	type LiveQueryClosed,
	make as makeClient,
	type PipelineOptions,
	type PipelineResults,
	pipeline,
	type Queryable,
	type RequestTiming,
	run,
	type StatementConflict,
	type StatementDecodeError,
	type StatementEncodeError,
	type StatementError,
	type StatementRequestError,
	type StatementScopeError,
	type StatementTiming,
	type StatementUnsupported,
	SurrealClient,
	stream,
	type TransactionClient,
	type TransactionError,
	type TransactionOptions,
	type TransactionOutcomeUnknown,
	timed,
	withTransaction,
} from '../../src/client/index.js';
import * as SurrealSchema from '../../src/index.js';
import {
	type Expr,
	eq,
	type FitsUpdateRows,
	type Live,
	type LiveStatement,
	type LiveStatementTypeId,
	live,
	make,
	type NoParams,
	type NoResult,
	param,
	renameNames,
	type Statement,
	type StoredRow,
	updateEach,
} from '../../src/statement/index.js';
import {
	PersonId,
	type PersonRow,
	PersonTable,
} from '../golden/example/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

const count = make({
	name: 'number',
	sql: '42;',
	params: {},
	result: Schema.Number,
});

const names = make({
	name: 'names',
	sql: '$names;',
	params: { names: Schema.Array(Schema.String), include: Schema.Boolean },
	result: Schema.Array(Schema.String),
	throws: ['miss'],
});

const rows = Schema.Array(
	Schema.Struct({
		id: PersonId,
		set: Schema.Struct({ name: SurrealSchema.String }),
	}),
);

type RowsBinding = { readonly rows: typeof rows };
type OneLive = LiveEvent<{ readonly name: string }, typeof PersonId.Type>;
type PipelineFailure =
	| StatementEncodeError
	| StatementScopeError
	| StatementRequestError;

export const clientContract = (session: Queryable, client: Client): void => {
	same<ReturnType<typeof makeClient>, Client>(true);
	makeClient({ query: session.query, transaction: session.transaction });
	const bounds = [
		bind(count),
		bind(names, { names: ['Ada'], include: true }),
	] as const;
	const options: PipelineOptions = {
		each: 'transaction',
		unsafeSkipValueCheck: true,
	};
	const fromClient = client.pipeline(bounds, options);
	const fromContext = pipeline(bounds, options);
	type Pipeline = readonly [
		Result.Result<number, StatementError<never>>,
		Result.Result<ReadonlyArray<string>, StatementError<'miss'>>,
	];
	same<PipelineResults<typeof bounds>, Pipeline>(true);
	same<typeof fromClient, Effect.Effect<Pipeline, PipelineFailure>>(true);
	same<
		typeof fromContext,
		Effect.Effect<Pipeline, PipelineFailure, SurrealClient>
	>(true);

	const timedContext = timed(fromContext);
	same<
		typeof timedContext,
		Effect.Effect<
			readonly [Pipeline, ReadonlyArray<RequestTiming>],
			PipelineFailure,
			SurrealClient
		>
	>(true);
	same<
		RequestTiming['kind'],
		'run' | 'transaction' | 'pipeline' | 'stream' | 'bulk' | 'interactive'
	>(true);
	same<
		Pick<RequestTiming, 'encode' | 'wait' | 'decode' | 'total'>,
		{
			readonly encode: Duration.Duration;
			readonly wait: Duration.Duration;
			readonly decode: Duration.Duration;
			readonly total: Duration.Duration;
		}
	>(true);
	same<RequestTiming['statements'], ReadonlyArray<StatementTiming>>(true);
	same<StatementTiming['server'], Duration.Duration | undefined>(true);

	const transactionOptions: TransactionOptions = {};
	const body = (tx: TransactionClient) =>
		Effect.gen(function* () {
			type ScopedRun = typeof tx.run<
				typeof count.params,
				typeof count.result,
				never
			>;
			type ScopedTransaction = typeof tx.transaction<
				readonly [(typeof bounds)[0]]
			>;
			type ScopedBulk = typeof tx.bulk<
				typeof names.params,
				typeof names.result,
				'miss',
				'names'
			>;
			same<
				keyof NonNullable<Parameters<ScopedRun>[2]>,
				'unsafeSkipValueCheck'
			>(true);
			same<
				keyof NonNullable<Parameters<ScopedTransaction>[1]>,
				'unsafeSkipValueCheck'
			>(true);
			same<
				Extract<keyof Parameters<ScopedBulk>[2], 'retry' | 'atomic'>,
				never
			>(true);
			const value = yield* tx.run(
				count,
				{},
				{ unsafeSkipValueCheck: true },
			);
			const [again] = yield* tx.transaction([bind(count)], {
				unsafeSkipValueCheck: true,
			});
			const summary = yield* tx.bulk(
				names,
				{ names: ['Ada'], include: true },
				{ over: 'names' },
			);
			return { value, again, summary };
		});
	const interactive = client.withTransaction(body, transactionOptions);
	same<
		typeof interactive,
		Effect.Effect<
			{
				value: number;
				again: number;
				summary: BulkSummary<ReadonlyArray<string>>;
			},
			StatementError<'miss'> | BulkWriteError<'miss'> | TransactionError
		>
	>(true);
	const throughContext = withTransaction(run(count), transactionOptions);
	same<
		typeof throughContext,
		Effect.Effect<
			number,
			StatementError<never> | TransactionError,
			SurrealClient
		>
	>(true);
	same<
		TransactionError,
		| StatementConflict
		| StatementRequestError
		| StatementUnsupported
		| TransactionOutcomeUnknown
	>(true);

	const input = Stream.fromEffect(
		Effect.service(SurrealClient).pipe(
			Effect.flatMap(() => Effect.fail('source' as const)),
		),
	);
	const bulkOptions: BulkOptions<'names', string, 'source', SurrealClient> = {
		over: 'names',
		chunk: 1,
		atomic: false,
		beforeChunk: ({ rows }) => {
			same<typeof rows, ReadonlyArray<string>>(true);
			return Effect.void;
		},
		onProgress: (_progress) => {
			same<typeof _progress, BulkProgress>(true);
			return Effect.void;
		},
	};
	same<ListBindingsOf<typeof names.params>, 'names'>(true);
	const bulk = client.bulk(
		names,
		{ names: input, include: true },
		bulkOptions,
	);
	same<
		typeof bulk,
		Effect.Effect<
			BulkSummary<ReadonlyArray<string>>,
			| StatementError<'miss'>
			| BulkWriteError<'miss'>
			| StatementUnsupported
			| 'source',
			SurrealClient
		>
	>(true);

	const clientStream = client.stream(names, {
		names: ['Ada'],
		include: true,
	});
	const contextStream = stream(names, { names: ['Ada'], include: true });
	same<
		typeof clientStream,
		Stream.Stream<string, StatementError<'miss'> | StatementUnsupported>
	>(true);
	same<
		typeof contextStream,
		Stream.Stream<
			string,
			StatementError<'miss'> | StatementUnsupported,
			SurrealClient
		>
	>(true);
};

export const statementContract = (client: Client): void => {
	const updateRows = param('rows', rows);
	same<
		FitsUpdateRows<typeof PersonTable, typeof rows, typeof updateRows>,
		Expr<typeof rows, RowsBinding>
	>(true);
	const update = updateEach(PersonTable, updateRows).build('update rows');
	const updateIgnoringMissing = updateEach(PersonTable, updateRows, {
		missing: 'ignore',
	}).build();
	same<
		typeof update,
		Statement<RowsBinding, NoResult<'many'>, 'Record not found'>
	>(true);
	same<
		typeof updateIgnoringMissing,
		Statement<RowsBinding, NoResult<'many'>, never>
	>(true);
	same<ReturnType<typeof renameNames>, string | undefined>(true);
	renameNames('$amount', { amount: 'amount__1' });

	const builder = live(PersonTable);
	same<
		typeof builder,
		Live<typeof PersonTable, NoParams, StoredRow<typeof PersonTable>>
	>(true);
	const everyPerson = builder.build('everyone');
	same<
		typeof everyPerson,
		LiveStatement<NoParams, StoredRow<typeof PersonTable>, typeof PersonId>
	>(true);
	same<
		(typeof everyPerson)[typeof LiveStatementTypeId],
		typeof LiveStatementTypeId
	>(true);
	same<typeof everyPerson.row.Type, PersonRow>(true);
	const watch = builder
		.where((row) => eq(row.name, param('name', SurrealSchema.String)))
		.project((row) => ({ name: row.name }))
		.build('one name');
	const scoped = client.liveScoped(watch, { name: 'Ada' });
	const events = client.live(watch, { name: 'Ada' });
	same<
		typeof scoped,
		Effect.Effect<Stream.Stream<OneLive, LiveError>, LiveError, Scope.Scope>
	>(true);
	same<typeof events, Stream.Stream<OneLive, LiveError>>(true);
	same<
		LiveError,
		| StatementEncodeError
		| StatementRequestError
		| StatementDecodeError
		| StatementUnsupported
		| LiveQueryClosed
	>(true);
};
