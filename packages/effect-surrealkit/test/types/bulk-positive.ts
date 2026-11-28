import { Effect, Schedule, Schema, Stream } from 'effect';

import {
	type BulkProgress,
	type BulkSummary,
	type Client,
	SurrealClient,
	type TransactionClient,
	type TransactionOutcomeUnknown,
} from '../../src/client/index.js';
import { Int, RecordIdFor } from '../../src/index.js';
import {
	insert,
	param,
	type Thrown,
	updateEach,
} from '../../src/statement/index.js';

const id = RecordIdFor('bulk_row');
const Row = {
	name: 'bulk_row',
	id,
	select: Schema.Struct({ id, n: Int }),
	create: Schema.Struct({ n: Int }),
	merge: Schema.Struct({ n: Schema.optionalKey(Int) }),
} as const;
const createRows = Schema.Array(Schema.Struct({ n: Int }));
const patches = Schema.Array(
	Schema.Struct({ id, set: Schema.Struct({ n: Int }) }),
);
const write = insert(Row, param('rows', createRows)).returning('none').build();
const required = updateEach(Row, param('rows', patches)).build();
const ignored = updateEach(Row, param('rows', patches), {
	missing: 'ignore',
}).build();

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

same<Thrown<typeof required>, 'Record not found'>(true);
same<Thrown<typeof ignored>, never>(true);

export const acceptRoot = (client: Client) => {
	const rows = Stream.fromEffect(
		Effect.service(SurrealClient).pipe(
			Effect.andThen(Effect.fail('source' as const)),
		),
	);
	const writeAll = client.bulk(
		write,
		{ rows },
		{
			over: 'rows',
			chunk: 4,
			retry: Schedule.recurs(1),
			beforeChunk: ({ rows: original }) => {
				same<typeof original, ReadonlyArray<{ readonly n: number }>>(
					true,
				);
				return Effect.fail('before' as const);
			},
			onProgress: (_progress) => {
				same<typeof _progress, BulkProgress>(true);
				return Effect.void;
			},
		},
	);
	same<
		Extract<Effect.Error<typeof writeAll>, TransactionOutcomeUnknown>,
		TransactionOutcomeUnknown
	>(true);
	same<
		Extract<Effect.Error<typeof writeAll>, 'source' | 'before'>,
		'source' | 'before'
	>(true);
	same<Effect.Services<typeof writeAll>, SurrealClient>(true);
	same<Effect.Success<typeof writeAll>, BulkSummary<readonly []>>(true);
	return writeAll;
};

export const viaService = Effect.gen(function* () {
	const client = yield* SurrealClient;
	return yield* acceptRoot(client);
});

export const acceptScoped = (tx: TransactionClient) => {
	const bulk = tx.bulk(
		write,
		{ rows: new Set([{ n: 1 }]) },
		{ over: 'rows' },
	);
	same<Extract<Effect.Error<typeof bulk>, TransactionOutcomeUnknown>, never>(
		true,
	);
	same<Effect.Success<typeof bulk>, BulkSummary<readonly []>>(true);
	return bulk;
};
