import { Effect, type Schedule } from 'effect';

import type { ElementOf } from '../statement/expr.js';
import type {
	Codec,
	Input,
	Params,
	Statement,
} from '../statement/statement.js';
import type {
	BulkInput,
	BulkOptions,
	BulkSummary,
	ListBindingsOf,
} from './bulk.js';
import type {
	BoundStatements,
	Client,
	Results,
	RunArgsWithOptions,
	RunOptions,
	ThrownByAny,
} from './client.js';
import type {
	BulkWriteError,
	StatementConflict,
	StatementError,
	StatementRequestError,
	StatementUnsupported,
	TransactionOutcomeUnknown,
} from './errors.js';
import type { Transport } from './internal/transport.js';

export interface TransactionOptions {
	/** Retry the whole transaction body on a statement or commit conflict. */
	readonly retry?: Schedule.Schedule<unknown, StatementConflict>;
}

type TransactionRunOptions = Omit<RunOptions, 'retry'>;

export interface TransactionClient {
	readonly run: <
		P extends Params,
		R extends Codec,
		T extends string,
		I = Input<P>,
	>(
		statement: Statement<P, R, T>,
		...args: RunArgsWithOptions<P, I, TransactionRunOptions>
	) => Effect.Effect<R['Type'], StatementError<T>>;

	readonly transaction: <const S extends ReadonlyArray<unknown>>(
		statements: BoundStatements<S>,
		options?: TransactionRunOptions,
	) => Effect.Effect<Results<S>, StatementError<ThrownByAny<S>>>;

	readonly bulk: <
		P extends Params,
		R extends Codec,
		T extends string,
		K extends ListBindingsOf<P>,
		E = never,
		C = never,
	>(
		statement: Statement<P, R, T>,
		input: BulkInput<P, K, E, C>,
		options: Omit<
			BulkOptions<K, ElementOf<P[K]['Type']>, E, C>,
			'retry' | 'atomic'
		>,
	) => Effect.Effect<
		BulkSummary<R['Type']>,
		StatementError<T> | BulkWriteError<T> | StatementUnsupported | E,
		C
	>;
}

export type TransactionError =
	| StatementConflict
	| StatementRequestError
	| StatementUnsupported
	| TransactionOutcomeUnknown;

const unimplemented = () =>
	Effect.die(
		new Error(
			'effect-surrealkit: withTransaction is not implemented yet (lane L4)',
		),
	);

export const makeWithTransaction = (
	_transport: Transport,
): Client['withTransaction'] => unimplemented;

export const withTransactionContext = <A, E, R>(
	_client: Client,
	_effect: Effect.Effect<A, E, R>,
	_options: TransactionOptions | undefined,
): Effect.Effect<A, E | TransactionError, R> => unimplemented();
