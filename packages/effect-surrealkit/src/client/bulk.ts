import { Effect, type Schedule, type Stream } from 'effect';

import type { ElementOf } from '../statement/expr.js';
import type { Input, Params } from '../statement/statement.js';
import type { Client } from './client.js';
import type { StatementConflict } from './errors.js';
import type { TransactionHandle, Transport } from './internal/transport.js';

/** The names of the bindings whose decoded values are lists. */
export type ListBindingsOf<P extends Params> = {
	[K in keyof P]-?: [P[K]['Type']] extends [never]
		? never
		: [P[K]['Type']] extends [ReadonlyArray<unknown>]
			? K
			: never;
}[keyof P] &
	string;

export type BulkInput<
	P extends Params,
	K extends ListBindingsOf<P>,
	E,
	R,
> = Omit<Input<P>, K> & {
	readonly [Key in K]:
		| Iterable<ElementOf<P[Key]['Type']>>
		| Stream.Stream<ElementOf<P[Key]['Type']>, E, R>;
};

export interface BulkProgress {
	/** Chunks finished so far. */
	readonly chunks: number;
	/** Rows finished so far. */
	readonly rows: number;
	/** Total row count when the input is a collection. */
	readonly total: number | undefined;
}

export interface BulkOptions<K extends string, Row, E = never, R = never> {
	/** The list binding that holds the rows. */
	readonly over: K;
	/** Rows per request. A positive integer; defaults to 500. */
	readonly chunk?: number;
	/** Commit all chunks together (default) or each chunk separately. */
	readonly atomic?: boolean;
	/** Called with the original rows before sending each chunk. Its failure stops the write. */
	readonly beforeChunk?: (chunk: {
		readonly index: number;
		readonly rows: ReadonlyArray<Row>;
	}) => Effect.Effect<void, E, R>;
	/** Called once each chunk is acknowledged. */
	readonly onProgress?: (
		progress: BulkProgress,
	) => Effect.Effect<void, never, R>;
	/** Retry the whole atomic write or the conflicting non-atomic chunk. */
	readonly retry?: Schedule.Schedule<unknown, StatementConflict>;
	readonly unsafeSkipValueCheck?: boolean;
}

export interface BulkSummary<A> {
	readonly rows: number;
	readonly chunks: number;
	/** The decoded result of each chunk, in order. */
	readonly results: ReadonlyArray<A>;
}

type BulkSender =
	| { readonly transport: Pick<Transport, 'begin' | 'send'> }
	| { readonly transaction: Pick<TransactionHandle, 'send'> };

export const makeBulk =
	(_sender: BulkSender): Client['bulk'] =>
	() =>
		Effect.die(
			new Error(
				'effect-surrealkit: bulk is not implemented yet (lane L5)',
			),
		);
