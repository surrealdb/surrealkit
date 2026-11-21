import { Effect, type Result } from 'effect';

import type { Bound, Client } from './client.js';
import type { StatementError } from './errors.js';
import type { Transport } from './internal/transport.js';

export interface PipelineOptions {
	/** Wrap each statement in its own transaction, or run its text as it is (default). */
	readonly each?: 'statement' | 'transaction';
	readonly unsafeSkipValueCheck?: boolean;
}

/** One result for each bound statement, in request order. */
export type PipelineResults<S extends ReadonlyArray<unknown>> = {
	readonly [I in keyof S]: S[I] extends Bound<infer R, infer T>
		? Result.Result<R['Type'], StatementError<T>>
		: never;
};

export const makePipeline =
	(_transport: Transport): Client['pipeline'] =>
	() =>
		Effect.die(
			new Error(
				'effect-surrealkit: pipeline is not implemented yet (lane L1)',
			),
		);
