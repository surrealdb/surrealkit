import { type Duration, Effect } from 'effect';

import type { RequestKind } from './internal/trace.js';

export interface StatementTiming {
	readonly statement: string;
	/** The engine's duration summed across the statement's slots, when available. */
	readonly server?: Duration.Duration;
	/** Number of rows in a list result. */
	readonly rows?: number;
}

export interface RequestTiming {
	readonly kind: RequestKind;
	/** The span name of the request. */
	readonly name: string;
	/** Requests sent, including retries. */
	readonly sends: number;
	readonly encode: Duration.Duration;
	/** Time from the last send until its answer arrived. */
	readonly wait: Duration.Duration;
	/** Sum of the statement durations, when available. */
	readonly server?: Duration.Duration;
	readonly decode: Duration.Duration;
	readonly total: Duration.Duration;
	readonly statements: ReadonlyArray<StatementTiming>;
}

/** Runs an effect and collects its request timings in order. */
export const timed = <A, E, R>(
	_effect: Effect.Effect<A, E, R>,
): Effect.Effect<readonly [A, ReadonlyArray<RequestTiming>], E, R> =>
	Effect.die(
		new Error('effect-surrealkit: timed is not implemented yet (lane L3)'),
	);
