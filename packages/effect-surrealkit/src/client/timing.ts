import { Context, type Duration, Effect } from 'effect';

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
	/** Transport attempts by the client, including its own conflict retries. */
	readonly sends: number;
	readonly encode: Duration.Duration;
	/** Elapsed time of the last transport attempt, including any retries inside the SDK. */
	readonly wait: Duration.Duration;
	/** Sum of the statement durations, when available. */
	readonly server?: Duration.Duration;
	readonly decode: Duration.Duration;
	readonly total: Duration.Duration;
	readonly statements: ReadonlyArray<StatementTiming>;
}

export const timingCollectors = Context.Reference<
	ReadonlyArray<(timing: RequestTiming) => void>
>('effect-surrealkit/TimingCollectors', { defaultValue: () => [] });

/** Runs an effect and collects its request timings in completion order. */
export const timed = <A, E, R>(
	effect: Effect.Effect<A, E, R>,
): Effect.Effect<readonly [A, ReadonlyArray<RequestTiming>], E, R> =>
	Effect.gen(function* () {
		const outer = yield* timingCollectors;
		const timings: Array<RequestTiming> = [];
		let active: Array<RequestTiming> | undefined = timings;
		const append = (timing: RequestTiming): void => {
			active?.push(timing);
		};
		const value = yield* Effect.provideService(effect, timingCollectors, [
			...outer,
			append,
		]).pipe(
			Effect.ensuring(
				Effect.sync(() => {
					// Detached fibers may still hold this collector after the scope settles.
					active = undefined;
				}),
			),
		);
		return [value, timings] as const;
	});
