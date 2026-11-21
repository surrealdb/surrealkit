import { Clock, Duration, Effect, Metric } from 'effect';

/**
 * The metrics every run updates, through Effect's `Metric`:
 *
 * - `runs`: a counter of runs, by `statement` (its name) and `outcome`
 *   (`success`, the error's tag, or `interrupted`);
 * - `duration`: a timer of each run, from encoding to decoding, by
 *   `statement`;
 * - `retries`: a counter of requests sent again after a transaction
 *   conflict, by `statement`.
 */
export const metrics = {
	runs: Metric.counter('effect_surrealkit_statement_runs', {
		description: 'Statements run, by statement name and outcome',
		incremental: true,
	}),
	duration: Metric.timer('effect_surrealkit_statement_duration', {
		description: 'Time to run a statement, by statement name',
	}),
	retries: Metric.counter('effect_surrealkit_statement_retries', {
		description: 'Requests sent again after a transaction conflict',
		incremental: true,
	}),
} as const;

const { runs, duration: durations, retries } = metrics;

/**
 * The span and metrics of one run. The span carries the static text, the
 * sources, a family variant's key, the number of rows when the result is a
 * list and the error tag on failure; never a binding value.
 */
export const traced = <A, E extends { readonly _tag: string }>(
	name: string,
	sql: string,
	sources: ReadonlyArray<string> | undefined,
	variant: string | undefined,
	rows: boolean,
	sent: { count: number },
	effect: Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
	Effect.gen(function* () {
		const started = yield* Clock.currentTimeNanos;
		const record = (outcome: string) =>
			Effect.gen(function* () {
				const ended = yield* Clock.currentTimeNanos;
				const statement = { statement: name };
				yield* Metric.update(
					Metric.withAttributes(runs, { ...statement, outcome }),
					1,
				);
				yield* Metric.update(
					Metric.withAttributes(durations, statement),
					Duration.nanos(ended - started),
				);
				if (sent.count > 1) {
					yield* Metric.update(
						Metric.withAttributes(retries, statement),
						sent.count - 1,
					);
				}
			});
		return yield* effect.pipe(
			Effect.tap((result) =>
				rows && Array.isArray(result)
					? Effect.annotateCurrentSpan(
							'db.response.returned_rows',
							result.length,
						)
					: Effect.void,
			),
			Effect.tapError((error) =>
				Effect.annotateCurrentSpan('error.type', error._tag),
			),
			Effect.withSpan(name, {
				kind: 'client',
				attributes: {
					'db.system.name': 'surrealdb',
					'db.operation.name': name,
					// Static text: values travel as bindings and are not recorded.
					'db.query.text': sql,
					...(sources !== undefined &&
						sources.length > 0 && {
							'code.file.path': sources.join(', '),
						}),
					// A family variant's declared key: never a binding value.
					...(variant !== undefined && {
						'effect_surrealkit.statement.variant': variant,
					}),
				},
			}),
			Effect.tap(() => record('success')),
			Effect.tapError((error) => record(error._tag)),
			Effect.onInterrupt(() => record('interrupted')),
		);
	});

export interface Recorder {
	readonly encode: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	readonly wait: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	readonly decode: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	readonly server: (
		statement: string,
		duration: Duration.Duration | undefined,
	) => Effect.Effect<void>;
	readonly rows: (statement: string, count: number) => Effect.Effect<void>;
}

export type RequestKind =
	| 'run'
	| 'transaction'
	| 'pipeline'
	| 'stream'
	| 'bulk'
	| 'interactive';

const passthrough: Recorder = {
	encode: (effect) => effect,
	wait: (effect) => effect,
	decode: (effect) => effect,
	server: () => Effect.void,
	rows: () => Effect.void,
};

export const recording = <A, E>(
	_kind: RequestKind,
	_name: string,
	use: (recorder: Recorder) => Effect.Effect<A, E>,
): Effect.Effect<A, E> => use(passthrough);
