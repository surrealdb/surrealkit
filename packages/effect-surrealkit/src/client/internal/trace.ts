import { Clock, Duration, Effect, Metric, type Scope } from 'effect';

import {
	type RequestTiming,
	type StatementTiming,
	timingCollectors,
} from '../timing.js';

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
	group?: {
		readonly names: ReadonlyArray<string>;
		readonly outcome: (result: A, index: number) => string;
	},
): Effect.Effect<A, E> =>
	Effect.gen(function* () {
		const started = yield* Clock.currentTimeNanos;
		const record = (outcome: (index: number) => string) =>
			Effect.gen(function* () {
				const ended = yield* Clock.currentTimeNanos;
				for (const [index, member] of (
					group?.names ?? [name]
				).entries()) {
					yield* Metric.update(
						Metric.withAttributes(runs, {
							statement: member,
							outcome: outcome(index),
						}),
						1,
					);
				}
				const statement = { statement: name };
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
			Effect.tap((result) =>
				record((index) => group?.outcome(result, index) ?? 'success'),
			),
			Effect.tapError((error) => record(() => error._tag)),
			Effect.onInterrupt(() => record(() => 'interrupted')),
		);
	});

export interface Recorder {
	readonly encode: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	/** Measures one client transport attempt. Wrap this effect in the retry, if any. */
	readonly wait: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	readonly decode: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
	/** Registers the next statement, whether or not its duration is available. */
	readonly server: (
		statement: string,
		duration: Duration.Duration | undefined,
	) => Effect.Effect<void>;
	/** Records a row count for the statement just registered. */
	readonly rows: (statement: string, count: number) => Effect.Effect<void>;
}

export type RequestKind =
	| 'run'
	| 'transaction'
	| 'pipeline'
	| 'stream'
	| 'bulk'
	| 'interactive';

const encodeDuration = Metric.timer(
	'effect_surrealkit_statement_encode_duration',
);
const serverDuration = Metric.timer(
	'effect_surrealkit_statement_server_duration',
);
const decodeDuration = Metric.timer(
	'effect_surrealkit_statement_decode_duration',
);

const micros = (duration: Duration.Duration): number =>
	Duration.toMillis(duration) * 1_000;

const report = (timing: RequestTiming): Effect.Effect<void> =>
	Effect.gen(function* () {
		const { name, sends, encode, wait, server, decode } = timing;
		yield* Effect.annotateCurrentSpan({
			'effect_surrealkit.encode_us': micros(encode),
			'effect_surrealkit.wait_us': micros(wait),
			...(server === undefined
				? {}
				: { 'effect_surrealkit.server_us': micros(server) }),
			'effect_surrealkit.decode_us': micros(decode),
			'effect_surrealkit.sends': sends,
		});
		const tags = { statement: name };
		yield* Metric.update(
			Metric.withAttributes(encodeDuration, tags),
			encode,
		);
		yield* Metric.update(
			Metric.withAttributes(decodeDuration, tags),
			decode,
		);
		if (server !== undefined) {
			yield* Metric.update(
				Metric.withAttributes(serverDuration, tags),
				server,
			);
		}
		if (sends > 0) {
			const collectors = yield* timingCollectors;
			for (const collect of collectors) collect(timing);
		}
	});

const measure = <A, E>(
	effect: Effect.Effect<A, E>,
	save: (elapsed: Duration.Duration) => void,
	onStart?: () => void,
): Effect.Effect<A, E> =>
	Effect.uninterruptibleMask((restore) =>
		Effect.gen(function* () {
			const start = yield* Clock.currentTimeNanos;
			onStart?.();
			return yield* restore(effect).pipe(
				Effect.ensuring(
					Effect.gen(function* () {
						save(
							Duration.nanos(
								(yield* Clock.currentTimeNanos) - start,
							),
						);
					}),
				),
			);
		}),
	);

/** A stream measures the time spent pulling frames across one transport request. */
export interface ScopedRecorder extends Recorder {
	readonly pull: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
}

const startRecording = (kind: RequestKind, name: string) =>
	Effect.gen(function* () {
		const start = yield* Clock.currentTimeNanos;
		const statements: Array<StatementTiming> = [];
		let sends = 0;
		let encode = Duration.zero;
		let wait = Duration.zero;
		let decode = Duration.zero;
		let server: Duration.Duration | undefined;
		const recorder: ScopedRecorder = {
			encode: (effect) =>
				measure(effect, (elapsed) => {
					encode = Duration.sum(encode, elapsed);
				}),
			wait: (effect) =>
				measure(
					effect,
					(elapsed) => {
						wait = elapsed;
					},
					() => {
						sends++;
					},
				),
			pull: (effect) =>
				measure(
					effect,
					(elapsed) => {
						wait = Duration.sum(wait, elapsed);
					},
					() => {
						sends = 1;
					},
				),
			decode: (effect) =>
				measure(effect, (elapsed) => {
					decode = Duration.sum(decode, elapsed);
				}),
			server: (statement, duration) =>
				Effect.sync(() => {
					statements.push(
						duration === undefined
							? { statement }
							: { statement, server: duration },
					);
					if (duration !== undefined) {
						server =
							server === undefined
								? duration
								: Duration.sum(server, duration);
					}
				}),
			rows: (statement, rows) =>
				Effect.sync(() => {
					const index = statements.length - 1;
					const part = statements[index];
					if (part?.statement === statement)
						statements[index] = { ...part, rows };
				}),
		};
		const finish = Effect.gen(function* () {
			const total = Duration.nanos(
				(yield* Clock.currentTimeNanos) - start,
			);
			yield* report({
				kind,
				name,
				sends,
				encode,
				wait,
				decode,
				total,
				statements: statements.slice(),
				...(server === undefined ? {} : { server }),
			});
		});
		return { recorder, finish };
	});

export const recording = <A, E>(
	kind: RequestKind,
	name: string,
	use: (recorder: Recorder) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
	Effect.flatMap(startRecording(kind, name), ({ recorder, finish }) =>
		use(recorder).pipe(Effect.ensuring(finish)),
	);

export const recordingScoped = (
	kind: RequestKind,
	name: string,
): Effect.Effect<ScopedRecorder, never, Scope.Scope> =>
	Effect.acquireRelease(
		startRecording(kind, name),
		({ finish }) => finish,
	).pipe(Effect.map(({ recorder }) => recorder));
