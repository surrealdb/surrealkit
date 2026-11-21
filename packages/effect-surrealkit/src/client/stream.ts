import {
	Cause,
	Clock,
	Duration,
	Effect,
	Exit,
	Metric,
	Schema,
	Scope,
	Stream,
} from 'effect';
import { UnavailableFeatureError, UnsupportedFeatureError } from 'surrealdb';

import { codecIssueText } from '../internal/codecIssue.js';
import type { Codec, Params, Statement } from '../statement/statement.js';
import type { Client, InputOf } from './client.js';
import {
	StatementDecodeError,
	type StatementError,
	StatementUnsupported,
	StatementWriteOutcomeError,
} from './errors.js';
import {
	classify,
	declarer,
	isCollateralFailure,
	messageOf,
} from './internal/classify.js';
import {
	changesOf,
	committedNote,
	countOf,
	decode,
} from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { requestPart } from './internal/join.js';
import { withClientStreamSpan } from './internal/span.js';
import { metrics, recordingScoped } from './internal/trace.js';
import type { Request, Transport } from './internal/transport.js';

export const makeStream =
	(transport: Transport): Client['stream'] =>
	<P extends Params, Row extends Codec, Throws extends string, I>(
		statement: Statement<P, Schema.$Array<Row>, Throws>,
		...args: InputOf<P, I>
	) => {
		const { name, sql, result, slots, valueSlot } = statement;
		const index = valueSlot - 1;
		const writes = changesOf([statement]);
		const input = (args[0] ?? {}) as Readonly<Record<string, unknown>>;
		const decodeRow = Schema.decodeUnknownEffect(result.value);
		const classifyError = (cause: unknown) =>
			cause instanceof UnsupportedFeatureError ||
			cause instanceof UnavailableFeatureError
				? new StatementUnsupported({
						statement: name,
						feature: 'stream',
						message: messageOf(cause),
						cause,
					})
				: classify<Throws>(
						name,
						(reason) => declarer([statement], reason),
						cause,
					);

		return Stream.unwrap(
			Effect.gen(function* () {
				const recorder = yield* recordingScoped('stream', name);
				const started = yield* Clock.currentTimeNanos;
				let outcome = 'success';
				let rows = 0;
				let server: Duration.Duration | undefined;
				yield* Effect.addFinalizer((exit) =>
					Effect.gen(function* () {
						yield* recorder.server(name, server);
						yield* recorder.rows(name, rows);
						const label = Exit.hasInterrupts(exit)
							? 'interrupted'
							: outcome;
						yield* Effect.annotateCurrentSpan(
							'db.response.returned_rows',
							rows,
						);
						if (label !== 'success' && label !== 'interrupted')
							yield* Effect.annotateCurrentSpan(
								'error.type',
								label,
							);
						yield* Metric.update(
							Metric.withAttributes(metrics.runs, {
								statement: name,
								outcome: label,
							}),
							1,
						);
						yield* Metric.update(
							Metric.withAttributes(metrics.duration, {
								statement: name,
							}),
							Duration.nanos(
								(yield* Clock.currentTimeNanos) - started,
							),
						);
					}),
				);
				const countError = <E extends { readonly _tag: string }>(
					error: E,
				) =>
					Effect.sync(() => {
						if (!Cause.isDone(error)) outcome = error._tag;
					});
				const bindings = yield* recorder
					.encode(
						Effect.gen(function* () {
							if (
								result.ast.checks !== undefined ||
								result.ast.encoding !== undefined
							) {
								return yield* Effect.fail(
									new StatementUnsupported({
										statement: name,
										feature: 'stream',
										message:
											'stream result must be a plain Schema.Array of rows without array-wide checks or transformations',
									}),
								);
							}
							const encoded = yield* encode(statement, input);
							yield* checkValues(encoded, () => name, undefined);
							return encoded;
						}),
					)
					.pipe(Effect.tapError(countError));

				const request: Request = {
					mode: 'query',
					sql,
					bindings,
					parts: [requestPart(statement, bindings, 0, slots)],
				};
				const scope = yield* Scope.Scope;
				// Abort before closing the iterator, even if its next() has not returned.
				const sourceScope = yield* Scope.fork(scope, 'sequential');
				const signal = yield* Effect.abortSignal;
				const frames = Stream.fromAsyncIterable(
					{
						async *[Symbol.asyncIterator]() {
							yield* transport.stream(request, signal);
						},
					},
					classifyError,
				);
				const pull = yield* Stream.toPull(frames).pipe(
					Effect.provideService(Scope.Scope, sourceScope),
				);
				const nextFrame = recorder.pull(pull);

				const completed = new Set<number>();
				const none = new Set<number>();
				let collateral: { readonly error: unknown } | undefined;
				const makeDecodeError = (slot: number, message: string) =>
					new StatementDecodeError({
						statement: name,
						slot,
						committed: false,
						message,
					});
				const decodeError = <E>(
					error: E,
				): E | StatementWriteOutcomeError => {
					if (
						writes === 'none' ||
						!(error instanceof StatementDecodeError)
					)
						return error;
					const committed = completed.size === slots;
					return new StatementWriteOutcomeError({
						request: name,
						cause: new StatementDecodeError({
							statement: name,
							slot: error.slot,
							committed,
							message:
								error.message +
								(committed
									? committedNote(writes)
									: '. The stream ended before the write outcome was acknowledged. Read the data back before replaying'),
						}),
					});
				};
				// Reuse run's validation for missing, scalar and non-value results. Valid rows never build the list.
				const validate = (slot: number, value: unknown) => {
					const values: Array<unknown> = [];
					values[slot] = value;
					return recorder
						.decode(decode(statement, values, 0, 'none'))
						.pipe(Effect.mapError(decodeError));
				};
				const checkedFrame = nextFrame.pipe(
					Effect.catchIf(Cause.isDone, (end) =>
						Effect.gen(function* () {
							if (collateral !== undefined)
								return yield* Effect.fail(
									classifyError(collateral.error),
								);
							yield* countOf(
								name,
								Array.from(completed),
								slots,
								'none',
							).pipe(Effect.mapError(decodeError));
							return yield* Effect.fail(end);
						}),
					),
				);
				const nextRow = Effect.gen(function* () {
					while (true) {
						const [frame] = yield* checkedFrame;
						const { slot } = frame;
						if (
							(frame.kind === 'done' || frame.kind === 'error') &&
							frame.server !== undefined
						) {
							server = Duration.sum(
								server ?? Duration.zero,
								frame.server,
							);
						}
						if (frame.kind === 'error') {
							if (!isCollateralFailure(frame.error))
								return yield* Effect.fail(
									classifyError(frame.error),
								);
							collateral ??= frame;
						}
						if (collateral !== undefined) continue;
						if (
							!Number.isInteger(slot) ||
							slot < 0 ||
							slot >= slots ||
							completed.has(slot)
						) {
							return yield* Effect.fail(
								decodeError(
									makeDecodeError(
										0,
										`unexpected stream frame for result ${slot + 1} of ${slots}`,
									),
								),
							);
						}
						if (frame.kind === 'done') {
							completed.add(slot);
							if (slot === index ? rows === 0 : !none.has(slot))
								yield* validate(slot, []);
						} else if (frame.kind === 'value') {
							if (slot === index || frame.value !== undefined)
								yield* validate(slot, frame.value);
							none.add(slot);
						} else if (frame.kind === 'row') {
							if (slot !== index) {
								yield* validate(slot, [frame.value]);
								continue;
							}
							const row = yield* recorder
								.decode(
									decodeRow(frame.value, {
										onExcessProperty: 'error',
									}),
								)
								.pipe(
									Effect.mapError((error) =>
										decodeError(
											makeDecodeError(
												valueSlot,
												codecIssueText(
													error.issue,
													result.value.ast,
												),
											),
										),
									),
								);
							rows++;
							return [row] as const;
						}
					}
				}).pipe(Effect.tapError(countError));
				return Stream.fromPull(Effect.succeed(nextRow));
			}),
		).pipe(
			withClientStreamSpan(name, {
				kind: 'client',
				attributes: {
					'db.system.name': 'surrealdb',
					'db.operation.name': name,
					'db.query.text': sql,
					...(statement.sources === undefined ||
					statement.sources.length === 0
						? {}
						: { 'code.file.path': statement.sources.join(', ') }),
					...(statement.variant === undefined
						? {}
						: {
								'effect_surrealkit.statement.variant':
									statement.variant,
							}),
				},
			}),
		) as Stream.Stream<
			Row['Type'],
			StatementError<Throws> | StatementUnsupported
		>;
	};
