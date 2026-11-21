import { Duration, Effect, Result } from 'effect';

import type { Bound, BoundStatements, Client } from './client.js';
import {
	StatementDecodeError,
	type StatementError,
	StatementRequestError,
} from './errors.js';
import {
	classify,
	declarer,
	firstFailure,
	messageOf,
} from './internal/classify.js';
import {
	changesOf,
	committedNote,
	countOf,
	decode,
	decodeOutcome,
} from './internal/decode.js';
import { checkValues, encode } from './internal/encode.js';
import { joinWith } from './internal/join.js';
import { type Recorder, recording, traced } from './internal/trace.js';
import type {
	Request,
	RequestPart,
	SlotAnswer,
	Transport,
} from './internal/transport.js';

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

const wrap = { before: 'BEGIN;', after: 'COMMIT;' };

type Outcome = Result.Result<unknown, StatementError<string>>;

const decodePart = (
	{ statement, slots: expected }: RequestPart,
	answers: ReadonlyArray<SlotAnswer>,
	wrapped: boolean,
): Effect.Effect<Outcome> => {
	const failed = firstFailure(answers);
	if (failed !== undefined) {
		return Effect.succeed(
			Result.fail(
				classify(
					statement.name,
					(reason) => declarer([statement], reason),
					failed.error,
				),
			),
		);
	}
	const writes = changesOf([statement]);
	return Effect.gen(function* () {
		const values = answers.map((slot) =>
			slot.ok ? slot.value : undefined,
		);
		if (
			wrapped &&
			(values[0] !== undefined || values[expected - 1] !== undefined)
		) {
			return yield* Effect.fail(
				new StatementDecodeError({
					statement: statement.name,
					slot: 0,
					committed: writes !== 'none',
					message: `the BEGIN and COMMIT results of a pipeline block must be NONE${committedNote(writes)}`,
				}),
			);
		}
		return yield* decode(statement, values, wrapped ? 1 : 0, writes);
	}).pipe(
		Effect.mapError(decodeOutcome(statement.name, writes)),
		Effect.result,
	);
};

const serverTime = (
	slots: ReadonlyArray<SlotAnswer>,
): Duration.Duration | undefined => {
	let duration: Duration.Duration | undefined;
	for (const { server } of slots) {
		if (server !== undefined)
			duration = Duration.sum(duration ?? Duration.zero, server);
	}
	return duration;
};

const decodeAnswers = (
	{ parts }: Request,
	answers: ReadonlyArray<SlotAnswer>,
	wrapped: boolean,
	recorder: Recorder,
): Effect.Effect<Array<Outcome>> => {
	const expected = parts.reduce((total, { slots }) => total + slots, 0);
	const aligned = answers.length === expected;
	return Effect.forEach(parts, (part) =>
		Effect.gen(function* () {
			const { statement, offset, slots } = part;
			// A wrong total leaves no reliable way to assign slots to parts.
			if (!aligned) {
				yield* recorder.server(statement.name, undefined);
				const writes = changesOf([statement]);
				return yield* countOf(
					statement.name,
					answers,
					expected,
					writes,
				).pipe(
					Effect.mapError(decodeOutcome(statement.name, writes)),
					Effect.result,
				);
			}
			const returned = answers.slice(offset, offset + slots);
			yield* recorder.server(statement.name, serverTime(returned));
			const outcome = yield* decodePart(part, returned, wrapped);
			if (Result.isSuccess(outcome) && Array.isArray(outcome.success)) {
				yield* recorder.rows(statement.name, outcome.success.length);
			}
			return outcome;
		}),
	);
};

export const makePipeline =
	(transport: Transport): Client['pipeline'] =>
	<const S extends ReadonlyArray<unknown>>(
		given: BoundStatements<S>,
		options?: PipelineOptions,
	) => {
		const statements = given as unknown as ReadonlyArray<Bound>;
		if (statements.length === 0)
			return Effect.succeed([] as unknown as PipelineResults<S>);
		const names = statements.map(({ statement }) => statement.name);
		const name = `pipeline(${names.join(', ')})`;
		const sql = statements.map(({ statement }) => statement.sql).join('\n');
		const sources = [
			...new Set(
				statements.flatMap(({ statement }) => statement.sources ?? []),
			),
		];
		const wrapped = options?.each === 'transaction';
		return traced(
			name,
			sql,
			sources,
			undefined,
			false,
			recording('pipeline', name, (recorder) =>
				Effect.gen(function* () {
					const request = yield* recorder.encode(
						Effect.gen(function* () {
							const joined = yield* joinWith(
								statements.map(({ statement, input }) => ({
									statement,
									bindings: Effect.suspend(() =>
										encode(statement, input),
									),
								})),
								{
									mode: 'query',
									refuseTransactional: wrapped,
									...(wrapped && { wrap }),
								},
							);
							for (const {
								statement,
								bindings,
							} of joined.parts) {
								yield* checkValues(
									bindings,
									() => statement.name,
									options,
								);
							}
							return joined;
						}),
					);
					const answers = yield* recorder.wait(
						Effect.tryPromise({
							try: (signal) => transport.send(request, signal),
							catch: (cause) =>
								new StatementRequestError({
									statement: name,
									message: messageOf(cause),
									cause,
								}),
						}),
					);
					return yield* recorder.decode(
						decodeAnswers(request, answers, wrapped, recorder),
					);
				}),
			),
			{
				names,
				outcome: (outcomes, index) => {
					const outcome = outcomes[index];
					return outcome !== undefined && Result.isFailure(outcome)
						? outcome.failure._tag
						: 'success';
				},
			},
		).pipe(
			Effect.map((outcomes) => outcomes as unknown as PipelineResults<S>),
		);
	};
