import { Effect, type Schedule } from 'effect';
import { BoundQuery } from 'surrealdb';

import type { AnyStatement } from '../../statement/statement.js';
import type { Queryable, RunOptions } from '../client.js';

/** The abort signal a request takes, as the SDK names it. */
export type Signal = NonNullable<
	Parameters<ReturnType<Queryable['query']>['signal']>[0]
>;

/**
 * What a client sends its requests through: one statement's text with its
 * encoded bindings, or a transaction's joined text with the bindings of all
 * its statements. Each resolves to the list of results, one per SurrealQL
 * statement, or rejects as the SDK does (`classify` reads the rejection).
 *
 * @internal The SDK is the only transport the package offers; the scripted
 * client of `effect-surrealkit/testing` is the other.
 */
export interface Transport {
	readonly query: (
		statement: AnyStatement,
		bindings: Readonly<Record<string, unknown>>,
		signal: Signal,
	) => PromiseLike<unknown>;
	readonly transaction: (
		statements: ReadonlyArray<AnyStatement>,
		request: {
			readonly sql: string;
			readonly bindings: Readonly<Record<string, unknown>>;
		},
		signal: Signal,
	) => PromiseLike<unknown>;
}

export const sdkTransport = (db: Queryable): Transport => {
	// One request per transaction, encoded once and sent again as it is on a
	// retry: the same bindings object makes the same `BoundQuery`.
	const requests = new WeakMap<object, BoundQuery>();
	return {
		query: (statement, bindings, signal) =>
			db
				.query(statement.sql, bindings as Record<string, unknown>)
				.signal(signal),
		transaction: (_, { sql, bindings }, signal) => {
			let request = requests.get(bindings);
			if (request === undefined) {
				request = new BoundQuery(
					sql,
					bindings as Record<string, unknown>,
				);
				requests.set(bindings, request);
			}
			return db.transaction([request], { signal });
		},
	};
};

/**
 * Sends a request, again while it conflicts and `retry` allows. Each send
 * is the same request; `sent` counts them.
 */
export const sending = <A, E extends { readonly _tag: string }>(
	send: Effect.Effect<A, E>,
	options: RunOptions | undefined,
): Effect.Effect<A, E> =>
	options?.retry === undefined
		? send
		: send.pipe(
				Effect.retry({
					schedule: options.retry as unknown as Schedule.Schedule<
						unknown,
						E
					>,
					while: (error: E) => error._tag === 'StatementConflict',
				}),
			);
