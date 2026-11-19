/**
 * A scripted client: canned results per statement name, with no engine.
 *
 * It is the real client over a scripted transport, so everything the client
 * checks before sending still runs: each binding through its codec, the
 * value check, the scope rules of a transaction, the number of results and
 * NONE in all but the last, and the result codec. What the SDK refuses on
 * its own before sending a transaction is refused the same way, with its
 * message: `BEGIN`, `COMMIT` or `CANCEL` in the text, and a `RETURN`
 * followed by another statement.
 */
import { Layer } from 'effect';

import {
	type Client,
	makeWith,
	SurrealClient,
	type Transport,
} from '../client/client.js';
import { type AnyStatement, scanTopLevel } from '../statement/statement.js';

const ReplyTypeId = '~effect-surrealkit/testing/Reply';

/** What a scripted statement answers. */
export type Reply =
	| {
			readonly [ReplyTypeId]: 'result';
			/** The last result, as the SDK would return it (NONE is `undefined`). */
			readonly value: unknown;
	  }
	| { readonly [ReplyTypeId]: 'throw'; readonly message: string }
	| { readonly [ReplyTypeId]: 'conflict' }
	| { readonly [ReplyTypeId]: 'failure'; readonly message: string };

/** The statement's last result is `value`; the ones before it are NONE. */
export const result = (value: unknown): Reply => ({
	[ReplyTypeId]: 'result',
	value,
});

/** The statement raises `THROW message`, as the engine reports it. */
export const thrown = (message: string): Reply => ({
	[ReplyTypeId]: 'throw',
	message,
});

/** The request conflicts with another transaction. */
export const conflict = (): Reply => ({ [ReplyTypeId]: 'conflict' });

/** The request fails, as a SurrealQL or transport error would. */
export const failure = (message: string): Reply => ({
	[ReplyTypeId]: 'failure',
	message,
});

const isReply = (value: unknown): value is Reply =>
	typeof value === 'object' && value !== null && ReplyTypeId in value;

/**
 * The script: for each statement name, a reply, a plain value (the same as
 * `result(value)`), or a function of the statement's encoded bindings that
 * returns either.
 */
export type Script = Readonly<
	Record<
		string,
		unknown | ((bindings: Readonly<Record<string, unknown>>) => unknown)
	>
>;

/** One request the scripted client answered. */
export interface Call {
	/** The statement's name. */
	readonly statement: string;
	/** Its bindings, encoded, as they would have been sent. */
	readonly bindings: Readonly<Record<string, unknown>>;
	/** The transaction it was sent in, when it was. */
	readonly transaction?: string;
}

/** A client over a script, and the requests it answered, in order. */
export interface FakeClient extends Client {
	readonly calls: ReadonlyArray<Call>;
}

/** An error shaped as the SDK reports it, which the client classifies. */
const sdkError = (
	message: string,
	fields: Readonly<Record<string, unknown>> = {},
): Error => Object.assign(new Error(message), fields);

const rejection = (reply: Reply): Error | undefined => {
	switch (reply[ReplyTypeId]) {
		case 'throw':
			return sdkError(`An error occurred: ${reply.message}`, {
				kind: 'Thrown',
			});
		case 'conflict':
			return sdkError(
				'Transaction conflict: the transaction was retried but still conflicted',
				{ isTransactionConflict: true },
			);
		case 'failure':
			return sdkError(reply.message);
		default:
			return undefined;
	}
};

const control: ReadonlySet<string> = new Set(['BEGIN', 'COMMIT', 'CANCEL']);

/**
 * What the SDK's `transaction()` refuses before sending: the client sends a
 * transaction as one query, `queries[0]`.
 */
const sdkRefusal = (sql: string): Error | undefined => {
	let returned = false;
	for (const keyword of scanTopLevel(sql).keywords) {
		if (control.has(keyword)) {
			return sdkError(
				`queries[0] contains a ${keyword} statement. transaction() wraps the queries in BEGIN and COMMIT itself, so they must not contain any`,
			);
		}
		if (returned) {
			return sdkError(
				'queries[0] contains a RETURN statement which is followed by another statement (in queries[0]). Inside a transaction RETURN ends it early: the statements after it would be skipped but the transaction would still commit. Use SELECT or a bare expression instead, or move the RETURN to the end',
			);
		}
		if (keyword === 'RETURN') returned = true;
	}
	return undefined;
};

/**
 * A client that answers each statement by its name from `script`, and
 * records every request in `calls`. A statement the script does not name
 * fails as a request would, naming it.
 *
 * @example
 * const client = fake({
 *   'adults in a city': [{ id: personId, name: 'Ann', nickname: undefined }],
 *   'read own profile': thrown('Not the owner'),
 * });
 */
export const fake = (script: Script): FakeClient => {
	const calls: Array<Call> = [];
	const reply = (
		statement: AnyStatement,
		bindings: Readonly<Record<string, unknown>>,
	): Reply => {
		if (!Object.hasOwn(script, statement.name)) {
			return failure(
				`the scripted client has no reply for \`${statement.name}\``,
			);
		}
		const entry = script[statement.name];
		const value =
			typeof entry === 'function'
				? (
						entry as (
							bindings: Readonly<Record<string, unknown>>,
						) => unknown
					)(bindings)
				: entry;
		return isReply(value) ? value : result(value);
	};
	const slotsOf = (statement: AnyStatement, value: unknown) => [
		...Array.from({ length: statement.slots - 1 }, () => undefined),
		value,
	];
	const own = (
		statement: AnyStatement,
		bindings: Readonly<Record<string, unknown>>,
	): Readonly<Record<string, unknown>> =>
		Object.fromEntries(
			Object.keys(statement.params).flatMap((name) =>
				Object.hasOwn(bindings, name) ? [[name, bindings[name]]] : [],
			),
		);
	const transport: Transport = {
		query: async (statement, bindings) => {
			calls.push({
				statement: statement.name,
				bindings: { ...bindings },
			});
			const answer = reply(statement, bindings);
			const error = rejection(answer);
			if (error !== undefined) throw error;
			return slotsOf(
				statement,
				answer[ReplyTypeId] === 'result' ? answer.value : undefined,
			);
		},
		transaction: async (statements, { sql, bindings }) => {
			const refused = sdkRefusal(sql);
			if (refused !== undefined) throw refused;
			const name = `transaction(${statements.map((item) => item.name).join(', ')})`;
			const results: Array<unknown> = [];
			for (const statement of statements) {
				const mine = own(statement, bindings);
				calls.push({
					statement: statement.name,
					bindings: mine,
					transaction: name,
				});
				const answer = reply(statement, mine);
				const error = rejection(answer);
				// The transaction is atomic: one failure answers for all of it.
				if (error !== undefined) throw error;
				results.push(
					...slotsOf(
						statement,
						answer[ReplyTypeId] === 'result'
							? answer.value
							: undefined,
					),
				);
			}
			return results;
		},
	};
	return Object.assign(makeWith(transport), { calls });
};

/** Provides {@link SurrealClient} as a scripted client. */
export const fakeLayer = (script: Script): Layer.Layer<SurrealClient> =>
	Layer.succeed(SurrealClient, fake(script));
