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
import { Features, UnsupportedFeatureError } from 'surrealdb';

import { type Client, makeWith, SurrealClient } from '../client/client.js';
import type { StatementUnsupported } from '../client/errors.js';
import type {
	LiveHandle,
	Request,
	RequestPart,
	Signal,
	SlotAnswer,
	Transport,
} from '../client/internal/transport.js';
import { scanTopLevel } from '../statement/statement.js';

const ReplyTypeId = '~effect-surrealkit/testing/Reply';

type LiveMessage =
	LiveHandle['messages'] extends AsyncIterable<infer Message>
		? Message
		: never;
type Feature = StatementUnsupported['feature'];

/** What a scripted statement answers. */
export type Reply =
	| {
			readonly [ReplyTypeId]: 'result';
			/** The last result, as the SDK would return it (NONE is `undefined`). */
			readonly value: unknown;
	  }
	| { readonly [ReplyTypeId]: 'throw'; readonly message: string }
	| { readonly [ReplyTypeId]: 'conflict' }
	| { readonly [ReplyTypeId]: 'failure'; readonly message: string }
	| {
			readonly [ReplyTypeId]: 'events';
			readonly messages: ReadonlyArray<LiveMessage>;
	  }
	| { readonly [ReplyTypeId]: 'unsupported'; readonly feature: Feature };

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

/** The events a scripted live query delivers, in order. */
export const events = (messages: ReadonlyArray<LiveMessage>): Reply => ({
	[ReplyTypeId]: 'events',
	messages,
});

/** The scripted connection refuses a feature. For begin, use the script entry named begin. */
export const unsupported = (feature: Feature): Reply => ({
	[ReplyTypeId]: 'unsupported',
	feature,
});

const isReply = (value: unknown): value is Reply =>
	typeof value === 'object' && value !== null && ReplyTypeId in value;

/**
 * The script: for each statement name, a reply, a plain value (the same as
 * `result(value)`), or a function of the statement's encoded bindings that
 * returns either. The begin, commit, cancel and kill entries also script
 * their corresponding lifecycle operations when a client uses them.
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
	/** Its encoded bindings under the statement's own names. */
	readonly bindings: Readonly<Record<string, unknown>>;
	/** The transaction it was sent in, when it was. */
	readonly transaction?: string;
	/** The pipeline it was sent in, when it was. */
	readonly pipeline?: string;
	/** The interactive transaction it was sent in, when it was. */
	readonly interactive?: string;
	/** Lifecycle operations use their name in statement, or the live query's name for kill. */
	readonly operation?: 'begin' | 'commit' | 'cancel' | 'kill';
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

const sdkFeatures = {
	'interactive transaction': Features.Transactions,
	'live query': Features.LiveQueries,
	stream: Features.QueryStreaming,
} as const;

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
		case 'unsupported':
			return new UnsupportedFeatureError(sdkFeatures[reply.feature]);
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

type CallContext = Pick<Call, 'transaction' | 'pipeline' | 'interactive'>;
type Bindings = RequestPart['bindings'];

const success = (value: unknown): SlotAnswer => ({ ok: true, value });

const firstSlot = (part: RequestPart): number =>
	part.offset + (part.slots === part.statement.slots + 2 ? 1 : 0);

const valueSlot = (part: RequestPart): number =>
	firstSlot(part) + part.statement.valueSlot - 1;

const requestContext = (
	{ mode, parts }: Request,
	collected: boolean,
): CallContext => {
	if (mode === 'query' && collected) return {};
	const names = parts.map(({ statement }) => statement.name).join(', ');
	return mode === 'atomic'
		? { transaction: `transaction(${names})` }
		: { pipeline: `pipeline(${names})` };
};

/** Creates the transport used by the scripted client. @internal */
export const scriptedTransport = (
	script: Script,
): {
	readonly transport: Transport;
	readonly calls: ReadonlyArray<Call>;
} => {
	const calls: Array<Call> = [];
	let transactions = 0;

	const entryFor = (name: string, bindings: Bindings): unknown => {
		const entry = script[name];
		return typeof entry === 'function'
			? (entry as (bindings: Bindings) => unknown)(bindings)
			: entry;
	};

	const reply = (
		{ statement: { name }, bindings }: RequestPart,
		context: CallContext,
	): Reply => {
		calls.push({ statement: name, bindings: { ...bindings }, ...context });
		if (!Object.hasOwn(script, name)) {
			return failure(`the scripted client has no reply for \`${name}\``);
		}
		const value = entryFor(name, bindings);
		return isReply(value) ? value : result(value);
	};

	const dispatch = async (
		request: Request,
		signal: Signal,
		collected: boolean,
		interactive?: string,
	): Promise<ReadonlyArray<SlotAnswer>> => {
		signal.throwIfAborted();
		const context =
			interactive === undefined
				? requestContext(request, collected)
				: { interactive };
		const whole =
			collected ||
			(request.mode === 'atomic' && interactive === undefined);
		if (request.mode === 'atomic' && interactive === undefined) {
			const refused = sdkRefusal(request.sql);
			if (refused !== undefined) throw refused;
		}
		const size = request.parts.reduce(
			(size, part) => Math.max(size, part.offset + part.slots),
			0,
		);
		const slots = Array.from({ length: size }, () => success(undefined));
		for (const part of request.parts) {
			try {
				const answer = reply(part, context);
				const error = rejection(answer);
				if (error !== undefined) throw error;
				slots[valueSlot(part)] = success(
					answer[ReplyTypeId] === 'result' ? answer.value : undefined,
				);
			} catch (error) {
				if (whole) throw error;
				slots[firstSlot(part)] = { ok: false, error };
			}
		}
		return slots;
	};

	const lifecycle = async (
		operation: NonNullable<Call['operation']>,
		context: CallContext,
		statement: string = operation,
	): Promise<void> => {
		calls.push({ statement, bindings: {}, operation, ...context });
		if (!Object.hasOwn(script, operation)) return;
		const answer = await entryFor(operation, {});
		if (isReply(answer)) {
			const error = rejection(answer);
			if (error !== undefined) throw error;
		}
	};

	const transport: Transport = {
		collect: async (request, signal) => {
			const slots = await dispatch(request, signal, true);
			return slots.map((slot) => {
				if (!slot.ok) throw slot.error;
				return slot.value;
			});
		},
		send: (request, signal) => dispatch(request, signal, false),
		stream: async function* (request, signal) {
			for (const part of request.parts) {
				signal.throwIfAborted();
				const answer = reply(part, {});
				const error = rejection(answer);
				if (error instanceof UnsupportedFeatureError) throw error;
				if (error !== undefined) {
					yield { kind: 'error', slot: firstSlot(part), error };
					continue;
				}
				for (
					let slot = part.offset;
					slot < part.offset + part.slots;
					slot++
				) {
					if (
						slot === valueSlot(part) &&
						answer[ReplyTypeId] === 'result'
					) {
						if (Array.isArray(answer.value)) {
							for (const value of answer.value) {
								signal.throwIfAborted();
								yield { kind: 'row', slot, value };
							}
						} else {
							yield { kind: 'value', slot, value: answer.value };
						}
					} else {
						yield { kind: 'value', slot, value: undefined };
					}
					yield { kind: 'done', slot };
				}
			}
		},
		begin: async (signal) => {
			signal.throwIfAborted();
			const interactive = `transaction ${++transactions}`;
			await lifecycle('begin', { interactive });
			return {
				send: (request, signal) =>
					dispatch(request, signal, false, interactive),
				commit: () => lifecycle('commit', { interactive }),
				cancel: () => lifecycle('cancel', { interactive }),
			};
		},
		live: async (request, signal) => {
			signal.throwIfAborted();
			const [part] = request.parts;
			if (part === undefined)
				throw new Error('the scripted live query has no statement');
			const answer = reply(part, {});
			const error = rejection(answer);
			if (error !== undefined) throw error;
			if (answer[ReplyTypeId] !== 'events') {
				throw new Error(
					`the scripted client needs events for \`${part.statement.name}\``,
				);
			}
			let killed = false;
			return {
				messages: (async function* () {
					for (const message of answer.messages) {
						if (killed) return;
						yield message;
					}
				})(),
				kill: async () => {
					killed = true;
					await lifecycle('kill', {}, part.statement.name);
				},
			};
		},
	};
	return { transport, calls };
};

/**
 * A client that answers each statement by its name from the script, and
 * records every request in calls. A missing reply is a request failure.
 */
export const fake = (script: Script): FakeClient => {
	const { transport, calls } = scriptedTransport(script);
	return Object.assign(makeWith(transport), { calls });
};

/** Provides {@link SurrealClient} as a scripted client. */
export const fakeLayer = (script: Script): Layer.Layer<SurrealClient> =>
	Layer.succeed(SurrealClient, fake(script));
