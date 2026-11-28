import {
	BoundQuery,
	ErrorFrame,
	QueryError,
	type QueryLike,
	type QueryResponseFailure,
	type TransactionOptions,
} from 'surrealdb';

import {
	AnalyzeError,
	DEFAULT_HOLE_DELIMITERS,
	lex,
} from '../analyze/lexer.js';
import type { Queryable } from '../client/client.js';

type Query = ReturnType<Queryable['query']>;

const forwardData = (
	target: object,
	key: PropertyKey,
	{
		configurable = false,
		enumerable = false,
		writable = false,
	}: PropertyDescriptor,
): PropertyDescriptor => {
	let method: unknown;
	let bound: unknown;
	return {
		configurable,
		enumerable,
		get: () => {
			const value: unknown = Reflect.get(target, key);
			if (typeof value !== 'function') return value;
			if (value !== method) {
				method = value;
				bound = value.bind(target);
			}
			return bound;
		},
		...(writable && {
			set: (value: unknown) => {
				if (!Reflect.set(target, key, value)) {
					throw new TypeError(
						`Cannot write session property ${String(key)}`,
					);
				}
			},
		}),
	};
};

/** Forwards session properties and retains the receiver required by SDK private fields. */
const forward = <T extends object>(target: T, overrides: object): T => {
	const facade = Object.defineProperties(
		{},
		Object.getOwnPropertyDescriptors(overrides),
	);
	for (
		let owner: object | null = target;
		owner !== null && owner !== Object.prototype;
		owner = Object.getPrototypeOf(owner) as object | null
	) {
		for (const key of Reflect.ownKeys(owner)) {
			if (
				(owner !== target && key === 'constructor') ||
				Object.hasOwn(facade, key)
			)
				continue;
			const descriptor = Object.getOwnPropertyDescriptor(owner, key);
			if (descriptor === undefined) continue;
			if ('value' in descriptor) {
				Object.defineProperty(
					facade,
					key,
					forwardData(target, key, descriptor),
				);
				continue;
			}
			if (descriptor.get !== undefined)
				descriptor.get = descriptor.get.bind(target);
			if (descriptor.set !== undefined)
				descriptor.set = descriptor.set.bind(target);
			Object.defineProperty(facade, key, descriptor);
		}
	}
	return facade as T;
};

const conflict = () =>
	new QueryError({
		kind: 'Query',
		message: 'Transaction conflict: Write conflict, retry the transaction.',
		details: { kind: 'TransactionConflict' },
	});

const notExecuted = () =>
	new QueryError({
		kind: 'Query',
		message: 'The query was not executed due to a failed transaction',
		details: { kind: 'NotExecuted' },
	});

/** The lexical bounds of each slot, including comments before the statement. */
const statementTexts = (sql: string): ReadonlyArray<string> => {
	const { tokens } = lex(sql, DEFAULT_HOLE_DELIMITERS);
	const texts: Array<string> = [];
	let depth = 0;
	let start = 0;
	let hasText = false;
	for (const token of tokens) {
		if (token.kind === 'punct') {
			if (token.value === ';' && depth === 0) {
				if (hasText) texts.push(sql.slice(start, token.end));
				start = token.end;
				hasText = false;
				continue;
			}
			if ('([{'.includes(token.value)) depth++;
			if (')]}'.includes(token.value)) depth--;
		}
		hasText = true;
	}
	if (hasText) texts.push(sql.slice(start));
	return texts;
};

const conflictResponses = (
	sql: string,
	marker: string,
): ReadonlyArray<QueryResponseFailure> => {
	let texts: ReadonlyArray<string>;
	try {
		texts = statementTexts(sql);
	} catch (cause) {
		if (cause instanceof AnalyzeError) throw conflict();
		throw cause;
	}
	if (texts.length === 0) throw conflict();
	const matching = Math.max(
		0,
		texts.findIndex((text) => text.includes(marker)),
	);
	return texts.map((_, index) => ({
		success: false,
		error: index === matching ? conflict() : notExecuted(),
	}));
};

const textOf = (query: QueryLike): string => {
	if (typeof query === 'string') return query;
	if (query instanceof BoundQuery) return query.query;
	return ('inner' in query ? query.inner : query.compile()).query;
};

/** Opens the SDK's iterable on its first read and forwards its early disposal. */
const view = <A>(
	open: () => AsyncIterableIterator<A>,
	inject: () => boolean,
): AsyncIterableIterator<A> & AsyncDisposable => {
	let source: AsyncIterableIterator<A> | undefined;
	let closed = false;
	const done = { done: true, value: undefined } as const;
	const iterator = {
		async next() {
			if (closed) return done;
			if (source === undefined) {
				try {
					if (inject()) throw conflict();
					source = open();
				} catch (error) {
					closed = true;
					throw error;
				}
			}
			return source.next();
		},
		async return() {
			closed = true;
			return source?.return === undefined ? done : source.return();
		},
		async throw(error: unknown) {
			closed = true;
			if (source?.throw !== undefined) return source.throw(error);
			throw error;
		},
		[Symbol.asyncIterator]() {
			return iterator;
		},
		async [Symbol.asyncDispose]() {
			await iterator.return();
		},
	};
	return iterator;
};

const wrapQuery = (
	query: Query,
	marker: string,
	take: () => boolean,
	signals: ReadonlyArray<AbortSignal> = [],
): Query => {
	const inject = () => {
		for (const signal of signals) signal.throwIfAborted();
		return take();
	};
	const responses = () => conflictResponses(query.inner.query, marker);
	let dispatched: Promise<unknown[]> | undefined;
	const collect = async (...indexes: number[]) => {
		if (inject()) throw conflict();
		return query.collect(...indexes);
	};
	const awaitOnce = () => {
		dispatched ??= collect();
		return dispatched;
	};
	return forward(query, {
		signal: (signal: AbortSignal | undefined) =>
			wrapQuery(
				query.signal(signal),
				marker,
				take,
				signal === undefined ? signals : [...signals, signal],
			),
		retry: (...args: Parameters<Query['retry']>) =>
			wrapQuery(query.retry(...args), marker, take, signals),
		requestTimeout: (...args: Parameters<Query['requestTimeout']>) =>
			wrapQuery(query.requestTimeout(...args), marker, take, signals),
		as: (...args: Parameters<Query['as']>) =>
			wrapQuery(query.as(...args), marker, take, signals),
		json: () => wrapQuery(query.json(), marker, take, signals),
		dispatch: collect,
		collect,
		// biome-ignore lint/suspicious/noThenProperty: the SDK Query is a thenable
		then: (...args: Parameters<Promise<unknown[]>['then']>) =>
			awaitOnce().then(...args),
		catch: (...args: Parameters<Promise<unknown[]>['catch']>) =>
			awaitOnce().catch(...args),
		finally: (...args: Parameters<Promise<unknown[]>['finally']>) =>
			awaitOnce().finally(...args),
		responses: async (...indexes: number[]) => {
			if (!inject()) return query.responses(...indexes);
			const slots = responses();
			if (indexes.length === 0) return slots;
			const positions = new Map(
				indexes.map((slot, index) => [slot, index]),
			);
			const selected: Array<QueryResponseFailure> = [];
			for (const [index, slot] of slots.entries()) {
				const position = positions.get(index);
				// The SDK rejects when any excluded slot fails.
				if (position === undefined) throw slot.error;
				selected[position] = slot;
			}
			return selected;
		},
		rows: <A>(parse?: (row: unknown) => A) =>
			view(() => query.rows(parse), inject),
		statements: () => view(() => query.statements(), inject),
		stream: async function* () {
			if (!inject()) {
				yield* query.stream();
				return;
			}
			for (const [index, slot] of responses().entries()) {
				yield new ErrorFrame(index, undefined, slot.error);
			}
		},
	});
};

/**
 * Injects the SDK's transaction conflict into the first matching requests.
 * None of an injected request reaches the engine. For slot responses, the
 * first matching statement receives the conflict and all other slots report
 * that they did not execute. The count includes matching requests passed
 * through after the injections are exhausted.
 */
export const conflictingSession = <S extends Queryable>(
	session: S,
	options: { readonly marker: string; readonly times: number },
): { readonly session: S; readonly matched: { readonly count: number } } => {
	const { marker, times } = options;
	const matched = { count: 0 };
	const take = () => ++matched.count <= times;
	const query = (...args: ReadonlyArray<unknown>): Query => {
		const request = Reflect.apply(session.query, session, args) as Query;
		return request.inner.query.includes(marker)
			? wrapQuery(request, marker, take)
			: request;
	};
	const transaction = async <R extends unknown[]>(
		queries: ReadonlyArray<QueryLike>,
		options?: TransactionOptions,
	): Promise<R> => {
		if (queries.some((query) => textOf(query).includes(marker))) {
			options?.signal?.throwIfAborted();
			if (take()) throw conflict();
		}
		return session.transaction<R>(queries, options);
	};
	return { session: forward(session, { query, transaction }), matched };
};
