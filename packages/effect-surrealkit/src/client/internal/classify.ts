import {
	AuthenticationError,
	ExpressionError,
	InvalidSessionError,
	NotAllowedError,
	NotFoundError,
} from 'surrealdb';

import type { AnyStatement } from '../../statement/statement.js';
import {
	StatementConflict,
	StatementIndexConflict,
	StatementRecordExists,
	StatementRequestError,
	StatementThrown,
	StatementUnexpectedThrow,
} from '../errors.js';
import type { SlotAnswer } from './transport.js';

export const causedBy = (
	cause: unknown,
	matches: (error: Error) => boolean,
): boolean => {
	const visited = new Set<Error>();
	for (
		let error = cause;
		error instanceof Error && !visited.has(error);
		error = error.cause
	) {
		if (matches(error)) return true;
		visited.add(error);
	}
	return false;
};

export const isAuthenticationFailure = (cause: unknown): boolean =>
	causedBy(
		cause,
		(error) =>
			error instanceof AuthenticationError ||
			(error instanceof NotAllowedError &&
				error.details?.kind === 'Auth'),
	);

export const isClosedSession = (cause: unknown): boolean =>
	causedBy(
		cause,
		(error) =>
			error instanceof InvalidSessionError ||
			(error instanceof NotFoundError &&
				error.details?.kind === 'Session'),
	);

export type FailedSlot = Extract<SlotAnswer, { readonly ok: false }>;

/** The SDK marks statements rolled back by a different failure as not executed. */
export const isCollateralFailure = (error: unknown): boolean => {
	if (!(error instanceof Error)) return false;
	const query = error as Error & {
		readonly kind?: unknown;
		readonly details?: unknown;
		readonly isNotExecuted?: unknown;
		readonly isCancelled?: unknown;
	};
	if (query.kind !== undefined && query.kind !== 'Query') return false;
	if (query.isNotExecuted === true || query.isCancelled === true) return true;
	// Older servers can report the same condition without structured details.
	return (
		!query.details &&
		/^(?:The query was not executed due to a (?:failed|cancelled) transaction|Cannot COMMIT: the transaction was aborted due to a prior error)/u.test(
			query.message,
		)
	);
};

/** Preserve the causal error within this block, including a conflict reported only at COMMIT. */
export const firstFailure = (
	slots: ReadonlyArray<SlotAnswer>,
): FailedSlot | undefined => {
	let first: FailedSlot | undefined;
	for (const slot of slots) {
		if (slot.ok) continue;
		first ??= slot;
		if (!isCollateralFailure(slot.error)) return slot;
	}
	return first;
};

const thrownPrefix = 'An error occurred: ';

// The collector gives the SDK one query. Only complete, known diagnostic
// strings from the SDK's preflight can be shown without exposing its input.
const transactionRefusals = new Set([
	...['BEGIN', 'COMMIT', 'CANCEL'].map(
		(keyword) =>
			`queries[0] contains a ${keyword} statement. transaction() wraps the queries in BEGIN and COMMIT itself, so they must not contain any`,
	),
	'queries[0] contains a RETURN statement which is followed by another statement (in queries[0]). Inside a transaction RETURN ends it early: the statements after it would be skipped but the transaction would still commit. Use SELECT or a bare expression instead, or move the RETURN to the end',
]);

/** Internal live-registration diagnostics whose text is controlled by the client. */
export class LiveRegistrationError extends Error {
	constructor(
		problem: 'missing-id' | 'cleanup-unconfirmed',
		cause?: unknown,
	) {
		super(
			problem === 'missing-id'
				? 'LIVE SELECT did not return one query UUID'
				: 'live query attachment failed and cleanup could not be confirmed',
			{ cause },
		);
	}
}

export const requestErrorText = (error: unknown): string => {
	if (error instanceof LiveRegistrationError) return error.message;
	if (error instanceof ExpressionError) {
		const { message } = error;
		if (transactionRefusals.has(message)) return message;
	}
	return 'SurrealDB request failed';
};

/** The statement that declares a `THROW` message, and where. */
interface Declarer {
	readonly statement: string;
	readonly source?: string;
	readonly lines?: ReadonlyArray<number>;
}

/**
 * What declares the `THROW` message `reason`: the statement's name (which
 * ones when several do) and, when one statement does and records where,
 * the source and lines; `undefined` when none does.
 */
const declaredBy = (
	statements: ReadonlyArray<AnyStatement>,
	reason: string,
): Declarer | undefined => {
	const declaring = statements.filter((statement) =>
		(statement.throws as ReadonlyArray<string>).includes(reason),
	);
	const names = [...new Set(declaring.map((statement) => statement.name))];
	if (names.length > 1) {
		return {
			statement: `one of ${names.map((name) => `\`${name}\``).join(', ')}`,
		};
	}
	const [first] = declaring;
	if (first === undefined) return undefined;
	const site = first.throwSites?.[reason];
	return site === undefined
		? { statement: first.name }
		: { statement: first.name, source: site.source, lines: site.lines };
};

/** What an engine error reports, as far as the client reads it. */
interface Reported {
	readonly kind?: unknown;
	readonly details?: unknown;
	readonly message: string;
}

/**
 * The whole message SurrealDB 3.3 and 4.0 write for a duplicate on a unique
 * index, `Internal` with no details: the index, the duplicated value and the
 * record that holds it. `test/conformance.test.ts` pins it on both engines
 * ("constraint errors"); the protocol documentation says a message may
 * change between releases, so a message of any other form stays a request
 * error.
 */
const duplicateMessage =
	/^Database index `[\s\S]+` already contains [\s\S]+, with record `[\s\S]+`$/u;

/**
 * Whether the engine error `error` reports a duplicate on a unique index:
 * an error the server reported (it has a `kind`), other than a `THROW`, whose
 * whole message has the pinned form. Its kind and details are not read
 * further: surrealdb/surrealdb#7653 would report the same message as
 * `AlreadyExists` of kind `Record`, carrying the id of the record that holds
 * the value, so the message, not the kind, tells a duplicate from a record
 * that exists.
 */
export const isIndexDuplicate = (error: Reported): boolean =>
	typeof error.kind === 'string' &&
	error.kind !== 'Thrown' &&
	duplicateMessage.test(error.message);

/**
 * The declared unique index that the engine error `error` reports a
 * duplicate value of, or `undefined`. The name is read as the message's
 * prefix, ``Database index `<name>` already contains ``, for each name in
 * `declared`, so an index name the statement does not declare is never
 * read, and a name that contains a backtick is matched whole. The longest
 * declared name wins.
 */
export const conflictingIndex = (
	error: Reported,
	declared: Iterable<string>,
): string | undefined => {
	if (!isIndexDuplicate(error)) return;
	const { message } = error;
	let found: string | undefined;
	for (const index of declared) {
		if (
			message.startsWith(
				`Database index \`${index}\` already contains `,
			) &&
			(found === undefined || index.length > found.length)
		) {
			found = index;
		}
	}
	return found;
};

/**
 * Whether the engine error `error` is a record that already exists: a
 * structured `AlreadyExists` of kind `Record` that is not a duplicate on a
 * unique index (`isIndexDuplicate`).
 */
export const isRecordExists = (error: Reported): boolean => {
	if (error.kind !== 'AlreadyExists' || isIndexDuplicate(error)) return false;
	const { details } = error as {
		readonly details?: { readonly kind?: unknown };
	};
	return typeof details === 'object' && details?.kind === 'Record';
};

/**
 * The statement that declares the unique index `index`: its name, or which
 * ones when several do.
 */
const indexDeclarer = (
	statements: ReadonlyArray<AnyStatement>,
	index: string,
): string => {
	const names = [
		...new Set(
			statements
				.filter((statement) => statement.uniqueIndexes?.includes(index))
				.map((statement) => statement.name),
		),
	];
	return names.length === 1
		? (names[0] as string)
		: `one of ${names.map((name) => `\`${name}\``).join(', ')}`;
};

/**
 * Maps an SDK error to the client's error for the request `name`, which sent
 * `statements`: a declared `THROW`, a transaction conflict, a record that
 * exists or a duplicate on a declared unique index each get their own
 * class, and anything else is a request error.
 *
 * The SDK chains server errors through `cause`. They are matched by shape
 * rather than by class, so a second installed copy of the SDK still maps.
 */
export const classify = <Throws extends string, Unique extends string = never>(
	name: string,
	statements: ReadonlyArray<AnyStatement>,
	cause: unknown,
):
	| StatementRequestError
	| StatementConflict
	| StatementRecordExists
	| StatementIndexConflict<Unique>
	| StatementThrown<Throws>
	| StatementUnexpectedThrow => {
	const declared = new Set(
		statements.flatMap((statement) => statement.uniqueIndexes ?? []),
	);
	let unexpected: string | undefined;
	for (
		let error: unknown = cause;
		error instanceof Error;
		error = error.cause
	) {
		const { isTransactionConflict, kind } = error as {
			readonly isTransactionConflict?: unknown;
			readonly kind?: unknown;
		};
		if (kind === 'Thrown') {
			const reason = error.message.startsWith(thrownPrefix)
				? error.message.slice(thrownPrefix.length)
				: error.message;
			const declarer = declaredBy(statements, reason);
			if (declarer !== undefined) {
				return new StatementThrown<Throws>({ ...declarer, reason });
			}
			unexpected ??= reason;
		}
		if (isTransactionConflict === true) {
			return new StatementConflict({
				statement: name,
				message: 'Transaction conflict',
				cause,
			});
		}
		// A duplicate on a unique index is read before a record that exists,
		// whatever its kind: one on an index the statement does not declare
		// is a request error, never `StatementRecordExists`.
		if (isIndexDuplicate(error)) {
			const index = conflictingIndex(error, declared);
			if (index !== undefined) {
				return new StatementIndexConflict<Unique>({
					statement: indexDeclarer(statements, index),
					index,
					cause,
				});
			}
			continue;
		}
		if (isRecordExists(error)) {
			return new StatementRecordExists({
				statement: name,
				message: `\`${name}\` tried to create a record whose id already exists`,
				cause,
			});
		}
	}
	if (unexpected !== undefined) {
		return new StatementUnexpectedThrow({
			statement: name,
			reason: unexpected,
			cause,
		});
	}
	return new StatementRequestError({
		statement: name,
		message: requestErrorText(cause),
		cause,
	});
};
