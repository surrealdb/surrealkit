import type { AnyStatement } from '../../statement/statement.js';
import {
	StatementConflict,
	StatementRequestError,
	StatementThrown,
	StatementUnexpectedThrow,
} from '../errors.js';
import type { SlotAnswer } from './transport.js';

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

export const messageOf = (error: unknown): string =>
	error instanceof Error ? error.message : String(error);

/** The statement that declares a `THROW` message, and where. */
interface Declarer {
	readonly statement: string;
	readonly source?: string;
	readonly lines?: ReadonlyArray<number>;
}

/**
 * The SDK chains server errors through `cause`. They are matched by shape
 * rather than by class, so a second installed copy of the SDK still maps.
 */
export const classify = <Throws extends string>(
	name: string,
	/** What declares `reason`, or `undefined` when nothing does. */
	declaredBy: (reason: string) => Declarer | undefined,
	cause: unknown,
):
	| StatementRequestError
	| StatementConflict
	| StatementThrown<Throws>
	| StatementUnexpectedThrow => {
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
			const declarer = declaredBy(reason);
			if (declarer !== undefined) {
				return new StatementThrown<Throws>({ ...declarer, reason });
			}
			unexpected ??= reason;
		}
		if (isTransactionConflict === true) {
			return new StatementConflict({
				statement: name,
				message: error.message,
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
		message: messageOf(cause),
		cause,
	});
};

/**
 * What declares the `THROW` message `reason`: the statement's name (which
 * ones when several do) and, when one statement does and records where,
 * the source and lines; `undefined` when none does.
 */
export const declarer = (
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
