/**
 * What can go wrong when a statement runs, one class per cause, so a caller
 * can tell a refused value from a failed request, a guard that fired, a
 * conflict worth retrying and an answer that is not what was declared.
 */
import { Data } from 'effect';

/**
 * A binding's value failed its codec. Nothing was sent: an out-of-range
 * decimal or datetime, for instance, would otherwise hang the request.
 */
export class StatementEncodeError extends Data.TaggedError(
	'StatementEncodeError',
)<{
	readonly statement: string;
	readonly binding: string;
	readonly message: string;
}> {}

/** The request failed: transport, permissions, a SurrealQL error. */
export class StatementRequestError extends Data.TaggedError(
	'StatementRequestError',
)<{
	readonly statement: string;
	readonly message: string;
	readonly cause: unknown;
}> {}

/**
 * The statement raised one of the `THROW` messages it declares. A message
 * it does not declare is a {@link StatementRequestError}.
 */
export class StatementThrown<
	Reason extends string = string,
> extends Data.TaggedError('StatementThrown')<{
	readonly statement: string;
	readonly reason: string;
}> {
	declare readonly reason: Reason;

	override get message(): string {
		return this.reason;
	}
}

/**
 * The engine reported a transaction conflict with a concurrent write. The
 * request changed nothing and can be sent again.
 */
export class StatementConflict extends Data.TaggedError('StatementConflict')<{
	readonly statement: string;
	readonly message: string;
	readonly cause: unknown;
}> {}

/**
 * The engine answered, and the answer is not what the statement declares:
 * a different number of results, a value where NONE was required, or a
 * final result its codec refuses. A write may already have been applied.
 */
export class StatementDecodeError extends Data.TaggedError(
	'StatementDecodeError',
)<{
	readonly statement: string;
	/** The 1-based result that failed, or 0 for the count of results. */
	readonly slot: number;
	readonly message: string;
}> {}

/** Every error of running a statement that can `THROW` the messages `T`. */
export type StatementError<Throws extends string = never> =
	| StatementEncodeError
	| StatementRequestError
	| StatementConflict
	| StatementDecodeError
	| ([Throws] extends [never] ? never : StatementThrown<Throws>);
