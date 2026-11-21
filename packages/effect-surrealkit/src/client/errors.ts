/**
 * What can go wrong when a statement runs, one class per cause, so a caller
 * can tell a refused value from a failed request, a guard that fired, a
 * conflict worth retrying and an answer that is not what was declared.
 */
import { Data } from 'effect';

/**
 * A binding's value failed its codec, or its encoded value is not one the
 * SDK sends without loss (`path` says where inside it). Nothing was sent:
 * an out-of-range decimal or datetime, for instance, would otherwise hang
 * the request, and a lone surrogate or a class instance would be altered.
 */
export class StatementEncodeError extends Data.TaggedError(
	'StatementEncodeError',
)<{
	readonly statement: string;
	readonly binding: string;
	/** Where inside the encoded value the problem is; empty for the value. */
	readonly path?: ReadonlyArray<string | number>;
	readonly message: string;
}> {}

/**
 * `transaction` refused its statements before sending anything, because
 * they cannot share one variable scope or one transaction: a binding
 * another statement `LET`s, one name bound to two different values, or a
 * statement that writes its own `BEGIN`, `COMMIT` or `CANCEL`.
 */
export class StatementScopeError extends Data.TaggedError(
	'StatementScopeError',
)<{
	readonly statement: string;
	/** The binding at issue; absent for a statement's own transaction. */
	readonly binding?: string;
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
 * it does not declare is a {@link StatementUnexpectedThrow}. When the part
 * that declares the message carries `source` and `throwsAt`, so does this.
 */
export class StatementThrown<
	Reason extends string = string,
> extends Data.TaggedError('StatementThrown')<{
	readonly statement: string;
	readonly reason: string;
	/** Where the declaring part came from, such as its `.surql` file. */
	readonly source?: string;
	/** The lines of `source` that throw this message. */
	readonly lines?: ReadonlyArray<number>;
}> {
	declare readonly reason: Reason;

	override get message(): string {
		return this.reason;
	}
}

/**
 * The statement raised a `THROW` whose message it does not declare: a
 * dynamic message, or text that changed without its declaration.
 */
export class StatementUnexpectedThrow extends Data.TaggedError(
	'StatementUnexpectedThrow',
)<{
	readonly statement: string;
	/** The thrown text. */
	readonly reason: string;
	readonly cause: unknown;
}> {
	override get message(): string {
		return `\`${this.statement}\` threw '${this.reason}', which it does not declare`;
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
 * final result its codec refuses. For a write, the client reports the failure
 * inside a {@link StatementWriteOutcomeError} and records whether the engine
 * acknowledged the write before decoding failed.
 */
export class StatementDecodeError extends Data.TaggedError(
	'StatementDecodeError',
)<{
	readonly statement: string;
	/** The 1-based result that failed, or 0 for the count of results. */
	readonly slot: number;
	/**
	 * The request succeeded and the statement is not known to be
	 * read-only, so whatever it wrote is committed although the result
	 * could not be decoded. The message says whether it certainly wrote or
	 * may have.
	 */
	readonly committed: boolean;
	readonly message: string;
}> {}

/**
 * A statement that always returns a row when it applies (a `create` or
 * `relate` that returns rows) returned NONE or an empty list. SurrealDB
 * reports a write the session's permissions deny as an empty result,
 * without an error, so the session may lack permission; nothing was
 * written by that statement.
 */
export class StatementEmptyResult extends Data.TaggedError(
	'StatementEmptyResult',
)<{
	readonly statement: string;
	/** The 1-based result that was empty. */
	readonly slot: number;
	readonly message: string;
}> {}

/**
 * A write or a statement that may write returned data its result codec refused.
 * `cause.committed` is true after the engine acknowledged the request. A
 * stream can fail while decoding provisional rows, before the write is
 * acknowledged. Read back the data before replaying the request.
 */
export class StatementWriteOutcomeError extends Data.TaggedError(
	'StatementWriteOutcomeError',
)<{
	/** The statement, or the transaction, that was sent. */
	readonly request: string;
	readonly cause: StatementDecodeError;
}> {
	override get message(): string {
		return this.cause.message;
	}
}

/** Every error of running a statement that can `THROW` the messages `T`. */
export type StatementError<Throws extends string = never> =
	| StatementEncodeError
	| StatementScopeError
	| StatementRequestError
	| StatementUnexpectedThrow
	| StatementConflict
	| StatementDecodeError
	| StatementEmptyResult
	| StatementWriteOutcomeError
	| ([Throws] extends [never] ? never : StatementThrown<Throws>);

/** The transport or engine lacks a required feature. No write request was sent. */
export class StatementUnsupported extends Data.TaggedError(
	'StatementUnsupported',
)<{
	readonly feature: 'interactive transaction' | 'live query' | 'stream';
	readonly statement?: string;
	readonly message: string;
	readonly cause?: unknown;
}> {}

/** The commit was sent and no answer arrived. Read back before retrying. */
export class TransactionOutcomeUnknown extends Data.TaggedError(
	'TransactionOutcomeUnknown',
)<{
	readonly message: string;
	readonly cause: unknown;
}> {}

/** A non-atomic bulk write failed after committing any preceding chunks. */
export class BulkWriteError<
	Throws extends string = never,
> extends Data.TaggedError('BulkWriteError')<{
	readonly statement: string;
	/** Zero-based index of the failed chunk. */
	readonly chunk: number;
	readonly chunksCommitted: number;
	readonly rowsCommitted: number;
	readonly cause: StatementError<Throws>;
}> {}

/** The live query ended without the caller closing it. */
export class LiveQueryClosed extends Data.TaggedError('LiveQueryClosed')<{
	readonly statement: string;
	readonly reason: 'killed' | 'disconnected';
	readonly message: string;
}> {}
