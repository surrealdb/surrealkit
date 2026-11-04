/** Why a query is in its error state. */
export type SurrealQueryErrorCode =
	/** The query, or the live query's SELECT or LIVE statement, failed. */
	| 'query'
	/** The text cannot be made live; `message` says why. */
	| 'unsupported'
	/** The engine killed the live query (its table was removed, for one). */
	| 'killed';

/** A failed query, with the text and bindings that failed. */
export class SurrealQueryError extends Error {
	override readonly name = 'SurrealQueryError';
	readonly code: SurrealQueryErrorCode;
	readonly query: string;
	readonly params: Record<string, unknown> | undefined;

	constructor(
		code: SurrealQueryErrorCode,
		message: string,
		query: string,
		params: Record<string, unknown> | undefined,
		options?: { cause?: unknown },
	) {
		super(message, options);
		this.code = code;
		this.query = query;
		this.params = params;
	}

	/** Wraps whatever a query threw. */
	static from(
		cause: unknown,
		query: string,
		params: Record<string, unknown> | undefined,
	): SurrealQueryError {
		if (cause instanceof SurrealQueryError) return cause;
		const message = cause instanceof Error ? cause.message : String(cause);
		return new SurrealQueryError('query', message, query, params, {
			cause,
		});
	}
}
