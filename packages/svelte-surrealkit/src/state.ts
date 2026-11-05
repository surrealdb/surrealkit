import type { SurrealQueryError } from 'surrealkit-query';

/** The state `<Query>` and `<LiveQuery>` pass to `children` beside the result. */
export interface QueryViewState {
	/** Newer results are on their way: today, exactly when `stale` is set. */
	readonly loading: boolean;
	/**
	 * The result is the previous parameters', kept while the result for the
	 * new parameters loads.
	 */
	readonly stale: boolean;
	/**
	 * A failure that left a result to show, when no `error` snippet claimed
	 * it.
	 */
	readonly error: SurrealQueryError | undefined;
}
