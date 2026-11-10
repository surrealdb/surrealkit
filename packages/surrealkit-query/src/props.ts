/**
 * Prop types shared by every binding's `<Query>` and `<LiveQuery>`.
 *
 * A component infers one type parameter, the type of its `q` (query text, a
 * `preload` payload, or a handle or hook result for a query already made),
 * and reads the query text off it. So a component is typed by its query
 * however `q` is given, and `params` follows the form: text and payloads
 * take the query's parameters, a handle takes none, since it has its own.
 */

import type { SurrealQueryError } from './error.js';
import type { LiveText, LiveTextError, ParamsProp } from './types.js';

/** The query text of a `q`: the text itself, or a payload's or handle's `text`. */
export type TextOf<S> = S extends string
	? S
	: S extends { readonly text: infer T extends string }
		? T
		: never;

/** Whether a `q` is a handle (or hook result): anything with `refetch`. */
export type IsHandle<S> = S extends { readonly refetch: unknown }
	? true
	: false;

/**
 * `unknown` for text that can be live; for text that cannot, the brand that
 * makes `q` unsatisfiable, so the compiler names the error.
 */
export type LiveGuard<S> = S extends string
	? [LiveText<S>] extends [S & LiveTextError]
		? LiveTextError
		: unknown
	: unknown;

/**
 * The `params` prop for a `q`: the query's parameters for text and payloads,
 * required when the query needs one; none for a handle, which carries its
 * own.
 */
export type ParamsFor<S> =
	IsHandle<S> extends true ? { params?: never } : ParamsProp<TextOf<S>>;

/** The state a component passes to `children` beside the result. */
export interface QueryViewState {
	/** Newer results are on their way: today, exactly when `stale` is set. */
	readonly loading: boolean;
	/**
	 * The result is the previous parameters', kept while the result for the
	 * new parameters loads.
	 */
	readonly stale: boolean;
	/**
	 * A failure that left a result to show, when no `error` snippet or prop
	 * claimed it.
	 */
	readonly error: SurrealQueryError | undefined;
}

/**
 * Whether a component's result counts as empty, for its `empty` snippet or
 * prop: `undefined`, `null` or an empty array.
 */
export function isEmptyResult(value: unknown): boolean {
	return value == null || (Array.isArray(value) && value.length === 0);
}
