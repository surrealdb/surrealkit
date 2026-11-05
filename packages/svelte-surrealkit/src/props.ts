/**
 * The prop types of `<Query>` and `<LiveQuery>`, inferred from one type
 * parameter: the type of `q` itself, whether it is query text, a `preload`
 * payload or a handle. The query text is read off it, so a component is
 * typed by its query however `q` is given, and `params` depends on the form:
 * text and payloads take the query's parameters, a handle takes none.
 */

import type {
	LiveText,
	LiveTextError,
	ParamsProp,
	Preloaded,
	QueryText,
} from 'surrealkit-query';

import type { LiveHandle, QueryHandle } from './runes.svelte.js';

/** What `<Query q>` accepts. */
export type QuerySource =
	| QueryText
	| Preloaded<QueryText>
	| QueryHandle<unknown, QueryText>;

/** What `<LiveQuery q>` accepts. */
export type LiveSource =
	| QueryText
	| Preloaded<QueryText>
	| LiveHandle<unknown, QueryText>;

/** The query text of a source. */
export type TextOf<S> = S extends string
	? S
	: S extends { readonly text: infer T extends string }
		? T
		: never;

/** Whether a source is a handle from `createQuery` or `createLive`. */
type IsHandle<S> = S extends { readonly refetch: unknown } ? true : false;

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
 * The `params` prop for a source: the query's parameters for text and
 * payloads, required when the query needs one; none for a handle, which
 * carries its own.
 */
export type ParamsFor<S> =
	IsHandle<S> extends true ? { params?: never } : ParamsProp<TextOf<S>>;
