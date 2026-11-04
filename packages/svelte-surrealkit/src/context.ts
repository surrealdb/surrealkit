/**
 * The client every query below a component uses.
 *
 * Call {@link setSurreal} once, in the root layout, with the browser's
 * connected `Surreal`. On the server, render with a client scoped to the
 * request, never a module-level one: a server module is shared by every
 * request and every user.
 */

import {
	cacheFor,
	type QueryCacheOptions,
	type SurrealLike,
} from 'surrealkit-query';
import { getContext, hasContext, setContext } from 'svelte';

const KEY = Symbol.for('svelte-surrealkit.client');

/**
 * Makes `db` the client of every query in this component and below.
 * `options` configure the client's cache; pass the generated `liveQueries`
 * as `live` so live queries subscribe as the engine described them.
 */
export function setSurreal(
	db: SurrealLike,
	options?: QueryCacheOptions,
): SurrealLike {
	cacheFor(db, options);
	return setContext(KEY, db);
}

/**
 * The client {@link setSurreal} provided. Call it during component
 * initialisation. Throws when no ancestor set one.
 */
export function getSurreal(): SurrealLike {
	if (!hasContext(KEY)) {
		throw new Error(
			'svelte-surrealkit: no client. Call setSurreal(db) in a parent component, or pass { client }.',
		);
	}
	return getContext<SurrealLike>(KEY);
}
