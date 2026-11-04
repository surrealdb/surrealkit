/**
 * The client every query below a component uses.
 *
 * Call {@link setSurreal} once, in the root layout, with the browser's
 * connected `Surreal`. On the server, render with a client scoped to the
 * request, never a module-level one: a server module is shared by every
 * request and every user.
 */
import { getContext, hasContext, setContext } from 'svelte';
const KEY = Symbol.for('svelte-surrealkit.client');
/** Makes `db` the client of every query in this component and below. */
export function setSurreal(db) {
    return setContext(KEY, db);
}
/**
 * The client {@link setSurreal} provided. Call it during component
 * initialisation. Throws when no ancestor set one.
 */
export function getSurreal() {
    if (!hasContext(KEY)) {
        throw new Error('svelte-surrealkit: no client. Call setSurreal(db) in a parent component, or pass { client }.');
    }
    return getContext(KEY);
}
