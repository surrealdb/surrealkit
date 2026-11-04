/**
 * The client every query below a component uses.
 *
 * Call {@link setSurreal} once, in the root layout, with the browser's
 * connected `Surreal`. On the server, render with a client scoped to the
 * request, never a module-level one: a server module is shared by every
 * request and every user.
 */
import type { SurrealLike } from 'surrealkit-query';
/** Makes `db` the client of every query in this component and below. */
export declare function setSurreal(db: SurrealLike): SurrealLike;
/**
 * The client {@link setSurreal} provided. Call it during component
 * initialisation. Throws when no ancestor set one.
 */
export declare function getSurreal(): SurrealLike;
