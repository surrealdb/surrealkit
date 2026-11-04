/**
 * `createQuery` and `createLive`: typed queries as Svelte 5 reactive state.
 *
 * ```svelte
 * <script lang="ts">
 * 	import { createLive } from 'svelte-surrealkit';
 *
 * 	let lim = $state(10);
 * 	let start = $state(0);
 * 	const users = createLive(
 * 		'SELECT * FROM user ORDER BY name LIMIT $lim START $start',
 * 		() => ({ lim, start }),
 * 	);
 * </script>
 *
 * {#each users.data as user (user.id)}<p>{user.name}</p>{/each}
 * ```
 *
 * The query text is a key of the generated `Queries` registry, which types
 * `data` and the parameters. Parameters are a getter, so the query follows
 * the `$state` and `$derived` values it reads: when they change to different
 * values the old query stops (a live one is killed) and the new one starts.
 * A getter that returns equal values starts nothing.
 *
 * The handle reads lazily: the query starts the first time `data`, `status`,
 * `error` or `loading` is read inside an effect (markup counts), and stops
 * once nothing reads it, which includes the component being destroyed. That
 * works in a component and in a shared `.svelte.ts` module alike.
 *
 * `q` may also be a {@link Preloaded} payload from a `load` function, which
 * renders the server's result at once, or a getter returning either.
 */
import { type DataOf, type LiveRowOf, type LiveText, type ParamsOf, type ParamsOptional, type Preloaded, type QueryStatus, type QueryText, type SurrealLike, type SurrealQueryError } from 'surrealkit-query';
/** A query's reactive view. */
export interface QueryHandle<T> {
    /** The result, once there is one. */
    readonly data: T | undefined;
    readonly error: SurrealQueryError | undefined;
    readonly status: QueryStatus;
    /** `status === 'pending'`. */
    readonly loading: boolean;
    /** Runs the query again. */
    refetch(): Promise<void>;
}
/** A live query's reactive view. `data` is always the current rows. */
export interface LiveHandle<Row> extends Omit<QueryHandle<Row[]>, 'data'> {
    /** The current rows, `[]` until the first snapshot. */
    readonly data: Row[];
}
export interface CreateOptions {
    /** The client to use instead of the one {@link setSurreal} provided. */
    client?: SurrealLike;
}
/** A value, or a getter for one that is re-read when its dependencies change. */
export type Getter<T> = T | (() => T);
/** A query's text or payload, or a getter for either. */
export type QuerySource<Q extends string> = Getter<Q | Preloaded<Q>>;
/**
 * The arguments after `q`: a getter for the parameters (required when `Q`
 * needs any), then options. The parameters are always a getter: a function
 * has a `name`, so a union of an object and a getter would accept a getter
 * returning the wrong parameters.
 */
export type CreateArgs<Q extends string> = ParamsOptional<Q> extends true ? [params?: () => ParamsOf<Q> | undefined, options?: CreateOptions] : [params: () => ParamsOf<Q>, options?: CreateOptions];
/** A one-shot query. `data` is the query's result. */
export declare function createQuery<Q extends QueryText>(q: QuerySource<Q>, ...args: CreateArgs<Q>): QueryHandle<DataOf<Q>>;
/**
 * A live query. `q` is a plain SELECT; the rows follow every change to the
 * records it selects. See the package README for which clauses are applied
 * to notifications directly and which re-run the SELECT.
 */
export declare function createLive<Q extends QueryText>(q: Getter<LiveText<Q> | Preloaded<Q>>, ...args: CreateArgs<Q>): LiveHandle<LiveRowOf<Q>>;
type Kind = 'query' | 'live';
/** Shared by the runes and the components. */
export declare function handle(kind: Kind, q: Getter<string | Preloaded>, params: (() => object | undefined) | undefined, options: CreateOptions | undefined): QueryHandle<unknown>;
export {};
