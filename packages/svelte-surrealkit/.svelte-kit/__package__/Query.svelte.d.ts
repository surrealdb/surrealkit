import type { DataOf, Preloaded, QueryText, SurrealLike, SurrealQueryError } from 'surrealkit-query';
import type { Snippet } from 'svelte';
declare function $$render<Q extends QueryText>(): {
    props: {
        /** The query text, or a payload from `preload`. */
        q: Q | Preloaded<Q>;
        /** The client to use instead of the one `setSurreal` provided. */
        client?: SurrealLike | undefined;
        /** Rendered with the result. */
        children: Snippet<[DataOf<Q>]>;
        /** Rendered until the first result. */
        loading?: Snippet;
        /** Rendered on failure. */
        error?: Snippet<[SurrealQueryError]>;
    } & {
        params?: import("surrealkit-query").ParamsOf<Q> | undefined;
    } & (import("surrealkit-query").ParamsOptional<Q> extends true ? Record<never, never> : {
        params: import("surrealkit-query").ParamsOf<Q>;
    });
    exports: {};
    bindings: "";
    slots: {};
    events: {};
};
declare class __sveltets_Render<Q extends QueryText> {
    props(): ReturnType<typeof $$render<Q>>['props'];
    events(): ReturnType<typeof $$render<Q>>['events'];
    slots(): ReturnType<typeof $$render<Q>>['slots'];
    bindings(): "";
    exports(): {};
}
interface $$IsomorphicComponent {
    new <Q extends QueryText>(options: import('svelte').ComponentConstructorOptions<ReturnType<__sveltets_Render<Q>['props']>>): import('svelte').SvelteComponent<ReturnType<__sveltets_Render<Q>['props']>, ReturnType<__sveltets_Render<Q>['events']>, ReturnType<__sveltets_Render<Q>['slots']>> & {
        $$bindings?: ReturnType<__sveltets_Render<Q>['bindings']>;
    } & ReturnType<__sveltets_Render<Q>['exports']>;
    <Q extends QueryText>(internal: unknown, props: ReturnType<__sveltets_Render<Q>['props']> & {}): ReturnType<__sveltets_Render<Q>['exports']>;
    z_$$bindings?: ReturnType<__sveltets_Render<any>['bindings']>;
}
/**
 * A one-shot query, rendered from its result.
 *
 * ```svelte
 * <Query q="SELECT * FROM user LIMIT $lim START $start" params={{ lim, start }}>
 * 	{#snippet children(users)}
 * 		{#each users as user (user.id)}<p>{user.name}</p>{/each}
 * 	{/snippet}
 * 	{#snippet loading()}<p>Loading…</p>{/snippet}
 * 	{#snippet error(failure)}<p>{failure.message}</p>{/snippet}
 * </Query>
 * ```
 *
 * `q` is a key of the generated `Queries` registry, or a `Preloaded` payload
 * from a `load` function. It types the `children` argument and `params`,
 * which is required when the query has a required parameter. The query runs
 * again when `params` changes to different values.
 *
 * `children` renders once there is a result, `loading` until then, and
 * `error` on failure. With no `error` snippet a failure is thrown, so the
 * nearest `<svelte:boundary>` handles it.
 */
declare const Query: $$IsomorphicComponent;
type Query<Q extends QueryText> = InstanceType<typeof Query<Q>>;
export default Query;
