import type { LiveRowOf, LiveText, Preloaded, QueryText, SurrealLike, SurrealQueryError } from 'surrealkit-query';
import type { Snippet } from 'svelte';
declare function $$render<Q extends QueryText>(): {
    props: {
        /** The SELECT to make live, or a payload from `preload`. */
        q: LiveText<Q> | Preloaded<Q>;
        /** The client to use instead of the one `setSurreal` provided. */
        client?: SurrealLike | undefined;
        /** Rendered with the current rows. */
        children: Snippet<[LiveRowOf<Q>[]]>;
        /** Rendered until the first snapshot. */
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
 * A live query, rendered from its current rows.
 *
 * ```svelte
 * <LiveQuery q="SELECT * FROM user ORDER BY name LIMIT $lim START $start" params={{ lim, start }}>
 * 	{#snippet children(users)}
 * 		{#each users as user (user.id)}<p>{user.name}</p>{/each}
 * 	{/snippet}
 * </LiveQuery>
 * ```
 *
 * `q` is a plain SELECT that is a key of the generated `Queries` registry, or
 * a `Preloaded` payload; the component makes it live. Mounting subscribes,
 * unmounting kills the live query, and new `params` kill it and subscribe
 * again. Components showing the same query and parameters share one live
 * query.
 *
 * `children` renders with the rows once the first snapshot arrives (an empty
 * result renders `children([])`), `loading` until then, and `error` on
 * failure. With no `error` snippet a failure is thrown, so the nearest
 * `<svelte:boundary>` handles it.
 */
declare const LiveQuery: $$IsomorphicComponent;
type LiveQuery<Q extends QueryText> = InstanceType<typeof LiveQuery<Q>>;
export default LiveQuery;
