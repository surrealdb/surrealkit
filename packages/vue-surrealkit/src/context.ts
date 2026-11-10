/**
 * The client every query in a component tree uses, through provide/inject.
 *
 * ```ts
 * // main.ts
 * createApp(App).use(surrealkit, { client: db, live: liveQueries }).mount('#app');
 * ```
 *
 * or, for one subtree, `provideSurreal(db, { live: liveQueries })` in a
 * component's setup. Give the browser's client: on the server, render with a
 * client scoped to the request, never a module-level one, which every
 * request and every user would share.
 */

import {
	cacheFor,
	type LiveManifest,
	type QueryCacheOptions,
	type SurrealLike,
} from 'surrealkit-query';
import { type App, type InjectionKey, inject, provide } from 'vue';

const KEY: InjectionKey<SurrealLike> = Symbol.for('vue-surrealkit.client');

/**
 * Makes `client` the client of every query in this component and below.
 * `options` configure the client's cache; pass the generated `liveQueries`
 * as `live` so live queries subscribe as the engine described them.
 */
export function provideSurreal(
	client: SurrealLike,
	options?: QueryCacheOptions,
): SurrealLike {
	cacheFor(client, options);
	provide(KEY, client);
	return client;
}

/**
 * The client a query uses: `override` when given, else the provided one.
 * Call it during setup. Throws when there is neither.
 */
export function useSurreal(override?: SurrealLike): SurrealLike {
	const client = override ?? inject(KEY, null);
	if (!client) {
		throw new Error(
			'vue-surrealkit: no client. Install the plugin (app.use(surrealkit, { client })), call provideSurreal(db) in a parent component, or pass { client }.',
		);
	}
	return client;
}

export interface SurrealKitPluginOptions extends QueryCacheOptions {
	/** The client every query in the app uses. */
	client: SurrealLike;
	/** The generated `liveQueries`. */
	live?: LiveManifest;
}

/** `app.use(surrealkit, { client, live })`: provides the client app-wide. */
export const surrealkit = {
	install(app: App, options: SurrealKitPluginOptions): void {
		const { client, ...cache } = options;
		cacheFor(client, cache);
		app.provide(KEY, client);
	},
};
