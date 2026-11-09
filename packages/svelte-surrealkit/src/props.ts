/**
 * What Svelte's `<Query q>` and `<LiveQuery q>` accept. The rest of the prop
 * types are shared with every binding, in `surrealkit-query`.
 */

import type { Preloaded, QueryText } from 'surrealkit-query';

import type { LiveHandle, QueryHandle } from './runes.svelte.js';

/** What `<Query q>` accepts. */
export type QueryProp =
	| QueryText
	| Preloaded<QueryText>
	| QueryHandle<unknown, QueryText>;

/** What `<LiveQuery q>` accepts. */
export type LiveProp =
	| QueryText
	| Preloaded<QueryText>
	| LiveHandle<unknown, QueryText>;

export type { LiveGuard, ParamsFor, TextOf } from 'surrealkit-query';
