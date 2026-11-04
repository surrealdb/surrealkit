export {
	cacheFor,
	type DataOf,
	isPreloaded,
	type LiveManifest,
	type LiveRowOf,
	type ParamsOf,
	type Preloaded,
	preload,
	type QueryCache,
	type QueryCacheOptions,
	type QueryStatus,
	type QueryText,
	type Register,
	type SurrealLike,
	SurrealQueryError,
} from 'surrealkit-query';
export { getSurreal, setSurreal } from './context.js';
export { default as LiveQuery } from './LiveQuery.svelte';
export { default as Query } from './Query.svelte';
export {
	type CreateArgs,
	type CreateOptions,
	createLive,
	createQuery,
	type Getter,
	type LiveHandle,
	type QueryHandle,
	type QuerySource,
} from './runes.svelte.js';
