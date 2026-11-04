import type { Queries } from '$lib/generated/queries';

// Types every <Query>, <LiveQuery>, createQuery and createLive by the
// generated registry.
declare module 'surrealkit-query' {
	interface Register {
		queries: Queries;
	}
}

export {};
