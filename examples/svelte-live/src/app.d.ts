import type { Surreal } from 'surrealdb';

import type { Queries, Typed } from '$lib/generated/queries';

// Types every <Query>, <LiveQuery>, createQuery and createLive by the
// generated registry.
declare module 'surrealkit-query' {
	interface Register {
		queries: Queries;
	}
}

declare global {
	namespace App {
		interface Locals {
			/** This request's client, signed in as its user (hooks.server.ts). */
			db: Typed<Surreal>;
			/** The user's record access token, when signed in. */
			token: string | undefined;
		}
	}
}

export {};
