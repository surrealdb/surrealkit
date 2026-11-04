import type { Surreal } from 'surrealdb';

import type { Typed } from '$lib/generated/queries';

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
