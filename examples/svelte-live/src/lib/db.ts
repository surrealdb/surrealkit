import { Surreal } from 'surrealdb';

import type { Typed } from '$lib/generated/queries';

/** The database `npm run db` starts; set VITE_SURREAL_URL to use another. */
export const SURREAL_URL: string =
	import.meta.env.VITE_SURREAL_URL ?? 'ws://127.0.0.1:8000';
export const SCOPE = { namespace: 'demo', database: 'demo' };
/** The record access users sign in with (database/schema/account.surql). */
export const ACCESS = 'account';

/**
 * A client connected as the user `token` signed in, or as a guest without
 * one. The server makes one per request (see hooks.server.ts) and closes it
 * after, since a server module is shared by every request and every user.
 */
export async function connect(token?: string): Promise<Typed<Surreal>> {
	const db = new Surreal();
	await db.connect(SURREAL_URL, token ? { ...SCOPE, authentication: token } : SCOPE);
	return db as Typed<Surreal>;
}
