import { Surreal } from 'surrealdb';

import type { Typed } from '$lib/generated/queries';

export const SURREAL_URL = 'ws://127.0.0.1:8000';
export const SCOPE = { namespace: 'demo', database: 'demo' };

/**
 * A connected client. In the browser, one for the page's lifetime; on the
 * server, call it per request and close it after, since a server module is
 * shared by every request.
 */
export async function connect(): Promise<Typed<Surreal>> {
	const db = new Surreal();
	await db.connect(SURREAL_URL, SCOPE);
	return db as Typed<Surreal>;
}
