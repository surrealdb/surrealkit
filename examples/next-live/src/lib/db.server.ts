import { after } from 'next/server';
import { cache } from 'react';
import { Surreal } from 'surrealdb';

import type { Typed } from '@/generated/queries';
import { SCOPE, SURREAL_URL } from '@/lib/config';

/**
 * This request's client, connected on first use and closed once the
 * response is sent. `cache()` scopes it to the request: a module-level
 * client would be shared by every request and every user.
 *
 * The server signs in with credentials only it can read. The browser never
 * sees them: it connects as a guest, which the schema lets read and nothing
 * else, and writes go through server actions.
 */
export const getDb = cache(async (): Promise<Typed<Surreal>> => {
	const db = new Surreal();
	await db.connect(SURREAL_URL, {
		...SCOPE,
		authentication: {
			username: process.env.SURREAL_USER ?? 'root',
			password: process.env.SURREAL_PASS ?? 'root',
		},
	});
	after(() => db.close());
	return db as Typed<Surreal>;
});
