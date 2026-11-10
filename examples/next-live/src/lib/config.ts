/** The database `npm run db` starts; set NEXT_PUBLIC_SURREAL_URL to use another. */
export const SURREAL_URL =
	process.env.NEXT_PUBLIC_SURREAL_URL ?? 'ws://127.0.0.1:8000';
export const SCOPE = { namespace: 'demo', database: 'demo' };
