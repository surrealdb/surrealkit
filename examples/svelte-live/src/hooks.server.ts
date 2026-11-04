import type { Handle } from '@sveltejs/kit';

import { connect } from '$lib/db';

/**
 * One client per request, signed in as the request's user. The server never
 * signs in as root: queries run with the user's permissions, as they do in
 * the browser.
 */
export const handle: Handle = async ({ event, resolve }) => {
	const token = event.cookies.get('token');
	try {
		event.locals.db = await connect(token);
		event.locals.token = token;
	} catch {
		// An expired or revoked token: carry on as a guest.
		event.cookies.delete('token', { path: '/' });
		event.locals.db = await connect();
		event.locals.token = undefined;
	}
	try {
		return await resolve(event);
	} finally {
		await event.locals.db.close();
	}
};
