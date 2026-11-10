import { redirect } from '@sveltejs/kit';
import { preload } from 'svelte-surrealkit';

import type { Actions, PageServerLoad } from './$types';

const PAGE_SIZE = 5;

// The first page and the count render on the server, as the signed-in user.
// The page subscribes to exactly these queries from the payloads, so each
// text is written once.
export const load: PageServerLoad = async ({ locals }) => {
	if (!locals.token) redirect(303, '/login');
	const [people, count] = await Promise.all([
		preload(
			locals.db,
			'SELECT id, name, team, joined FROM person ORDER BY name LIMIT $lim START $start',
			{ lim: PAGE_SIZE, start: 0 },
		),
		preload(locals.db, 'SELECT count() AS total FROM person GROUP ALL'),
	]);
	return { people, count };
};

export const actions: Actions = {
	signout: ({ cookies }) => {
		cookies.delete('token', { path: '/' });
		redirect(303, '/login');
	},
};
