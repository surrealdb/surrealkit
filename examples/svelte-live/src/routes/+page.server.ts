import { preload } from 'svelte-surrealkit';

import { connect } from '$lib/db';

import type { PageServerLoad } from './$types';

const PAGE_SIZE = 5;

// The first page renders on the server. The page component subscribes to
// exactly this query from the payload, so the text is written once.
export const load: PageServerLoad = async () => {
	const db = await connect();
	try {
		return {
			people: await preload(
				db,
				'SELECT id, name, team, joined FROM person ORDER BY name LIMIT $lim START $start',
				{ lim: PAGE_SIZE, start: 0 },
			),
		};
	} finally {
		await db.close();
	}
};
