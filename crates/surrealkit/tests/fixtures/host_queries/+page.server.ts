import { preload } from '$lib/db';

export const load = async ({ locals: { db } }) => ({
	people: await preload(db, "SELECT * FROM person"),
});
