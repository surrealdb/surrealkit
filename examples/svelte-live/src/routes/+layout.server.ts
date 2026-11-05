import type { LayoutServerLoad } from './$types';

// The browser authenticates its own connection with the same token, so it
// runs live queries with the user's permissions. The token expires (the
// access's DURATION FOR TOKEN); root credentials never come here.
export const load: LayoutServerLoad = ({ locals }) => ({
	token: locals.token ?? null,
});
