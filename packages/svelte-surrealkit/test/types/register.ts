import type { Queries } from '../../../surrealkit-query/test/types/queries.js';

declare module 'surrealkit-query' {
	interface Register {
		queries: Queries;
	}
}
