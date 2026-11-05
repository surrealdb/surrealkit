import type { Queries } from './queries.js';

declare module '../../src/types.js' {
	interface Register {
		queries: Queries;
	}
}
