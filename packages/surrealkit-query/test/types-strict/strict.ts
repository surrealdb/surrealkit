// With `strict: true`, text outside the registry is a type error.

import type { QueryText } from '../../src/index.js';
import type { Queries } from '../types/queries.js';

declare module '../../src/types.js' {
	interface Register {
		queries: Queries;
		strict: true;
	}
}

const known: QueryText = 'SELECT kind FROM event';
void known;
// @ts-expect-error not in the registry
const unknown: QueryText = 'SELECT * FROM elsewhere';
void unknown;
