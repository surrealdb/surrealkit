// With the engine's `LiveQueries` registered, a query whose refusal is not
// windowable is a type error; a windowable one is accepted.

import type { Surreal } from 'surrealdb';

import { cacheFor, type LiveText, type QueryText } from '../../src/index.js';
import type { Queries } from '../types/queries.js';

/** The engine's live entries, as typegen emits them. */
const liveQueries = {
	'SELECT kind FROM event': { query: 'LIVE SELECT kind FROM event' },
	'SELECT * FROM event': {
		refusal: { clauses: ['FROM a, b'], windowable: false },
	},
	'SELECT name, email FROM user WHERE name = $name LIMIT $limit': {
		query: 'LIVE SELECT name, email FROM user WHERE name = $name',
		exit: 'silent',
		watch: 'LIVE SELECT id FROM user',
		refusal: { clauses: ['LIMIT'], windowable: true },
	},
} as const;

type LiveQueries = {
	[K in keyof typeof liveQueries]: (typeof liveQueries)[K] & {
		notification: unknown;
	};
};

// The value is a manifest the cache accepts.
cacheFor(null as unknown as Surreal, { live: liveQueries });

declare module '../../src/types.js' {
	interface Register {
		queries: Queries;
		liveQueries: LiveQueries;
	}
}

function live<Q extends QueryText>(q: LiveText<Q>): Q {
	return q as Q;
}

live('SELECT kind FROM event');
live('SELECT name, email FROM user WHERE name = $name LIMIT $limit');
// @ts-expect-error the engine says re-running cannot serve this query live
live('SELECT * FROM event');
// @ts-expect-error not one array of rows, with or without the engine's entry
live('SELECT * FROM ONLY user:alice');
