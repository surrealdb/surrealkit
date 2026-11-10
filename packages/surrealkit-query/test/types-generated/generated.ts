// The registry and live entries exactly as `surreal kit compile` writes them
// (test/types/queries.ts): the generated `liveQueries` is a manifest the
// cache accepts, and its `windowable: false` refusal is a type error.

import type { Surreal } from 'surrealdb';

import { cacheFor, type LiveText, type QueryText } from '../../src/index.js';
import {
	type LiveQueries,
	liveQueries,
	type Queries,
} from '../types/queries.js';

declare module '../../src/types.js' {
	interface Register {
		queries: Queries;
		liveQueries: LiveQueries;
	}
}

declare const db: Surreal;
cacheFor(db, { live: liveQueries });

function live<Q extends QueryText>(q: LiveText<Q>): Q {
	return q as Q;
}

live('SELECT * FROM user');
live('SELECT * FROM user ORDER BY name LIMIT $lim');
live('SELECT * FROM user WHERE team = $team');
// @ts-expect-error the engine refuses FROM ONLY, and re-running cannot serve it
live('SELECT * FROM ONLY user:ada');
