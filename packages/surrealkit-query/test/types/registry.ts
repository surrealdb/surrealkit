// Type goldens for the registry types, against a module `surreal kit compile`
// wrote (test/types/queries.ts). Each `@ts-expect-error` is a case the
// compiler must reject.

import { type RecordId, Surreal } from 'surrealdb';

import {
	cacheFor,
	type DataOf,
	type LiveRowOf,
	type LiveText,
	type ParamsArg,
	type ParamsOf,
	preloadQuery,
	type QueryText,
} from '../../src/index.js';
import './register.js';

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
function expectType<T extends true>(_: T): void {}

type ByName = 'SELECT name, email FROM user WHERE name = $name LIMIT $limit';
type Alice = 'SELECT * FROM ONLY user:alice';
type Posts =
	'SELECT * FROM post WHERE published = $published AND author = $author';

// One statement resolves to its own result.
expectType<Equal<DataOf<ByName>, Array<{ email: string; name: string }>>>(true);
expectType<
	Equal<
		DataOf<Alice>,
		{ bio?: string; id: RecordId<'user'>; name: string } | undefined
	>
>(true);
// Several statements resolve to the tuple.
expectType<
	Equal<
		DataOf<'USE NS app DB main; INFO FOR USER admin;'>,
		[{ database?: string; namespace?: string }, string]
	>
>(true);

// Parameters: required when any is, optional otherwise.
expectType<
	Equal<ParamsArg<ByName>, [params: { name: string; limit?: number }]>
>(true);
// biome-ignore lint/complexity/noBannedTypes: the generated module writes `params: {}`.
expectType<Equal<ParamsArg<'SELECT kind FROM event'>, [params?: {}]>>(true);
const posts: ParamsOf<Posts> = {
	published: true,
	author: undefined,
};
void posts;
// @ts-expect-error a required parameter is missing
const missing: ParamsOf<Posts> = { published: true };
void missing;
// @ts-expect-error a parameter has the wrong type
const wrong: ParamsOf<ByName> = { name: 1 };
void wrong;

// Live rows are the SELECT's rows.
expectType<Equal<LiveRowOf<'SELECT kind FROM event'>, { kind: string }>>(true);
expectType<Equal<LiveText<'SELECT kind FROM event'>, 'SELECT kind FROM event'>>(
	true,
);
// Text whose result is not one array of rows cannot be live.
expectType<Equal<LiveRowOf<Alice>, never>>(true);
function live<Q extends QueryText>(q: LiveText<Q>): Q {
	return q as Q;
}
live('SELECT kind FROM event');
// @ts-expect-error FROM ONLY is not a row stream
live('SELECT * FROM ONLY user:alice');
// @ts-expect-error several statements are not a row stream
live('USE NS app DB main; INFO FOR USER admin;');

// Unregistered text is untyped while the registry is not strict.
expectType<Equal<DataOf<'SELECT * FROM elsewhere'>, unknown[]>>(true);
expectType<Equal<LiveRowOf<'SELECT * FROM elsewhere'>, unknown>>(true);
live('SELECT * FROM elsewhere');
const text: QueryText = 'SELECT * FROM elsewhere';
void text;

// A connected `Surreal` is a client the cache accepts.
const db = new Surreal();
cacheFor(db);
void preloadQuery(db, 'SELECT kind FROM event');
