// Type goldens for the hooks and the server entry, against a module
// `surreal kit compile` wrote. Each `@ts-expect-error` is a case the compiler
// must reject.

import { type RecordId, Surreal } from 'surrealdb';

import { LiveQuery, Query, useLive, useQuery } from '../../src/index.js';
import { type Preloaded, preload } from '../../src/server.js';
import './register.js';

const BY_NAME = 'SELECT name, email FROM user WHERE name = $name LIMIT $limit';

export function Typed({ name }: { name: string }) {
	const byName = useQuery(BY_NAME, { name, limit: 3 });
	byName.data satisfies Array<{ email: string; name: string }> | undefined;
	// @ts-expect-error a required parameter is missing
	useQuery(BY_NAME);
	// @ts-expect-error a parameter has the wrong type
	useQuery(BY_NAME, { name: 1 });
	// @ts-expect-error a query with no parameters takes none
	useQuery('SELECT kind FROM event', { stray: 1 });

	const events = useLive('SELECT * FROM event');
	events.data satisfies Array<{ id: RecordId<'event'>; kind: string }>;
	// @ts-expect-error FROM ONLY is not a row stream
	useLive('SELECT * FROM ONLY user:alice');
	// @ts-expect-error several statements are not a row stream
	useLive('USE NS app DB main; INFO FOR USER admin;');

	const untyped = useLive('SELECT * FROM elsewhere');
	untyped.data satisfies unknown[];
	return null;
}

export function Seeded({
	posts,
}: {
	posts: Preloaded<'SELECT kind FROM event'>;
}) {
	useLive(posts).data satisfies Array<{ kind: string }>;
	useQuery(posts).data satisfies Array<{ kind: string }> | undefined;
	return null;
}

export async function Page() {
	const db = new Surreal();
	const posts = await preload(db, 'SELECT kind FROM event');
	posts satisfies Preloaded<'SELECT kind FROM event'>;
	await preload(db, BY_NAME, { name: 'ada' });
	// @ts-expect-error a required parameter is missing
	await preload(db, BY_NAME);
	return <Seeded posts={posts} />;
}

export function Components({ name }: { name: string }) {
	const byName = useLive(BY_NAME, { name, limit: 3 });
	return (
		<>
			<LiveQuery q='SELECT kind FROM event'>
				{(rows) => rows.map((row) => row.kind satisfies string)}
			</LiveQuery>
			<Query q={BY_NAME} params={{ name }}>
				{(rows) => rows.map((row) => row.email satisfies string)}
			</Query>
			{/* A hook result carries its own params. */}
			<LiveQuery q={byName}>
				{(rows) => rows.map((row) => row.email satisfies string)}
			</LiveQuery>
			<LiveQuery
				q='SELECT kind FROM event'
				select={(rows) => rows.map((row) => row.kind)}
			>
				{(kinds) => kinds satisfies string[]}
			</LiveQuery>
			{/* @ts-expect-error a required parameter is missing */}
			<Query q={BY_NAME}>{() => null}</Query>
			{/* @ts-expect-error a hook result takes no params */}
			<LiveQuery q={byName} params={{ name }} />
			{/* @ts-expect-error FROM ONLY is not a row stream */}
			<LiveQuery q='SELECT * FROM ONLY user:alice' />
			<LiveQuery q='SELECT kind FROM event'>
				{/* @ts-expect-error not a field of the row */}
				{(rows) => rows.map((row) => row.nope)}
			</LiveQuery>
		</>
	);
}
