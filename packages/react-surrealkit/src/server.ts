/**
 * `react-surrealkit/server`: for Server Components, route handlers and
 * server actions. Nothing here is a client module.
 *
 * Use a client scoped to the request: a module-level client is shared by
 * every request and every user, along with its session. React's `cache()`
 * scopes one to a request:
 *
 * ```ts
 * // lib/db.server.ts
 * export const getDb = cache(async () => {
 * 	const db = new Surreal();
 * 	await db.connect(process.env.SURREAL_URL!, { namespace: 'app', database: 'app' });
 * 	return db;
 * });
 * ```
 *
 * Seed a client component with `preload`, naming the query once:
 *
 * ```tsx
 * // app/users/page.tsx
 * export default async function Page() {
 * 	const users = await preload(await getDb(), 'SELECT * FROM user LIMIT $lim', { lim: 10 });
 * 	return <Users users={users} />;
 * }
 *
 * // app/users/users.tsx
 * 'use client';
 * export function Users({ users }: { users: Preloaded<'SELECT * FROM user LIMIT $lim'> }) {
 * 	const live = useLive(users);
 * 	…
 * }
 * ```
 *
 * The payload is plain strings, so it crosses the Server Component boundary
 * as it is, and SDK values (`RecordId`, `DateTime`) arrive as themselves.
 */

export {
	type DataOf,
	isPreloaded,
	type ParamsOf,
	type Preloaded,
	preload,
	type QueryText,
	SurrealQueryError,
} from 'surrealkit-query';
