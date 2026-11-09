import { preload } from 'react-surrealkit/server';

import { getDb } from '@/lib/db.server';

/** Every task, newest first: the board's first paint, and then its live query. */
export const loadTasks = async () =>
	preload(
		await getDb(),
		'SELECT id, title, status, owner.name AS owner, created FROM task ORDER BY created DESC',
	);

/** The payload `loadTasks` returns, for the client component that shows it. */
export type TasksPayload = Awaited<ReturnType<typeof loadTasks>>;
