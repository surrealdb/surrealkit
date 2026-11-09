'use server';

import { RecordId } from 'surrealdb';

import { getDb } from '@/lib/db.server';

/**
 * Writes run here, on the server, with the server's credentials. They return
 * nothing: every list on the page is live, so the change reaches it (and
 * every other open tab) through its live query, with no refresh.
 */

type Status = 'todo' | 'doing' | 'done';

const task = (id: string) => new RecordId('task', id);

export async function createTask(form: FormData) {
	const title = String(form.get('title') ?? '').trim();
	const owner = String(form.get('owner') ?? '');
	if (!title) return;
	const db = await getDb();
	await db.query('CREATE task SET title = $title, owner = $owner', {
		title,
		owner: owner ? new RecordId('person', owner) : undefined,
	});
}

export async function moveTask(id: string, status: Status) {
	const db = await getDb();
	await db.query('UPDATE $id SET status = $status', { id: task(id), status });
}

export async function deleteTask(id: string) {
	const db = await getDb();
	await db.query('DELETE $id', { id: task(id) });
}
