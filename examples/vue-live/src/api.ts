/** The writes, through the server's routes (server/api.ts). */

type Status = 'todo' | 'doing' | 'done';

async function send(path: string, method: string, body?: unknown): Promise<void> {
	const response = await fetch(path, {
		method,
		headers: { 'content-type': 'application/json' },
		...(body === undefined ? {} : { body: JSON.stringify(body) }),
	});
	if (!response.ok) throw new Error(`${method} ${path}: ${response.status}`);
}

export const createTask = (title: string, owner: string) =>
	send('/api/tasks', 'POST', { title, owner });
export const moveTask = (id: string, status: Status) =>
	send(`/api/tasks/${id}`, 'PATCH', { status });
export const deleteTask = (id: string) => send(`/api/tasks/${id}`, 'DELETE');
