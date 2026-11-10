/**
 * The app's writes, as routes on Vite's dev and preview servers. They run in
 * Node with credentials only the server reads; the browser connects as a
 * guest that can read and nothing else. A route returns nothing: every list
 * is live, so a change reaches the page (and every other tab) by its live
 * query.
 *
 *   POST   /api/tasks       { title, owner? }
 *   PATCH  /api/tasks/:id   { status }
 *   DELETE /api/tasks/:id
 */

import type { IncomingMessage, ServerResponse } from 'node:http';

import { RecordId, Surreal } from 'surrealdb';
import type { Connect, Plugin } from 'vite';

// The generated module augments surrealkit-query's Register; TypeScript
// resolves that augmentation only when the program also imports the module.
import type {} from 'surrealkit-query';

import type { Typed } from '../src/generated/queries';

const SCOPE = { namespace: 'demo', database: 'demo' };
const STATUSES = ['todo', 'doing', 'done'] as const;

/** A client for one request, signed in with the server's credentials. */
async function connect(): Promise<Typed<Surreal>> {
	const db = new Surreal();
	await db.connect(process.env.SURREAL_URL ?? 'ws://127.0.0.1:8000', {
		...SCOPE,
		authentication: {
			username: process.env.SURREAL_USER ?? 'root',
			password: process.env.SURREAL_PASS ?? 'root',
		},
	});
	return db as Typed<Surreal>;
}

async function body(request: IncomingMessage): Promise<Record<string, unknown>> {
	let text = '';
	for await (const chunk of request) text += chunk;
	return text ? (JSON.parse(text) as Record<string, unknown>) : {};
}

async function handle(request: IncomingMessage, response: ServerResponse) {
	const [, id] = (request.url ?? '').match(/^\/api\/tasks(?:\/([\w-]+))?$/) ?? [];
	const db = await connect();
	try {
		if (request.method === 'POST' && id === undefined) {
			const { title, owner } = await body(request);
			if (typeof title !== 'string' || !title.trim()) return reply(response, 400);
			await db.query('CREATE task SET title = $title, owner = $owner', {
				title: title.trim(),
				owner: typeof owner === 'string' && owner ? new RecordId('person', owner) : undefined,
			});
			return reply(response, 204);
		}
		if (request.method === 'PATCH' && id) {
			const { status } = await body(request);
			const known = STATUSES.find((s) => s === status);
			if (!known) return reply(response, 400);
			await db.query('UPDATE $id SET status = $status', {
				id: new RecordId('task', id),
				status: known,
			});
			return reply(response, 204);
		}
		if (request.method === 'DELETE' && id) {
			await db.query('DELETE $id', { id: new RecordId('task', id) });
			return reply(response, 204);
		}
		return reply(response, 404);
	} catch (failure) {
		return reply(response, 500, String(failure));
	} finally {
		await db.close();
	}
}

function reply(response: ServerResponse, status: number, text = '') {
	response.statusCode = status;
	response.end(text);
}

const routes: Connect.NextHandleFunction = (request, response, next) => {
	if (!request.url?.startsWith('/api/tasks')) return next();
	void handle(request, response);
};

/** Serves `/api/tasks` from `vite` and `vite preview`. */
export function api(): Plugin {
	return {
		name: 'tasks-api',
		configureServer: (server) => void server.middlewares.use(routes),
		configurePreviewServer: (server) => void server.middlewares.use(routes),
	};
}
