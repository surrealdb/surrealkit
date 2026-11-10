// Applies database/schema to the server `npm run db` starts, in the `demo`
// namespace and database, and adds a few people and tasks. It signs in as
// root, which only server-side code may do: the browser connects as a guest.

import { readdirSync, readFileSync } from 'node:fs';

import { Surreal } from 'surrealdb';

const url = process.env.SURREAL_URL ?? 'ws://127.0.0.1:8000';
const db = new Surreal();
await db.connect(url, {
	authentication: {
		username: process.env.SURREAL_USER ?? 'root',
		password: process.env.SURREAL_PASS ?? 'root',
	},
});
await db.query(
	'DEFINE NAMESPACE IF NOT EXISTS demo; USE NS demo; DEFINE DATABASE IF NOT EXISTS demo;',
);
await db.use({ namespace: 'demo', database: 'demo' });
for (const file of readdirSync('database/schema').sort()) {
	const text = readFileSync(`database/schema/${file}`, 'utf8');
	await db.query(text.replaceAll(/^DEFINE (TABLE|FIELD) /gm, 'DEFINE $1 OVERWRITE '));
}
await db.query(`
	UPSERT person:ada SET name = 'Ada';
	UPSERT person:grace SET name = 'Grace';
	UPSERT person:linus SET name = 'Linus';
	UPSERT task:schema SET title = 'Write the schema', status = 'done', owner = person:ada;
	UPSERT task:types SET title = 'Generate the types', status = 'doing', owner = person:grace;
	UPSERT task:page SET title = 'Render the first page on the server', status = 'todo', owner = person:linus;
	UPSERT task:live SET title = 'Stream changes to the browser', status = 'todo';
	RELATE task:types->blocks->task:page;
`);
console.log(`seeded ${url} (ns demo, db demo)`);
await db.close();
