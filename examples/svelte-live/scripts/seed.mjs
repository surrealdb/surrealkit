// Applies database/schema and seeds a few people into the `demo` namespace
// and database of the server `npm run db` starts.

import { readdirSync, readFileSync } from 'node:fs';

import { Surreal } from 'surrealdb';

const url = process.env.SURREAL_URL ?? 'ws://127.0.0.1:8000';
const db = new Surreal();
await db.connect(url);
await db.query('DEFINE NAMESPACE IF NOT EXISTS demo; USE NS demo; DEFINE DATABASE IF NOT EXISTS demo;');
await db.use({ namespace: 'demo', database: 'demo' });
for (const file of readdirSync('database/schema').sort()) {
	await db.query(readFileSync(`database/schema/${file}`, 'utf8'));
}
const people = [
	['ada', 'red'], ['bob', 'blue'], ['cy', 'red'], ['dee', 'green'], ['eve', 'blue'],
	['fay', 'red'], ['gus', 'green'], ['hal', 'blue'], ['ida', 'red'], ['jo', 'green'],
	['kim', 'blue'], ['lu', 'red'],
];
for (const [name, team] of people) {
	await db.query('UPSERT type::record("person", $name) SET name = $name, team = $team', { name, team });
}
console.log(`seeded ${people.length} people at ${url} (ns demo, db demo)`);
await db.close();
