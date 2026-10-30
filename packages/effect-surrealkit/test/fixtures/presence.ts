/**
 * The field-presence evidence table (`semantics/field_presence.surql` in the
 * golden corpus): its DEFINE statements, and a capture of how an engine
 * stores them (`INFO … STRUCTURE`), from which a schema document is derived.
 */
import { readFileSync } from 'node:fs';

import type { Surreal } from 'surrealdb';

import { corpusPath } from '../documents.js';

/** Every `DEFINE …;` statement of the evidence file, in order. */
export const presenceDefinitions = (): string[] =>
	readFileSync(corpusPath('semantics', 'field_presence.surql'), 'utf8')
		.split('\n')
		.filter((line) => line.startsWith('DEFINE '));

type Json = Record<string, unknown>;

/** `INFO FOR DB STRUCTURE` plus `INFO FOR TABLE <t> STRUCTURE` for every table. */
export const captureStructure = async (
	db: Surreal,
): Promise<{ db: Json; tables: Json }> => {
	const [info] = await db.query<[Json]>('INFO FOR DB STRUCTURE');
	const tables: Json = {};
	for (const table of (info.tables as Array<{ name: string }>)
		.map((t) => t.name)
		.sort()) {
		const [structure] = await db.query<[Json]>(
			`INFO FOR TABLE ${table} STRUCTURE`,
		);
		tables[table] = structure;
	}
	// STRUCTURE carries SDK values (table names as `Table`); compare as JSON.
	return JSON.parse(JSON.stringify({ db: info, tables })) as {
		db: Json;
		tables: Json;
	};
};
