/**
 * Parts generated with schema detail, run on a real engine that holds the
 * same schema: the codecs their `binds` accept are the codecs the engine
 * stores the values as.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Effect, Schema } from 'effect';
import { RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { renderStatementModulesSync } from '../src/analyze/index.js';
import { type Client, make as makeClient } from '../src/client/index.js';
import { declaredNames, decodeDocument } from '../src/emitter/index.js';
import * as SurrealSchema from '../src/index.js';
import { sequence } from '../src/statement/index.js';
import { readJson } from './documents.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import { gauges } from './golden/statements-binds/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] binds engine suite skipped: ${engineSkipReason}`,
	);
}

/** The schema of `fixtures/binds.v2.json`, as SurrealQL. */
const schema = `
DEFINE TABLE site SCHEMAFULL;
DEFINE FIELD id ON site TYPE string;
DEFINE FIELD label ON site TYPE string;
DEFINE TABLE gauge SCHEMAFULL;
DEFINE FIELD station ON gauge TYPE string;
DEFINE FIELD reading ON gauge TYPE float;
DEFINE FIELD note ON gauge TYPE option<string>;
DEFINE FIELD site ON gauge TYPE record<site>;
DEFINE FIELD tags ON gauge TYPE array<string>;
DEFINE TABLE scratch SCHEMALESS;
DEFINE FUNCTION fn::scaled($value: float) -> float { RETURN $value * 2 };
`;

describe.skipIf(!engineConfigured)('parts with binds on an engine', () => {
	let engine: Engine;
	let client: Client;

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		await within(Promise.resolve(engine.db.query(schema)));
		await within(
			Promise.resolve(
				engine.db.query("CREATE site:north SET label = 'North';"),
			),
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('generates the parts with binds from the schema document', async () => {
		const document = await Effect.runPromise(
			decodeDocument(
				readJson(
					join(import.meta.dirname, 'fixtures', 'binds.v2.json'),
				),
			),
		);
		const sql = readFileSync(
			join(
				import.meta.dirname,
				'fixtures',
				'statements-binds',
				'gauges',
				'readings.surql',
			),
			'utf8',
		);
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'gauges',
					files: [{ name: 'readings', sql }],
					declared: declaredNames(document),
				},
			],
		});
		expect(files[0]?.content).toContain(
			'binds: { "floor": "float", "station": "string" }',
		);
		expect(gauges.readings.binds).toEqual({
			floor: 'float',
			station: 'string',
		});
	});

	it('runs the statements with the codecs their binds accept', async () => {
		const record = sequence(gauges.record).define({
			name: 'gauges.record',
			params: {
				raw: SurrealSchema.Float,
				site: SurrealSchema.RecordIdFor('site'),
				station: SurrealSchema.String.pipe(Schema.brand('app/Station')),
				tags: Schema.Array(SurrealSchema.String),
			},
			result: Schema.Unknown,
		});
		const readings = sequence(gauges.readings).define({
			name: 'gauges.readings',
			params: {
				floor: SurrealSchema.Float,
				station: SurrealSchema.String.check(Schema.isMinLength(1)),
			},
			result: Schema.Array(SurrealSchema.Float),
		});
		const site = sequence(gauges.site).define({
			name: 'gauges.site',
			params: { siteKey: SurrealSchema.String },
			result: Schema.Array(SurrealSchema.String),
		});
		const north = new RecordId('site', 'north');
		for (const raw of [1.5, 4, 0.25])
			await within(
				Effect.runPromise(
					client.run(record, {
						raw,
						site: north,
						station: Schema.decodeSync(
							SurrealSchema.String.pipe(
								Schema.brand('app/Station'),
							),
						)('west'),
						tags: ['daily'],
					}),
				),
			);
		expect(
			await within(
				Effect.runPromise(
					client.run(readings, { floor: 1, station: 'west' }),
				),
			),
		).toEqual([3, 8]);
		expect(
			await within(
				Effect.runPromise(client.run(site, { siteKey: 'north' })),
			),
		).toEqual(['North']);
	});
});
