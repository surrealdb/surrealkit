/**
 * Every typed aggregate over a decimal, an int, a float and a `number`
 * field, an optional field that is NONE in every record, and a `GROUP ALL`
 * over no record, on whichever engine the run uses (see `engine.ts`). Each
 * result is decoded strictly by the codec its statement derived, and the
 * raw values pin what SurrealDB 3.3 and 4.0 return: a decimal field's
 * `math::stddev`, `math::variance` and `math::median` are a decimal or a
 * number (a group of one record has the spread int 0; the median of an even
 * count is a float), while `math::mean` is always a number. The catalogue
 * functions that compute in the kind of their input (`math::sqrt`) decode
 * over the same stored fields, and those that may answer NONE do.
 */
import { Effect, Schema } from 'effect';
import { Decimal } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Client, make as makeClient } from '../src/client/index.js';
import { math, parse, vector } from '../src/functions/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	count,
	eq,
	max,
	mean,
	median,
	min,
	param,
	select,
	stddev,
	sum,
	variance,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] aggregate engine suite skipped: ${engineSkipReason}`,
	);
}

const MeasureId = SurrealSchema.RecordIdFor('measure');
const MeasureFields = {
	r: SurrealSchema.String,
	i: SurrealSchema.Int,
	f: SurrealSchema.Float,
	d: SurrealSchema.Decimal,
	n: SurrealSchema.SurrealNumber,
	od: SurrealSchema.Decimal,
} as const;
const Measure = Schema.Struct({
	id: MeasureId,
	r: MeasureFields.r,
	i: MeasureFields.i,
	f: MeasureFields.f,
	d: MeasureFields.d,
	n: MeasureFields.n,
	od: Schema.optionalKey(MeasureFields.od),
});
const MeasureWrite = Schema.Struct({
	r: MeasureFields.r,
	i: MeasureFields.i,
	f: MeasureFields.f,
	d: MeasureFields.d,
	n: MeasureFields.n,
	od: Schema.optionalKey(MeasureFields.od),
});
const required = {
	select: 'required',
	create: 'required',
	merge: 'optional',
	content: 'required',
} as const;
const MeasureTable = SurrealSchema.table({
	name: 'measure',
	type: 'normal',
	schemafull: true,
	drop: false,
	id: MeasureId,
	select: Measure,
	visible: Measure,
	create: MeasureWrite,
	merge: MeasureWrite,
	content: MeasureWrite,
	fields: MeasureFields,
	presence: {
		id: {
			select: 'required',
			create: 'excluded',
			merge: 'excluded',
			content: 'excluded',
		},
		r: required,
		i: required,
		f: required,
		d: required,
		n: required,
		od: {
			select: 'optional',
			create: 'optional',
			merge: 'removable',
			content: 'optional',
		},
	},
	relations: { in: [], out: [] },
	permissions: { select: false, create: false, update: false, delete: false },
	indexes: {},
});

const schema = `
	DEFINE TABLE measure SCHEMAFULL;
	DEFINE FIELD r ON measure TYPE string;
	DEFINE FIELD i ON measure TYPE int;
	DEFINE FIELD f ON measure TYPE float;
	DEFINE FIELD d ON measure TYPE decimal;
	DEFINE FIELD n ON measure TYPE number;
	DEFINE FIELD od ON measure TYPE option<decimal>;
	CREATE measure:1 CONTENT { r: 'a', i: 1, f: 1.5f, d: 1.5dec, n: 1.5dec };
	CREATE measure:2 CONTENT { r: 'a', i: 2, f: 2.5f, d: 2.5dec, n: 2 };
	CREATE measure:3 CONTENT { r: 'b', i: 4, f: 4f, d: 4dec, n: 4dec };
	CREATE measure:4 CONTENT { r: 'c', i: 1, f: 1f, d: 1dec, n: 1.5f };
	CREATE measure:5 CONTENT { r: 'c', i: 2, f: 2f, d: 2dec, n: 2.5f };
	CREATE measure:6 CONTENT { r: 'c', i: 6, f: 6f, d: 6dec, n: 6 };
`;

/** `Decimal(text)` for a decimal, `number(text)` for a number. */
const shape = (value: unknown): string =>
	value instanceof Decimal
		? `Decimal(${value.toString()})`
		: typeof value === 'number'
			? `number(${value})`
			: value === undefined
				? 'NONE'
				: `${typeof value}(${String(value)})`;

const shapes = (row: object): Record<string, string> =>
	Object.fromEntries(
		Object.entries(row).map(([key, value]) => [key, shape(value)]),
	);

describe.skipIf(!engineConfigured)('aggregates on the engine', () => {
	let engine: Engine;
	let client: Client;

	const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
		within(Effect.runPromise(effect), 20_000);

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		await engine.db.query(schema);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	it('a decimal field: stddev, variance and median are a decimal or a number, mean a number, sum, min and max decimals', async () => {
		const rows = await ok(
			client.run(
				select(MeasureTable)
					.groupBy((row) => ({ r: row.r }))
					.aggregate((row) => ({
						c: count(),
						sum: sum(row.d),
						mean: mean(row.d),
						median: median(row.d),
						sd: stddev(row.d),
						v: variance(row.d),
						min: min(row.d),
						max: max(row.d),
					}))
					.orderBy('r')
					.build(),
			),
		);
		expect(rows.map(shapes)).toStrictEqual([
			{
				r: 'string(a)',
				c: 'number(2)',
				sum: 'Decimal(4)',
				mean: 'number(2)',
				median: 'number(2)',
				sd: 'Decimal(0.7071067811865475244008443621)',
				v: 'Decimal(0.5)',
				min: 'Decimal(1.5)',
				max: 'Decimal(2.5)',
			},
			{
				r: 'string(b)',
				c: 'number(1)',
				sum: 'Decimal(4)',
				mean: 'number(4)',
				median: 'Decimal(4)',
				sd: 'number(0)',
				v: 'number(0)',
				min: 'Decimal(4)',
				max: 'Decimal(4)',
			},
			{
				r: 'string(c)',
				c: 'number(3)',
				sum: 'Decimal(9)',
				mean: 'number(3)',
				median: 'Decimal(2)',
				sd: 'Decimal(2.6457513110645905905016157536)',
				v: 'Decimal(7)',
				min: 'Decimal(1)',
				max: 'Decimal(6)',
			},
		]);
	});

	it('an int and a float field: every aggregate is a number', async () => {
		for (const pick of ['i', 'f'] as const) {
			const rows = await ok(
				client.run(
					select(MeasureTable)
						.groupBy((row) => ({ r: row.r }))
						.aggregate((row) => ({
							sum: sum(row[pick]),
							mean: mean(row[pick]),
							median: median(row[pick]),
							sd: stddev(row[pick]),
							v: variance(row[pick]),
							min: min(row[pick]),
							max: max(row[pick]),
						}))
						.orderBy('r')
						.build(),
				),
			);
			for (const row of rows) {
				const { r: _, ...aggregates } = row;
				for (const value of Object.values(aggregates)) {
					expect(typeof value).toBe('number');
				}
			}
			expect(rows.map((row) => row.sd)).toStrictEqual([
				Math.SQRT1_2,
				0,
				Math.sqrt(7),
			]);
		}
	});

	it('a number field holding decimals and floats: each aggregate decodes as what it holds', async () => {
		const rows = await ok(
			client.run(
				select(MeasureTable)
					.groupBy((row) => ({ r: row.r }))
					.aggregate((row) => ({
						sum: sum(row.n),
						mean: mean(row.n),
						median: median(row.n),
						sd: stddev(row.n),
						v: variance(row.n),
						min: min(row.n),
						max: max(row.n),
					}))
					.orderBy('r')
					.build(),
			),
		);
		expect(rows.map(shapes)).toStrictEqual([
			{
				r: 'string(a)',
				sum: 'Decimal(3.5)',
				mean: 'number(1.75)',
				median: 'number(1.75)',
				sd: 'Decimal(0.353553390593273762200422181)',
				v: 'Decimal(0.125)',
				min: 'Decimal(1.5)',
				max: 'number(2)',
			},
			{
				r: 'string(b)',
				sum: 'Decimal(4)',
				mean: 'number(4)',
				median: 'Decimal(4)',
				sd: 'number(0)',
				v: 'number(0)',
				min: 'Decimal(4)',
				max: 'Decimal(4)',
			},
			{
				r: 'string(c)',
				sum: 'number(10)',
				mean: 'number(3.3333333333333335)',
				median: 'number(2.5)',
				sd: 'number(2.3629078131263044)',
				v: 'number(5.583333333333334)',
				min: 'number(1.5)',
				max: 'number(6)',
			},
		]);
	});

	it('an optional decimal field that is NONE everywhere, and GROUP ALL over no record, aggregate as no value', async () => {
		const none = await ok(
			client.run(
				select(MeasureTable)
					.where((row) => eq(row.r, 'b'))
					.groupBy((row) => ({ r: row.r }))
					.aggregate((row) => ({
						sum: sum(row.od),
						mean: mean(row.od),
						median: median(row.od),
						sd: stddev(row.od),
						v: variance(row.od),
						min: min(row.od),
						max: max(row.od),
					}))
					.build(),
			),
		);
		expect(none.map(shapes)).toStrictEqual([
			{
				r: 'string(b)',
				sum: 'number(0)',
				mean: 'number(NaN)',
				median: 'NONE',
				sd: 'number(NaN)',
				v: 'number(NaN)',
				min: 'number(Infinity)',
				max: 'number(-Infinity)',
			},
		]);
		const empty = await ok(
			client.run(
				select(MeasureTable)
					.where((row) => eq(row.r, param('r', SurrealSchema.String)))
					.groupAll()
					.aggregate((row) => ({
						c: count(),
						dsum: sum(row.d),
						dmean: mean(row.d),
						dmedian: median(row.d),
						dsd: stddev(row.d),
						dv: variance(row.d),
						dmin: min(row.d),
						dmax: max(row.d),
						imedian: median(row.i),
						isd: stddev(row.i),
						fmedian: median(row.f),
						fv: variance(row.f),
					}))
					.build(),
				{ r: 'none' },
			),
		);
		expect(empty.map(shapes)).toStrictEqual([
			{
				c: 'number(0)',
				dsum: 'number(0)',
				dmean: 'number(NaN)',
				dmedian: 'NONE',
				dsd: 'number(NaN)',
				dv: 'number(NaN)',
				dmin: 'number(Infinity)',
				dmax: 'number(-Infinity)',
				imedian: 'NONE',
				isd: 'number(NaN)',
				fmedian: 'NONE',
				fv: 'number(NaN)',
			},
		]);
	});

	it('catalogue functions over stored fields: a decimal argument gives a decimal, a string that does not parse NONE', async () => {
		const rows = await ok(
			client.run(
				select(MeasureTable)
					.where((row) => eq(row.r, 'a'))
					.project((row) => ({
						rootD: math.sqrt(row.d),
						rootI: math.sqrt(row.i),
						rootN: math.sqrt(row.n),
						lerp: math.lerp(0, 10, row.d),
						host: parse.emailHost(row.r),
						path: parse.urlPath(row.r),
						median: math.median([]),
						sum: vector.sum([]),
					}))
					.orderBy((row) => row.i)
					.build(),
			),
		);
		expect(rows.map(shapes)).toStrictEqual([
			{
				rootD: 'Decimal(1.2247448713915890490986420374)',
				rootI: 'number(1)',
				rootN: 'Decimal(1.2247448713915890490986420374)',
				lerp: 'Decimal(15)',
				host: 'NONE',
				path: 'NONE',
				median: 'NONE',
				sum: 'NONE',
			},
			{
				rootD: 'Decimal(1.5811388300841896659994467722)',
				rootI: 'number(1.4142135623730951)',
				rootN: 'number(1.4142135623730951)',
				lerp: 'Decimal(25)',
				host: 'NONE',
				path: 'NONE',
				median: 'NONE',
				sum: 'NONE',
			},
		]);
	});
});
