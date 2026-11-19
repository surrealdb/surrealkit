/**
 * Tables with one field `f` under a unique index `<table>_f`, whose kinds a
 * lookup by an array treats differently on SurrealDB 3.3 and 4.0: 3.3 finds
 * an array through the index only when `f` is declared an array kind
 * (`test/conformance.test.ts` pins it). `lookupSchema` defines them.
 */
import { Schema } from 'effect';

import * as SurrealSchema from '../src/index.js';

const descriptor = <
	const Name extends string,
	const F extends Schema.Top,
	const Optional extends boolean,
>(
	name: Name,
	field: F,
	optional: Optional,
	schemafull: boolean,
) => {
	const id = SurrealSchema.RecordIdFor(name);
	const f = optional ? Schema.optionalKey(field) : field;
	const row = Schema.Struct({ id, f } as {
		readonly id: typeof id;
		readonly f: Optional extends true ? Schema.optionalKey<F> : F;
	});
	const write = Schema.Struct({ f } as {
		readonly f: Optional extends true ? Schema.optionalKey<F> : F;
	});
	const presence = optional
		? ({
				select: 'optional',
				create: 'optional',
				merge: 'removable',
				content: 'optional',
			} as const)
		: ({
				select: 'required',
				create: 'required',
				merge: 'optional',
				content: 'required',
			} as const);
	return SurrealSchema.table({
		name,
		type: 'normal',
		schemafull,
		drop: false,
		id,
		select: row,
		visible: row,
		create: write,
		merge: write,
		content: write,
		fields: { f: field },
		presence: {
			id: {
				select: 'required',
				create: 'excluded',
				merge: 'excluded',
				content: 'excluded',
			},
			f: presence,
		},
		relations: { in: [], out: [] },
		permissions: {
			select: false,
			create: false,
			update: false,
			delete: false,
		},
		indexes: {
			[`${name}_f`]: { type: 'unique', columns: [['f']] },
		} as unknown as {
			readonly [K in `${Name}_f`]: {
				readonly type: 'unique';
				readonly columns: readonly [readonly ['f']];
			};
		},
	});
};

/** A schemaless table whose `f` is `any`: an array, or anything else. */
export const LookupAnyTable = descriptor(
	'lk_any',
	SurrealSchema.SurrealValue,
	true,
	false,
);

/** `f` is `array<int> | string`. */
export const LookupUnionTable = descriptor(
	'lk_union',
	Schema.Union([Schema.Array(SurrealSchema.Int), SurrealSchema.String]),
	false,
	true,
);

/** `f` is `array<int> | set<string>`: a set is not an array here. */
export const LookupSetTable = descriptor(
	'lk_set',
	Schema.Union([
		Schema.Array(SurrealSchema.Int),
		SurrealSchema.SetOf(SurrealSchema.String),
	]),
	false,
	true,
);

/** A schemaless table whose `f` is `option<array<int>>`: only an array. */
export const LookupArrayTable = descriptor(
	'lk_array',
	Schema.Array(SurrealSchema.Int),
	true,
	false,
);

/** The four tables, a row each holding `[1, 2]`, and a row holding `'x'`. */
export const lookupSchema = `
	DEFINE TABLE lk_any SCHEMALESS;
	DEFINE FIELD f ON lk_any TYPE any;
	DEFINE INDEX lk_any_f ON lk_any FIELDS f UNIQUE;
	CREATE lk_any:1 CONTENT { f: [1, 2] };
	CREATE lk_any:2 CONTENT { f: 'x' };
	DEFINE TABLE lk_union SCHEMAFULL;
	DEFINE FIELD f ON lk_union TYPE array<int> | string;
	DEFINE INDEX lk_union_f ON lk_union FIELDS f UNIQUE;
	CREATE lk_union:1 CONTENT { f: [1, 2] };
	CREATE lk_union:2 CONTENT { f: 'x' };
	DEFINE TABLE lk_set SCHEMAFULL;
	DEFINE FIELD f ON lk_set TYPE array<int> | set<string>;
	DEFINE INDEX lk_set_f ON lk_set FIELDS f UNIQUE;
	CREATE lk_set:1 CONTENT { f: [1, 2] };
	CREATE lk_set:2 CONTENT { f: <set<string>>['x'] };
	DEFINE TABLE lk_array SCHEMALESS;
	DEFINE FIELD f ON lk_array TYPE option<array<int>>;
	DEFINE INDEX lk_array_f ON lk_array FIELDS f UNIQUE;
	CREATE lk_array:1 CONTENT { f: [1, 2] };
`;
