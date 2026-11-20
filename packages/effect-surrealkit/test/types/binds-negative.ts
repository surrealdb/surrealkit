/**
 * Negative type fixtures for `binds`: a codec whose stored type cannot hold
 * the kind its binding is used as is a compile error naming the binding
 * and the use. Each `@ts-expect-error` must be an actual error.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { part, sequence } from '../../src/statement/index.js';

const readSnapshots = part({
	sql: 'SELECT * FROM snapshot WHERE producer = $producer AND tally < $ceiling;',
	free: ['ceiling', 'producer'],
	uses: {
		ceiling: [
			{ kind: 'field', table: 'snapshot', field: 'tally', line: 1 },
		],
		producer: [
			{ kind: 'field', table: 'snapshot', field: 'producer', line: 1 },
		],
	},
	binds: { ceiling: 'int', producer: 'string' },
});

const writeSnapshot = part({
	sql: 'UPDATE snapshot SET producer = $producer;',
	free: ['producer'],
	uses: {
		producer: [
			{
				kind: 'field',
				table: 'snapshot',
				field: 'producer',
				role: 'assign',
				line: 1,
			},
		],
	},
	binds: { producer: 'string' },
});

const score = part({
	sql: 'RETURN fn::score($weight);',
	free: ['weight'],
	uses: {
		weight: [
			{ kind: 'argument', function: 'fn::score', index: 0, line: 1 },
		],
	},
	binds: { weight: 'float' },
});

const readGadget = part({
	sql: "SELECT * FROM type::record('gadget', $key);",
	free: ['key'],
	uses: { key: [{ kind: 'key', table: 'gadget', line: 1 }] },
	binds: { key: 'uuid' },
});

const readMeter = part({
	sql: 'SELECT * FROM meter WHERE model = $model;',
	free: ['model'],
	uses: {
		model: [{ kind: 'field', table: 'meter', field: 'model', line: 1 }],
	},
	binds: { model: 'option<string>' },
});

const writeMeter = part({
	sql: 'UPDATE meter SET model = $model;',
	free: ['model'],
	uses: {
		model: [
			{
				kind: 'field',
				table: 'meter',
				field: 'model',
				role: 'assign',
				line: 1,
			},
		],
	},
	binds: { model: 'string' },
});

const keyMeter = part({
	sql: "SELECT * FROM type::record('meter', $key);",
	free: ['key'],
	uses: { key: [{ kind: 'key', table: 'meter', line: 1 }] },
	binds: { key: 'string' },
});

// `rejectsNone`: a binding assigned to a field that rejects NONE.
const assignName = {
	kind: 'field',
	table: 'person',
	field: 'name',
	role: 'assign',
	line: 1,
} as const;

const renamePerson = part({
	sql: 'UPDATE person SET name = $name;',
	free: ['name'],
	uses: { name: [assignName] },
	binds: { name: 'string' },
	rejectsNone: { name: { table: 'person', field: 'name', kind: 'string' } },
});

// Its uses resolve to different kinds, so it has no `binds` entry.
const renameByAge = part({
	sql: 'UPDATE person SET name = $name WHERE age = $name;',
	free: ['name'],
	uses: {
		name: [
			assignName,
			{
				kind: 'field',
				table: 'person',
				field: 'age',
				role: 'compare',
				line: 1,
			},
		],
	},
	rejectsNone: { name: { table: 'person', field: 'name', kind: 'string' } },
});

// Compared with an optional field and assigned to one that rejects NONE.
const renameByNick = part({
	sql: 'UPDATE person SET name = $name WHERE nick = $name;',
	free: ['name'],
	uses: {
		name: [
			assignName,
			{
				kind: 'field',
				table: 'person',
				field: 'nick',
				role: 'compare',
				line: 1,
			},
		],
	},
	binds: { name: 'string' },
	rejectsNone: { name: { table: 'person', field: 'name', kind: 'string' } },
});

const tagPerson = part({
	sql: 'UPDATE person SET tags = $tags;',
	free: ['tags'],
	uses: {
		tags: [{ ...assignName, field: 'tags' }],
	},
	rejectsNone: {
		tags: { table: 'person', field: 'tags', kind: 'array<string>' },
	},
});

export const refusedNone = () => {
	sequence(renamePerson).define({
		// @ts-expect-error [none/assigned-undefined-or] NONE assigned to a string field is rejected by the engine
		params: { name: Schema.UndefinedOr(SurrealSchema.String) },
		result: Schema.Unknown,
	});
	sequence(renamePerson).define({
		// @ts-expect-error [none/assigned-undefined-or-branded] a brand does not take NONE away
		params: {
			name: Schema.UndefinedOr(
				SurrealSchema.String.pipe(Schema.brand('app/Name')),
			),
		},
		result: Schema.Unknown,
	});
	sequence(renameByAge).define({
		// @ts-expect-error [none/assigned-without-binds] refused without a `binds` entry too
		params: { name: Schema.UndefinedOr(SurrealSchema.String) },
		result: Schema.Unknown,
	});
	sequence(renameByNick).define({
		// @ts-expect-error [none/assigned-and-compared] one assignment that rejects NONE decides it
		params: { name: Schema.UndefinedOr(SurrealSchema.String) },
		result: Schema.Unknown,
	});
	sequence(tagPerson).define({
		// @ts-expect-error [none/assigned-array] the kind need not be simple
		params: {
			tags: Schema.UndefinedOr(Schema.Array(SurrealSchema.String)),
		},
		result: Schema.Unknown,
	});
	sequence(
		part({ sql: 'SELECT * FROM person WHERE age > $age;', free: ['age'] }),
		renamePerson,
	).define({
		// @ts-expect-error [none/assigned-in-sequence] refused in a sequence of parts
		params: {
			age: SurrealSchema.Int,
			name: Schema.UndefinedOr(SurrealSchema.String),
		},
		result: Schema.Unknown,
	});
};

export const rejected = () => {
	sequence(readMeter).define({
		// @ts-expect-error [binds/option-wrong-representation] a number inside `UndefinedOr` for an optional string
		params: { model: Schema.UndefinedOr(Schema.Number) },
		result: Schema.Unknown,
	});
	sequence(writeMeter).define({
		// @ts-expect-error [binds/assigned-narrow-undefined-or] a plain kind is checked as before: `'a' | undefined` does not compare with a string
		params: { model: Schema.UndefinedOr(Schema.Literal('a')) },
		result: Schema.Unknown,
	});
	sequence(keyMeter).define({
		// @ts-expect-error [binds/key-narrow-undefined-or] a key never accepts NONE
		params: { key: Schema.UndefinedOr(Schema.Literal('a')) },
		result: Schema.Unknown,
	});
	sequence(score).define({
		// @ts-expect-error [binds/argument-narrow-undefined-or] a plain argument is checked as before
		params: { weight: Schema.UndefinedOr(Schema.Literal(1)) },
		result: Schema.Unknown,
	});
	sequence(readMeter).define({
		// @ts-expect-error [binds/option-null-or-narrow] `null` is not NONE: `NullOr` of a literal still misfits
		params: { model: Schema.NullOr(Schema.Literal('a')) },
		result: Schema.Unknown,
	});
	sequence(readSnapshots).define({
		// @ts-expect-error [binds/field-wrong-representation] a number codec for a string field
		params: {
			ceiling: SurrealSchema.Int,
			producer: SurrealSchema.Int,
		},
		result: Schema.Unknown,
	});
	sequence(readSnapshots).define({
		// @ts-expect-error [binds/branded-wrong-representation] a brand does not change the stored type
		params: {
			ceiling: SurrealSchema.String.pipe(Schema.brand('app/Ceiling')),
			producer: SurrealSchema.String,
		},
		result: Schema.Unknown,
	});
	sequence(writeSnapshot).define({
		// @ts-expect-error [binds/assigned-wrong-representation] a number codec assigned to a string field
		params: { producer: SurrealSchema.Int },
		result: Schema.Unknown,
	});
	sequence(score).define({
		// @ts-expect-error [binds/argument-wrong-representation] a string codec for a float argument
		params: { weight: SurrealSchema.String },
		result: Schema.Unknown,
	});
	sequence(readGadget).define({
		// @ts-expect-error [binds/key-wrong-representation] a string codec for a uuid key
		params: { key: SurrealSchema.String },
		result: Schema.Unknown,
	});
};
