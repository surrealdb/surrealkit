/**
 * Type fixtures for `binds`: a part that states the kind each binding's
 * uses resolve to accepts, in `sequence(...).define`, every codec whose
 * stored type holds that kind, plain, branded or refined, exactly as a
 * comparison accepts it. Compiled by `tsc -p test/types`.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { part, sequence } from '../../src/statement/index.js';

const readSnapshots = part({
	sql: 'SELECT * FROM snapshot WHERE producer = $producer AND day >= $since AND tally < $ceiling;',
	free: ['ceiling', 'producer', 'since'],
	uses: {
		ceiling: [
			{ kind: 'field', table: 'snapshot', field: 'tally', line: 1 },
		],
		producer: [
			{ kind: 'field', table: 'snapshot', field: 'producer', line: 1 },
		],
		since: [{ kind: 'field', table: 'snapshot', field: 'day', line: 1 }],
	},
	binds: { ceiling: 'int', producer: 'string', since: 'datetime' },
});

const readOwner = part({
	sql: "SELECT * FROM type::record('gadget', $key) WHERE owner = $owner;",
	free: ['key', 'owner'],
	binds: { owner: 'record<person>' },
});

export const plain = sequence(readSnapshots).define({
	params: {
		ceiling: SurrealSchema.Int,
		producer: SurrealSchema.String,
		since: SurrealSchema.DateTime,
	},
	result: Schema.Unknown,
});

export const branded = sequence(readSnapshots).define({
	params: {
		ceiling: SurrealSchema.Int64,
		producer: SurrealSchema.String.pipe(Schema.brand('app/Producer')),
		since: SurrealSchema.DateTime,
	},
	result: Schema.Unknown,
});

export const refined = sequence(readSnapshots).define({
	params: {
		ceiling: SurrealSchema.Int.check(Schema.isGreaterThan(0)),
		producer: SurrealSchema.String.check(Schema.isMinLength(1)),
		since: SurrealSchema.DateTime,
	},
	result: Schema.Unknown,
});

export const records = sequence(readOwner).define({
	params: {
		key: SurrealSchema.String,
		owner: SurrealSchema.RecordIdFor('person'),
	},
	result: Schema.Unknown,
});

export const anyRecord = sequence(readOwner).define({
	params: { key: SurrealSchema.Int, owner: SurrealSchema.RecordIdAny },
	result: Schema.Unknown,
});

// A name an earlier part defines is not a binding, so its kind is not checked.
const letProducer = part({
	sql: 'LET $producer = 1;',
	defines: ['producer'],
});
export const shadowed = sequence(letProducer, readSnapshots).define({
	params: {
		ceiling: SurrealSchema.Int,
		since: SurrealSchema.DateTime,
	},
	result: Schema.Unknown,
});

// A binding every use of which accepts NONE is bound as `option<K>`: its
// codec fits when what it holds besides `undefined` compares with `K`.
const readMeter = part({
	sql: 'SELECT * FROM meter WHERE model = $model;',
	free: ['model'],
	uses: {
		model: [{ kind: 'field', table: 'meter', field: 'model', line: 1 }],
	},
	binds: { model: 'option<string>' },
});
export const optionalLiteral = sequence(readMeter).define({
	params: { model: Schema.UndefinedOr(Schema.Literal('a')) },
	result: Schema.Unknown,
});
export const optionalBranded = sequence(readMeter).define({
	params: {
		model: Schema.UndefinedOr(
			SurrealSchema.String.pipe(Schema.brand('app/Model')),
		),
	},
	result: Schema.Unknown,
});
export const optionalRefined = sequence(readMeter).define({
	params: {
		model: Schema.UndefinedOr(
			SurrealSchema.String.check(Schema.isMinLength(1)),
		),
	},
	result: Schema.Unknown,
});
export const optionalPlain = sequence(readMeter).define({
	params: { model: SurrealSchema.String },
	result: Schema.Unknown,
});
// `null` is not NONE: `NullOr` fits as it does on a plain kind.
export const optionalNullOr = sequence(readMeter).define({
	params: { model: Schema.NullOr(Schema.String) },
	result: Schema.Unknown,
});

const passOptional = part({
	sql: 'RETURN fn::tag($label);',
	free: ['label'],
	uses: {
		label: [{ kind: 'argument', function: 'fn::tag', index: 0, line: 1 }],
	},
	binds: { label: 'option<string>' },
});
export const optionalArgument = sequence(passOptional).define({
	params: { label: Schema.UndefinedOr(Schema.Literals(['a', 'b'])) },
	result: Schema.Unknown,
});

// A plain kind is checked as before: a codec that can hold NONE and is
// comparable as a whole still fits.
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
export const plainUndefinedOr = sequence(writeMeter).define({
	params: { model: Schema.UndefinedOr(Schema.String) },
	result: Schema.Unknown,
});
export const plainNullOr = sequence(writeMeter).define({
	params: { model: Schema.NullOr(Schema.String) },
	result: Schema.Unknown,
});

// `rejectsNone` refuses only a codec that can hold NONE for a binding
// assigned to a field that rejects it.
const renamePerson = part({
	sql: 'UPDATE person SET name = $name;',
	free: ['name'],
	uses: {
		name: [
			{
				kind: 'field',
				table: 'person',
				field: 'name',
				role: 'assign',
				line: 1,
			},
		],
	},
	binds: { name: 'string' },
	rejectsNone: { name: { table: 'person', field: 'name', kind: 'string' } },
});
export const assignedPlain = sequence(renamePerson).define({
	params: { name: SurrealSchema.String },
	result: Schema.Unknown,
});
export const assignedLiteral = sequence(renamePerson).define({
	params: { name: Schema.Literal('a') },
	result: Schema.Unknown,
});
export const assignedBranded = sequence(renamePerson).define({
	params: { name: SurrealSchema.String.pipe(Schema.brand('app/Name')) },
	result: Schema.Unknown,
});
// `unknown` holds everything, so it is not taken to hold NONE.
export const assignedUnknown = sequence(renamePerson).define({
	params: { name: Schema.Unknown },
	result: Schema.Unknown,
});
// A part that defines the name binds it itself: no codec, nothing refused.
export const assignedDefined = sequence(
	part({ sql: "LET $name = 'a';", defines: ['name'] }),
	renamePerson,
).define({
	params: {},
	result: Schema.Unknown,
});

// Compared, never assigned, with a field that rejects NONE: NONE compares
// (`WHERE $name = NONE OR name = $name`), so nothing is refused.
const findPerson = part({
	sql: 'SELECT * FROM person WHERE $name = NONE OR name = $name;',
	free: ['name'],
	uses: {
		name: [
			{
				kind: 'field',
				table: 'person',
				field: 'name',
				role: 'compare',
				line: 1,
			},
		],
	},
	binds: { name: 'option<string>' },
});
export const comparedUndefinedOr = sequence(findPerson).define({
	params: { name: Schema.UndefinedOr(SurrealSchema.String) },
	result: Schema.Unknown,
});

// Assigned to a field that accepts NONE (`option<string>`, or one with a
// DEFAULT): the generator writes no `rejectsNone` entry for it.
const renameNick = part({
	sql: 'UPDATE person SET nick = $nick;',
	free: ['nick'],
	uses: {
		nick: [
			{
				kind: 'field',
				table: 'person',
				field: 'nick',
				role: 'assign',
				line: 1,
			},
		],
	},
	binds: { nick: 'option<string>' },
});
export const assignedOptional = sequence(renameNick).define({
	params: { nick: Schema.UndefinedOr(SurrealSchema.String) },
	result: Schema.Unknown,
});
const retitlePerson = part({
	sql: 'UPDATE person SET title = $title;',
	free: ['title'],
	uses: {
		title: [
			{
				kind: 'field',
				table: 'person',
				field: 'title',
				role: 'assign',
				line: 1,
			},
		],
	},
	binds: { title: 'string' },
});
export const assignedDefaulted = sequence(retitlePerson).define({
	params: { title: Schema.UndefinedOr(SurrealSchema.String) },
	result: Schema.Unknown,
});
