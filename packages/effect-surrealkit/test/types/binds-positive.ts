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
