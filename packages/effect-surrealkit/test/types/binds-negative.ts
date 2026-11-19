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

export const rejected = () => {
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
