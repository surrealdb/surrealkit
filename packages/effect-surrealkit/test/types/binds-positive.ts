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
