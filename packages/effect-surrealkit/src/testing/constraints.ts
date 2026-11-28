/**
 * Whether the client still reads an engine's constraint errors: a check to
 * run against a new SurrealDB version before relying on
 * `StatementIndexConflict` and `StatementRecordExists` with it.
 */
import { Effect, Schema } from 'effect';

import { run, type SurrealClient } from '../client/client.js';
import type { StatementError } from '../client/errors.js';
import { make } from '../statement/statement.js';

const table = 'effect_surrealkit_constraint_check';
const index = `${table}_k`;

const write = (name: string, sql: string, slots = 1) =>
	make({
		name,
		sql,
		params: {},
		result: Schema.Unknown,
		slots,
		writes: true,
		uniqueIndexes: [index],
	});

const setup = write(
	'constraint check setup',
	`REMOVE TABLE IF EXISTS ${table}; DEFINE TABLE ${table} SCHEMALESS; DEFINE INDEX ${index} ON ${table} FIELDS k UNIQUE; CREATE ${table}:one SET k = 1 RETURN NONE;`,
	4,
);
const duplicate = write(
	'constraint check duplicate',
	`CREATE ${table}:two SET k = 1 RETURN NONE;`,
);
const existing = write(
	'constraint check existing',
	`CREATE ${table}:one SET k = 2 RETURN NONE;`,
);
const cleanup = write(
	'constraint check cleanup',
	`REMOVE TABLE IF EXISTS ${table};`,
);

/** The tag a write failed with, or `success` when it did not fail. */
const outcome = (statement: typeof duplicate) =>
	run(statement).pipe(
		Effect.match({
			onFailure: (error) => error._tag,
			onSuccess: () => 'success',
		}),
	);

/**
 * Whether the client still classifies the engine's constraint errors: it
 * defines the table `effect_surrealkit_constraint_check` with a unique
 * index in the client's database, writes a duplicate value and an id that
 * exists, and removes the table. `indexConflict` and `recordExists` are
 * true when they fail as `StatementIndexConflict` and
 * `StatementRecordExists`; `got` holds what each failed with otherwise
 * (`StatementRequestError` on an engine whose message the client does not
 * read). Run it against a disposable database, such as `memoryLayer`.
 */
export const checkConstraintErrors: Effect.Effect<
	{
		readonly indexConflict: boolean;
		readonly recordExists: boolean;
		readonly got: {
			readonly duplicate: string;
			readonly existing: string;
		};
	},
	StatementError<never, string>,
	SurrealClient
> = Effect.gen(function* () {
	yield* run(setup);
	const got = {
		duplicate: yield* outcome(duplicate),
		existing: yield* outcome(existing),
	};
	return {
		indexConflict: got.duplicate === 'StatementIndexConflict',
		recordExists: got.existing === 'StatementRecordExists',
		got,
	};
}).pipe(Effect.ensuring(Effect.ignore(run(cleanup))));
