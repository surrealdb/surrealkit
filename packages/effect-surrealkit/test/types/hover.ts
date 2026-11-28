/**
 * What an editor shows on hover for the input and result of statements:
 * `test/hover.test.ts` asks the language service for the quick info at each
 * `/*hover:<name>*\/` marker (the identifier after it) and for the resolved
 * input parameter of the call after each `/*input:<name>*\/` marker, and
 * pins the printed text. Compiled by `tsc -p test/types` too.
 */
import { Effect } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { run } from '../../src/client/index.js';
import {
	create,
	deleteFrom,
	eq,
	gte,
	type Input,
	param,
	type Result,
	relate,
	select,
	update,
} from '../../src/statement/index.js';
import {
	MemberOfTable,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonTable,
	TeamId,
} from '../golden/example/index.js';

export const adults = select(PersonTable)
	.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
	.where((row) => eq(row.nickname, param('nickname', SurrealSchema.String)))
	.where((row) => eq(row.address.city, param('city', SurrealSchema.String)))
	.project((row) => ({ id: row.id, name: row.name, nickname: row.nickname }))
	.build();
export type /*hover:select-input*/ AdultsInput = Input<
	(typeof adults)['params']
>;
export type /*hover:select-project-result*/ AdultsResult = Result<
	typeof adults
>;
export const /*hover:select-params*/ adultsParams = adults.params;

export const program = Effect.gen(function* () {
	const /*hover:run-result*/ rows = yield* /*input:run-input*/ run(adults, {
			minimum: 18,
			nickname: 'Al',
			city: 'Leeds',
		});
	return rows;
});

export const created = create(PersonTable)
	.content(param('person', PersonCreate))
	.returning((row) => ({ id: row.id, name: row.name }))
	.build();
export type /*hover:create-result*/ Created = Result<typeof created>;

export const merged = update(PersonTable, param('id', PersonId))
	.merge(param('changes', PersonMerge))
	.returning((row) => ({ id: row.id, email: row.email }))
	.build();
export type /*hover:merge-input*/ MergedInput = Input<
	(typeof merged)['params']
>;
export type /*hover:merge-result*/ Merged = Result<typeof merged>;

export const renamed = update(PersonTable)
	.merge(param('changes', PersonMerge))
	.where((row) => eq(row.role, 'member'))
	.returning((row) => ({ name: row.name }))
	.build();
export type /*hover:update-many-result*/ Renamed = Result<typeof renamed>;

export const removed = deleteFrom(PersonTable, param('id', PersonId))
	.returning('before')
	.build();
export const /*hover:delete-params*/ removedParams = removed.params;

export const joined = relate(
	MemberOfTable,
	param('person', PersonId),
	param('team', TeamId),
).build();
export type /*hover:relate-result*/ Joined = Result<typeof joined>;
export const /*hover:relate-params*/ joinedParams = joined.params;
