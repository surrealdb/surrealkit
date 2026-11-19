/**
 * Writes the type-check budget fixture: `count` statements over the golden
 * example modules, in a fixed rotation of six kinds, each with names of
 * its own so no two statements share an instantiation by accident:
 *
 * 0. a `select` with two conditions on bindings and a three-field projection,
 *    bound to its input;
 * 1. a `select` by id with `ONLY`, a sort key and `SELECT VALUE`;
 * 2. a `create` with content and a projected `RETURN`;
 * 3. an `update` by id with a narrow `MERGE`, and an `update` by condition;
 * 4. a `delete` by condition returning the rows, and a `relate`;
 * 5. a `sequence` of three parts with several free and defined names, and
 *    its contract, bound to its input.
 *
 * The text is a pure function of `count`, so the compiler's counters for it
 * are deterministic.
 */
const header = `import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { bind } from '../../src/client/index.js';
import {
	and,
	at,
	create,
	deleteFrom,
	eq,
	gte,
	isNone,
	or,
	param,
	part,
	relate,
	select,
	sequence,
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

export const unused = [and, at, create, deleteFrom, eq, gte, isNone, or, param, part, relate, select, sequence, update, bind, Schema, SurrealSchema, MemberOfTable, PersonCreate, PersonId, PersonMerge, PersonTable, TeamId] as const;
`;

const statement = (i: number): string => {
	switch (i % 6) {
		case 0:
			return `export const s${i} = select(PersonTable)
	.where((row) => gte(row.age, param('min${i}', SurrealSchema.Int)))
	.where((row) => or(eq(row.role, 'admin'), eq(row.address.city, param('city${i}', SurrealSchema.String))))
	.project((row) => ({ id: row.id, name: row.name, nickname: row.nickname }))
	.build('s${i}');
export const b${i} = bind(s${i}, { min${i}: ${i}, city${i}: 'c${i}' });
`;
		case 1:
			return `export const s${i} = select(PersonTable, param('id${i}', PersonId))
	.where((row) => and(isNone(row.nickname), eq(row.email, param('email${i}', SurrealSchema.String))))
	.orderBy((row) => at(row.phones, 0))
	.value((row) => row.name)
	.only()
	.build('s${i}');
`;
		case 2:
			return `export const s${i} = create(PersonTable)
	.content(param('person${i}', PersonCreate))
	.returning((row) => ({ id: row.id, email: row.email }))
	.build('s${i}');
`;
		case 3:
			return `export const s${i} = update(PersonTable, param('id${i}', PersonId))
	.merge(param('changes${i}', Schema.Struct({ name: PersonMerge.fields.name, bio: PersonMerge.fields.bio })))
	.build('s${i}');
export const t${i} = update(PersonTable)
	.merge(param('all${i}', PersonMerge))
	.where((row) => eq(row.role, param('role${i}', PersonTable.select.fields.role)))
	.returning('none')
	.build('t${i}');
`;
		case 4:
			return `export const s${i} = deleteFrom(PersonTable)
	.where((row) => eq(row.email, param('gone${i}', SurrealSchema.String)))
	.returning('before')
	.build('s${i}');
export const t${i} = relate(MemberOfTable, param('from${i}', PersonId), param('to${i}', TeamId))
	.content(param('edge${i}', MemberOfTable.create))
	.build('t${i}');
`;
		default:
			return `const bind${i} = part({
	sql: 'LET $owner${i} = $boundOwner${i}; LET $since${i} = $boundSince${i};',
	free: ['boundOwner${i}', 'boundSince${i}'],
	defines: ['owner${i}', 'since${i}'],
	slots: 2,
});
const guard${i} = part({
	sql: "IF $owner${i} != $principal${i} { THROW 'Not the owner ${i}' };",
	free: ['owner${i}', 'principal${i}'],
	throws: ['Not the owner ${i}'],
});
const read${i} = part({
	sql: 'SELECT VALUE name FROM ONLY $owner${i} WHERE created_at > $since${i} AND role = $role${i};',
	free: ['owner${i}', 'since${i}', 'role${i}'],
});
export const s${i} = sequence(bind${i}, guard${i}, read${i}).define({
	name: 's${i}',
	params: {
		boundOwner${i}: PersonId,
		boundSince${i}: SurrealSchema.DateTime,
		principal${i}: PersonId,
		role${i}: SurrealSchema.String,
	},
	result: Schema.UndefinedOr(SurrealSchema.String),
});
`;
	}
};

/** The fixture's text: the imports, then `count` statements. */
export const budgetFixture = (count: number): string =>
	[header, ...Array.from({ length: count }, (_, i) => statement(i))].join(
		'\n',
	);
