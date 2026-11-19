/**
 * The statement layer without an engine: the SurrealQL each builder renders,
 * the bindings it collects, and what is refused when a statement is defined.
 */
import { Exit, Schema } from 'effect';
import { DateTime, Duration, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	and,
	arrayGroup,
	at,
	call,
	cast,
	codecOf,
	contains,
	count,
	create,
	deleteFrom,
	eq,
	gt,
	gte,
	type IdRange,
	idRange,
	insert,
	insertRelation,
	inside,
	isNone,
	isNotNone,
	isStatement,
	literal,
	lt,
	lte,
	make,
	max,
	mean,
	median,
	type NoParams,
	ne,
	not,
	or,
	type Part,
	param,
	paramsOf,
	part,
	readAs,
	recordId,
	relate,
	rowOf,
	select,
	sequence,
	sqlOf,
	sum,
	timeMin,
	unsafeExpr,
	update,
	upsert,
} from '../src/statement/index.js';
import * as Corpus from './golden/corpus/index.js';
import {
	fn_greet,
	MemberOfTable,
	Person,
	PersonContent,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonTable,
	TeamId,
} from './golden/example/index.js';
import * as Members from './golden/members/index.js';

const names = (statement: { readonly params: object }) =>
	Object.keys(statement.params).sort();

describe('rows', () => {
	const row = rowOf(Person);

	it('have one expression per field, quoted where SurrealQL needs it', () => {
		expect(sqlOf(row.name)).toBe('name');
		expect(sqlOf(row['odd.name'])).toBe('⟨odd.name⟩');
		expect(sqlOf(row.address.city)).toBe('address.city');
		expect(Object.keys(row).sort()).toEqual(
			Object.keys(Person.fields).sort(),
		);
	});

	it('carry the field codec, and NONE for an optional field', () => {
		expect(codecOf(row.name)).toBe(Person.fields.name);
		const decode = Schema.decodeUnknownExit(codecOf(row.nickname));
		expect(Exit.isSuccess(decode(undefined))).toBe(true);
		expect(Exit.isSuccess(decode('an'))).toBe(true);
		expect(Exit.isSuccess(decode(1))).toBe(false);
		expect(
			Exit.isSuccess(
				Schema.decodeUnknownExit(codecOf(row.name))(undefined),
			),
		).toBe(false);
	});

	it('read no bindings', () => {
		expect(paramsOf(row.address.city)).toEqual({});
	});
});

describe('expressions', () => {
	const row = rowOf(Person);

	it('inline constants as the SDK spells them', () => {
		expect(sqlOf(literal('it\'s "x"'))).toBe('s"it\'s \\"x\\""');
		expect(sqlOf(literal(1.5))).toBe('1.5');
		expect(sqlOf(literal(true))).toBe('true');
		expect(sqlOf(literal(null))).toBe('NULL');
		expect(sqlOf(literal(undefined))).toBe('NONE');
		expect(
			sqlOf(
				literal(
					new DateTime('2026-01-02T03:04:05.123456789Z'),
					SurrealSchema.DateTime,
				),
			),
		).toBe('d"2026-01-02T03:04:05.123456789Z"');
		expect(
			sqlOf(literal(new Duration('1h30m'), SurrealSchema.Duration)),
		).toBe('1h30m');
		expect(
			sqlOf(
				literal(
					Schema.decodeUnknownSync(PersonId)(
						new RecordId('person', 'ann'),
					),
					PersonId,
				),
			),
		).toBe('r"person:ann"');
	});

	it('write a datetime constant with the package formatter', () => {
		// Finding 6: the SDK printed this one second late.
		const instant = SurrealSchema.dateTimeFromString(
			'1969-12-31T23:59:59.999999999Z',
		);
		expect(sqlOf(literal(instant, SurrealSchema.DateTime))).toBe(
			'd"1969-12-31T23:59:59.999999999Z"',
		);
		expect(
			sqlOf(
				literal(
					{ at: [instant] },
					Schema.Struct({ at: Schema.Array(SurrealSchema.DateTime) }),
				),
			),
		).toBe('{ "at": [ d"1969-12-31T23:59:59.999999999Z" ] }');
		// A record id spells its key with the SDK's formatter: refused.
		expect(() =>
			literal(new RecordId('t', [instant]), Schema.Unknown),
		).toThrow(/datetime.*bind it with `param`/);
	});

	it('validate every constant through its codec', () => {
		const row = rowOf(Person);
		// Finding 8: list items skipped the well-formed string check.
		expect(() => inside(row.name, ['ok', '\ud800'])).toThrow(
			/lone UTF-16 surrogate/,
		);
		expect(sqlOf(inside(row.age, [1, 2]))).toBe('age INSIDE [ 1, 2 ]');
		// An integer the engine cannot parse as an int.
		expect(() => literal(12345678901234567890n)).toThrow(
			/outside the SurrealDB int range/,
		);
		expect(() => literal(2n ** 63n)).toThrow(/outside the SurrealDB int/);
		expect(sqlOf(literal(-(2n ** 63n)))).toBe('-9223372036854775808');
		expect(() =>
			literal([2n ** 64n], Schema.Array(Schema.Unknown)),
		).toThrow(/outside the SurrealDB int range/);
		expect(() => literal({ a: '\udc00' }, Schema.Unknown)).toThrow(
			/lone UTF-16 surrogate/,
		);
		// Bytes have no literal the SDK can print: it wrote `5`.
		expect(() => literal(new Uint8Array([5]), SurrealSchema.Bytes)).toThrow(
			/no SurrealQL literal/,
		);
		const bare = Object.assign(Object.create(null), { a: 1 });
		expect(sqlOf(literal(bare, Schema.Unknown))).toBe('{ "a": 1 }');
	});

	it('refuse constants that have no faithful literal', () => {
		expect(() => literal(Number.NaN)).toThrow(/no portable SurrealQL/);
		expect(() =>
			(literal as (value: unknown) => unknown)(new RecordId('a', 'b')),
		).toThrow(/without a codec/);
		expect(() => literal('\ud800')).toThrow();
		expect(() =>
			literal(new Set(['a']), SurrealSchema.SetOf(SurrealSchema.String)),
		).toThrow(/Set or Map/);
	});

	it('render comparisons, parenthesising compound operands', () => {
		const minimum = param('minimum', SurrealSchema.Int);
		expect(sqlOf(eq(row.name, 'Ann'))).toBe('name = s"Ann"');
		expect(sqlOf(ne(row.role, 'admin'))).toBe('role != s"admin"');
		expect(sqlOf(gt(row.age, 1))).toBe('age > 1');
		expect(sqlOf(gte(row.age, minimum))).toBe('age >= $minimum');
		expect(sqlOf(lt(row.score, 2.5))).toBe('score < 2.5');
		expect(sqlOf(lte(row.score, 3))).toBe('score <= 3');
		expect(sqlOf(eq(row.bio, null))).toBe('bio = NULL');
		expect(sqlOf(isNone(row.manager))).toBe('manager = NONE');
		expect(sqlOf(isNotNone(row.manager))).toBe('manager != NONE');
		expect(
			sqlOf(
				and(
					or(eq(row.role, 'admin'), eq(row.role, 'owner')),
					not(isNone(row.manager)),
				),
			),
		).toBe('(role = s"admin" OR role = s"owner") AND !(manager = NONE)');
		expect(sqlOf(eq(eq(row.name, 'a'), true))).toBe('(name = s"a") = true');
	});

	it('render membership, casts, elements and function calls', () => {
		expect(sqlOf(inside(row.role, ['admin', 'member']))).toBe(
			'role INSIDE [ s"admin", s"member" ]',
		);
		expect(sqlOf(contains(row.tags, 'vip'))).toBe('tags CONTAINS s"vip"');
		expect(sqlOf(cast('string', row.id))).toBe('<string>id');
		expect(sqlOf(cast('bool', eq(row.name, 'a')))).toBe(
			'<bool>(name = s"a")',
		);
		expect(sqlOf(at(row.phones, 0))).toBe('phones[0]');
		expect(() => at(row.phones, -1)).toThrow(/not an array index/);
		expect(sqlOf(call(fn_greet, row.name))).toBe('fn::greet(name)');
		expect(
			sqlOf(
				call(
					fn_greet,
					row.name,
					param('greeting', SurrealSchema.String),
					2,
				),
			),
		).toBe('fn::greet(name, $greeting, 2)');
		expect(codecOf(call(fn_greet, row.name))).toBe(fn_greet.returns);
	});

	it('collect the bindings of every operand', () => {
		const condition = and(
			gte(row.age, param('minimum', SurrealSchema.Int)),
			eq(
				call(
					fn_greet,
					row.name,
					param('greeting', SurrealSchema.String),
				),
				param('expected', SurrealSchema.String),
			),
		);
		expect(Object.keys(paramsOf(condition)).sort()).toEqual([
			'expected',
			'greeting',
			'minimum',
		]);
	});

	it('refuse one binding declared with two codecs', () => {
		expect(() =>
			and(
				eq(row.name, param('needle', SurrealSchema.String)),
				gte(row.age, param('needle', SurrealSchema.Int)),
			),
		).toThrow(/\$needle.*two different codecs/);
		const shared = param('needle', SurrealSchema.String);
		expect(() =>
			and(eq(row.name, shared), eq(row.email, shared)),
		).not.toThrow();
	});

	it('refuse names a request cannot bind as written', () => {
		expect(() => param('auth', SurrealSchema.String)).toThrow(
			/set by SurrealDB/,
		);
		expect(() => param('this', SurrealSchema.String)).toThrow(
			/can be shadowed/,
		);
		expect(() => param('1st', SurrealSchema.String)).toThrow(
			/not a parameter name/,
		);
		expect(() => param('a-b', SurrealSchema.String)).toThrow(
			/not a parameter name/,
		);
	});

	it('take raw SurrealQL only through the named escape hatch', () => {
		const score = unsafeExpr({
			sql: 'search::score(1)',
			codec: Schema.Number,
			atomic: true,
		});
		expect(sqlOf(gt(score, 0))).toBe('search::score(1) > 0');
		const sum = unsafeExpr({
			sql: 'age + $years',
			codec: SurrealSchema.Int,
			params: { years: SurrealSchema.Int },
		});
		expect(sqlOf(gt(sum, 40))).toBe('(age + $years) > 40');
		expect(Object.keys(paramsOf(sum))).toEqual(['years']);
	});
});

describe('select', () => {
	it('reads the whole table by default', () => {
		const statement = select(PersonTable).build();
		expect(isStatement(statement)).toBe(true);
		expect(statement.sql).toBe('SELECT * FROM ⟨person⟩;');
		expect(statement.name).toBe('select person');
		expect(statement.slots).toBe(1);
		expect(names(statement)).toEqual([]);
	});

	it('rejects computed sort keys and aliases that hide a stored sort field', () => {
		expect(() =>
			// @ts-expect-error a parameter is not a field path; this checks the run-time guard
			select(PersonTable).orderBy(() => param('sort', Schema.String)),
		).toThrow(/field path/);
		expect(() =>
			select(PersonTable)
				.orderBy((r) => r.name)
				.project((r) => ({ name: r.role }))
				.build(),
		).toThrow(/shadows/);
		expect(() =>
			select(PersonTable)
				.project((r) => ({ name: r.role }))
				.orderBy((r) => r.name)
				.build(),
		).toThrow(/shadows/);
	});

	it('renders every clause in SurrealQL order', () => {
		const statement = select(PersonTable)
			.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
			.where((row) =>
				or(eq(row.role, 'admin'), eq(row.address.city, 'Leeds')),
			)
			.orderBy((row) => row.name)
			.orderBy((row) => row.created_at, 'DESC')
			.limit(param('rows', SurrealSchema.Int))
			.start(10)
			.build('adults');
		expect(statement.sql).toBe(
			'SELECT * FROM ⟨person⟩ WHERE age >= $minimum AND (role = s"admin" OR address.city = s"Leeds") ORDER BY name ASC, created_at DESC LIMIT $rows START 10;',
		);
		expect(statement.name).toBe('adults');
		expect(names(statement)).toEqual(['minimum', 'rows']);
	});

	it('projects named expressions, which may read bindings', () => {
		const statement = select(PersonTable)
			.project((row) => ({
				name: row.name,
				city: row.address.city,
				'odd alias': row['odd.name'],
				greeting: call(
					fn_greet,
					row.name,
					param('greeting', SurrealSchema.String),
				),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT name AS name, address.city AS city, ⟨odd.name⟩ AS ⟨odd alias⟩, fn::greet(name, $greeting) AS greeting FROM ⟨person⟩;',
		);
		expect(names(statement)).toEqual(['greeting']);
		expect(() => select(PersonTable).project(() => ({}))).toThrow(
			/at least one field/,
		);
	});

	it('decodes a projected NONE whether the key is missing or empty', () => {
		const statement = select(PersonTable)
			.project((row) => ({ name: row.name, nickname: row.nickname }))
			.build();
		const decode = Schema.decodeUnknownSync(statement.result);
		expect(
			decode([{ name: 'Ann', nickname: undefined }, { name: 'Bob' }], {
				onExcessProperty: 'error',
			}),
		).toEqual([
			{ name: 'Ann', nickname: undefined },
			{ name: 'Bob', nickname: undefined },
		]);
		expect(() =>
			decode([{ name: 'Ann', extra: 1 }], { onExcessProperty: 'error' }),
		).toThrow();
	});

	it('selects one value per row', () => {
		const statement = select(PersonTable)
			.value((row) => cast('string', row.id))
			.build();
		expect(statement.sql).toBe('SELECT VALUE <string>id FROM ⟨person⟩;');
	});

	it('reads at most one row with ONLY', () => {
		expect(
			select(PersonTable)
				.where((row) =>
					eq(row.email, param('email', SurrealSchema.String)),
				)
				.only()
				.build().sql,
		).toBe('SELECT * FROM ONLY ⟨person⟩ WHERE email = $email LIMIT 1;');
		expect(() => select(PersonTable).limit(2).only()).toThrow(
			/at most one row/,
		);
		expect(() => select(PersonTable).only().limit(2)).toThrow(
			/at most one row/,
		);
		expect(() => select(PersonTable).limit(-1)).toThrow(
			/non-negative integer/,
		);
	});

	it('reads a record by id', () => {
		const one = select(PersonTable, param('id', PersonId)).only().build();
		expect(one.sql).toBe('SELECT * FROM ONLY $id;');
		expect(names(one)).toEqual(['id']);
		expect(select(PersonTable, param('id', PersonId)).build().sql).toBe(
			'SELECT * FROM $id;',
		);
	});

	it('nests as a subquery with its own bindings', () => {
		const statement = select(PersonTable)
			.project((row) => ({
				name: row.name,
				peers: select(PersonTable)
					.where((peer) =>
						eq(peer.role, param('role', SurrealSchema.String)),
					)
					.value((peer) => peer.name)
					.toExpr(),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT name AS name, (SELECT VALUE name FROM ⟨person⟩ WHERE role = $role) AS peers FROM ⟨person⟩;',
		);
		expect(names(statement)).toEqual(['role']);
	});

	it('is immutable: a base query can be shared', () => {
		const base = select(PersonTable).where((row) => eq(row.role, 'admin'));
		const first = base.limit(1).build().sql;
		const second = base.orderBy((row) => row.name).build().sql;
		expect(first).toBe(
			'SELECT * FROM ⟨person⟩ WHERE role = s"admin" LIMIT 1;',
		);
		expect(second).toBe(
			'SELECT * FROM ⟨person⟩ WHERE role = s"admin" ORDER BY name ASC;',
		);
		expect(base.build().sql).toBe(
			'SELECT * FROM ⟨person⟩ WHERE role = s"admin";',
		);
	});

	it('orders by the stored field, refusing an alias that would hide it', () => {
		// Finding 3: SurrealDB resolves ORDER BY against the projection
		// first, so `ORDER BY age` sorted by an alias `age` of `name`.
		expect(() =>
			select(PersonTable)
				.orderBy((row) => row.age)
				.project((row) => ({ age: row.name }))
				.build(),
		).toThrow(/alias `age`.*`name`.*ORDER BY age/);
		expect(() =>
			select(PersonTable)
				.project((row) => ({ address: row.name }))
				.orderBy((row) => row.address.city)
				.toExpr(),
		).toThrow(/alias `address`/);
		expect(() =>
			select(PersonTable)
				.orderBy((row) => row.phones)
				.project((row) => ({ phones: at(row.phones, 0) }))
				.build(),
		).toThrow(/alias `phones`/);
		// An alias that is the ordered field itself keeps working.
		expect(
			select(PersonTable)
				.orderBy((row) => row.address.city)
				.orderBy((row) => row.age, 'DESC')
				.project((row) => ({ age: row.age, address: row.address }))
				.build().sql,
		).toBe(
			'SELECT age AS age, address AS address FROM ⟨person⟩ ORDER BY address.city ASC, age DESC;',
		);
		expect(
			select(PersonTable)
				.orderBy((row) => at(row.phones, 0))
				.project((row) => ({ city: row.address.city }))
				.build().sql,
		).toBe(
			'SELECT address.city AS city FROM ⟨person⟩ ORDER BY phones[0] ASC;',
		);
	});

	it('orders only by field paths', () => {
		// Finding 4: the engine parses nothing else after ORDER BY. The
		// types refuse it (test/types); untyped callers are refused here.
		const untyped = select(PersonTable).orderBy as (
			key: (row: unknown) => unknown,
		) => unknown;
		expect(() => untyped(() => cast('string', rowOf(Person).age))).toThrow(
			/ORDER BY takes a field path/,
		);
		expect(() => untyped(() => param('x', SurrealSchema.Int))).toThrow(
			/ORDER BY takes a field path/,
		);
		expect(() => untyped(() => 1)).toThrow(/ORDER BY takes a field path/);
	});

	it('reads through a narrower row schema for a restricted session', () => {
		const Visible = Schema.Struct({
			id: PersonId,
			name: SurrealSchema.String,
			email: Schema.optionalKey(SurrealSchema.String),
		});
		const statement = select(readAs(PersonTable, Visible))
			.where((row) => isNotNone(row.email))
			.build();
		expect(statement.sql).toBe(
			'SELECT * FROM ⟨person⟩ WHERE email != NONE;',
		);
		const decode = Schema.decodeUnknownSync(statement.result);
		expect(
			decode([{ id: new RecordId('person', 'a'), name: 'Ann' }]),
		).toHaveLength(1);
	});
});

it('checks schemafull views even when the table type was widened', () => {
	const view = Schema.Struct({ emial: Schema.String });
	const table = {
		...PersonTable,
		schemafull: Boolean(PersonTable.schemafull),
	};
	expect(() => readAs(table, view)).toThrow(
		/person has no declared field emial/,
	);
	expect(() => readAs({ ...table, schemafull: false }, view)).not.toThrow();
});

it('reads through the generated visible row with one argument, and refuses a descriptor without one', () => {
	const read = readAs(PersonTable);
	expect(read.select).toBe(PersonTable.visible);
	expect(select(read).build().sql).toBe('SELECT * FROM ⟨person⟩;');
	const { visible: _, ...handWritten } = PersonTable;
	const untyped = readAs as unknown as (table: unknown) => unknown;
	expect(() => untyped(handWritten)).toThrow(
		/the descriptor of `person` has no generated `visible` row/,
	);
});

describe('writes', () => {
	const person = param('person', PersonCreate);
	const id = param('id', PersonId);

	it('create one record and return it', () => {
		const statement = create(PersonTable).content(person).build();
		expect(statement.sql).toBe(
			'CREATE ONLY ⟨person⟩ CONTENT $person RETURN AFTER;',
		);
		expect(statement.name).toBe('create person');
		expect(names(statement)).toEqual(['person']);
		expect(
			create(PersonTable, id).content(person).returning('none').build()
				.sql,
		).toBe('CREATE ONLY $id CONTENT $person RETURN NONE;');
		expect(
			create(PersonTable)
				.content(person)
				.returning((row) => ({ id: row.id, city: row.address.city }))
				.build().sql,
		).toBe(
			'CREATE ONLY ⟨person⟩ CONTENT $person RETURN id AS id, address.city AS city;',
		);
	});

	it('update one record by id, or every match', () => {
		const changes = param('changes', PersonMerge);
		const one = update(PersonTable, id).merge(changes).build();
		expect(one.sql).toBe('UPDATE ONLY $id MERGE $changes RETURN AFTER;');
		expect(names(one)).toEqual(['changes', 'id']);
		expect(
			update(PersonTable)
				.merge(changes)
				.where((row) => eq(row.role, 'member'))
				.where((row) => lt(row.age, param('age', SurrealSchema.Int)))
				.returning('before')
				.build().sql,
		).toBe(
			'UPDATE ⟨person⟩ MERGE $changes WHERE role = s"member" AND age < $age RETURN BEFORE;',
		);
		expect(
			update(PersonTable, id)
				.content(param('content', PersonTable.content))
				.returning('none')
				.build().sql,
		).toBe('UPDATE ONLY $id CONTENT $content RETURN NONE;');
	});

	it('a write of one record with RETURN NONE decodes NONE alone, as its type says, and of many an empty list', () => {
		const ast = (statement: { readonly result: Schema.Top }) =>
			statement.result.ast._tag;
		const changes = param('changes', PersonTable.merge);
		expect(
			ast(
				update(PersonTable, id)
					.merge(changes)
					.returning('none')
					.build(),
			),
		).toBe('Undefined');
		expect(ast(deleteFrom(PersonTable, id).build())).toBe('Undefined');
		expect(ast(update(PersonTable).merge(changes).build())).toBe('Arrays');
	});

	it('delete, returning nothing unless asked', () => {
		expect(deleteFrom(PersonTable, id).build().sql).toBe(
			'DELETE ONLY $id RETURN NONE;',
		);
		expect(
			deleteFrom(PersonTable)
				.where((row) => eq(row.role, 'member'))
				.returning('before')
				.build().sql,
		).toBe('DELETE ⟨person⟩ WHERE role = s"member" RETURN BEFORE;');
	});

	it('relate two records through an edge table', () => {
		const statement = relate(
			MemberOfTable,
			param('person', PersonId),
			param('team', TeamId),
		)
			.content(param('membership', MemberOfTable.create))
			.build();
		expect(statement.sql).toBe(
			'RELATE ONLY $person->⟨member_of⟩->$team CONTENT $membership RETURN AFTER;',
		);
		expect(names(statement)).toEqual(['membership', 'person', 'team']);
	});

	it('declare that they write, and which of them always return a row', () => {
		const changes = param('changes', PersonMerge);
		const edge = relate(
			MemberOfTable,
			param('person', PersonId),
			param('team', TeamId),
		).content(param('membership', MemberOfTable.create));
		const cases = [
			[create(PersonTable).content(person).build(), true],
			[
				create(PersonTable)
					.content(person)
					.returning((row) => ({ id: row.id }))
					.build(),
				true,
			],
			[edge.build(), true],
			// RETURN NONE is NONE whether or not anything was written.
			[
				create(PersonTable).content(person).returning('none').build(),
				false,
			],
			[edge.returning('none').build(), false],
			// An update or delete may match nothing.
			[update(PersonTable, id).merge(changes).build(), false],
			[
				update(PersonTable)
					.merge(changes)
					.where((row) => eq(row.role, 'member'))
					.build(),
				false,
			],
			[deleteFrom(PersonTable, id).returning('before').build(), false],
		] as const;
		for (const [statement, nonEmpty] of cases) {
			expect(statement.writes, statement.sql).toBe(true);
			expect(statement.nonEmpty, statement.sql).toBe(nonEmpty);
		}
		const read = select(PersonTable).build();
		expect(read.writes).toBe(false);
		expect(read.nonEmpty).toBe(false);
		// An expression may call a function that writes: unknown, unless
		// the caller says.
		const filtered = select(PersonTable).where((row) =>
			eq(row.role, param('role', SurrealSchema.String)),
		);
		expect('writes' in filtered.build()).toBe(false);
		expect(filtered.build('read', { writes: false }).writes).toBe(false);
		expect(
			select(PersonTable)
				.project((row) => ({ name: row.name }))
				.build().writes,
		).toBe(false);
	});
});

describe('authored statements', () => {
	const bindWrite = part({
		sql: 'LET $producer = $boundProducer;\nLET $event = $boundEvent;\n',
		free: ['boundProducer', 'boundEvent'],
		defines: ['producer', 'event'],
		slots: 2,
	});
	const requireOwner = part({
		sql: "IF fn::can_access($producer) != true { THROW 'Not the owner' };",
		free: ['producer'],
		throws: ['Not the owner'],
	});
	const append = part({
		sql: "IF $event.id = NONE { THROW 'Not the owner' };\nRETURN $event.id;",
		free: ['event'],
		slots: 2,
		throws: ['Not the owner'],
	});

	it('run in one scope: later parts read what earlier parts define', () => {
		const statement = sequence(bindWrite, requireOwner, append).define({
			name: 'append event',
			params: {
				boundProducer: SurrealSchema.String,
				boundEvent: Schema.Struct({ id: SurrealSchema.String }),
			},
			result: SurrealSchema.String,
		});
		expect(statement.sql).toBe(
			[
				'LET $producer = $boundProducer;\nLET $event = $boundEvent;',
				"IF fn::can_access($producer) != true { THROW 'Not the owner' };",
				"IF $event.id = NONE { THROW 'Not the owner' };\nRETURN $event.id;",
			].join('\n'),
		);
		expect(statement.slots).toBe(5);
		expect(statement.throws).toEqual(['Not the owner']);
		expect(names(statement)).toEqual(['boundEvent', 'boundProducer']);
	});

	it('check the contract again when the parts are not typed as literals', () => {
		const untyped: ReadonlyArray<Part> = [bindWrite, requireOwner];
		expect(() =>
			sequence(...untyped).define({
				name: 'guard',
				params: { boundProducer: SurrealSchema.String },
				result: Schema.Undefined,
			}),
		).toThrow(/guard.*missing: \$boundEvent/);
		expect(() =>
			sequence(...untyped).define({
				params: {
					boundProducer: SurrealSchema.String,
					boundEvent: Schema.Unknown,
					producer: SurrealSchema.String,
				},
				result: Schema.Undefined,
			}),
		).toThrow(/not read by any part: \$producer/);
	});

	it('refuse parts that could not run as described', () => {
		const result = Schema.Undefined;
		expect(() =>
			sequence(part({ sql: 'RETURN 1' })).define({ params: {}, result }),
		).toThrow(/does not end with `;`/);
		expect(() =>
			sequence(part({ sql: 'RETURN $a;', free: ['b'] })).define({
				params: { b: Schema.Unknown },
				result,
			}),
		).toThrow(/lists `\$b`, which its text never mentions/);
		// `$auth` is the session's: a statement can read it, never bind it.
		expect(() =>
			sequence(part({ sql: 'RETURN $auth;', free: ['auth'] })).define({
				params: { auth: Schema.Unknown },
				result,
			}),
		).toThrow(/set by SurrealDB/);
		// A caller cannot bind `$event`, but a part may define it.
		expect(() =>
			sequence(part({ sql: 'RETURN $event;', free: ['event'] })).define({
				params: { event: Schema.Unknown },
				result,
			}),
		).toThrow(/can be shadowed/);
	});

	it('check a part against its own text when it is declared', () => {
		// Finding 5: a read nobody declared used to run as NONE.
		expect(() =>
			part({
				name: 'by email',
				sql: 'SELECT VALUE name FROM person WHERE email = $email;',
			}),
		).toThrow(/part `by email`.*free: the text reads \$email/);
		// A name the text only reads is not one it defines.
		expect(() =>
			part({ sql: 'RETURN $n;', free: ['n'], defines: ['n'] }),
		).toThrow(/defines: .*\$n.*not LET at the top level/);
		expect(() => part({ sql: 'LET $n = 1;', free: ['n'] })).toThrow(
			/free: .*\$n.*does not read/,
		);
		expect(() =>
			part({ sql: 'LET $a = 1; RETURN $a;', defines: ['a'] }),
		).toThrow(/slots: declared 1, the text returns 2/);
		expect(() => part({ sql: "THROW 'boom';" })).toThrow(
			/throws: the text can THROW 'boom'/,
		);
		expect(() =>
			part({ sql: 'RETURN 1;', throws: ['never thrown'] }),
		).toThrow(/throws: .*'never thrown'.*never THROWs/);
		expect(() =>
			part({ sql: 'SELECT * FROM person WHERE __FILTER__;' }),
		).toThrow(/has holes \(__FILTER__\)/);
		// What the text says, declared, is accepted as before.
		expect(() =>
			part({
				sql: 'LET $a = $b; IF $a { THROW "no" }; RETURN $a;',
				free: ['b'],
				defines: ['a'],
				slots: 3,
				throws: ['no'],
			}),
		).not.toThrow();
	});

	it('check a part that was not declared with `part` when it is sequenced', () => {
		const handMade: Part = {
			sql: 'RETURN $a;',
			free: [],
			defines: [],
			slots: 1,
			throws: [],
		};
		expect(() =>
			sequence(handMade).define({
				params: {},
				result: Schema.Unknown,
			}),
		).toThrow(/part 1 of the sequence.*free: the text reads \$a/);
	});

	it('refuse a directly declared text that LETs a name it does not declare', () => {
		const declare = (sql: string, defines?: ReadonlyArray<string>) =>
			make({
				sql,
				params: {},
				result: Schema.Unknown,
				slots: 2,
				...(defines === undefined ? {} : { defines }),
			});
		expect(() => declare("LET $owner = 'a'; RETURN $owner;")).toThrow(
			/LETs `\$owner` at the top level but declares no `defines`/,
		);
		expect(() => declare('RETURN 1;\n  let $x = 2;')).toThrow(/`\$x`/);
		expect(() => declare('/* c */ LET $y = 1; RETURN $y;')).toThrow(
			/`\$y`/,
		);
		// Declared, it is trusted, and a transaction refuses `$owner`.
		expect(
			declare("LET $owner = 'a'; RETURN $owner;", ['owner']).defines,
		).toEqual(['owner']);
		// Not top-level: in a block, a string, a quoted identifier or a comment.
		for (const sql of [
			'IF true { LET $x = 1; RETURN $x };',
			"RETURN 'LET $x = 1;';",
			'RETURN "; LET $x";',
			'SELECT * FROM `; LET $x`;',
			'-- LET $x = 1;\nRETURN 1;',
			'# LET $x = 1;\nRETURN 1;',
			'RETURN (SELECT * FROM [1]); /* LET $x */',
			'RETURN outlet;',
		]) {
			expect(declare(sql).defines).toEqual([]);
		}
	});

	it('declare a statement directly, unchecked', () => {
		const statement = make({
			sql: 'RETURN $a + 1;',
			params: { a: SurrealSchema.Int },
			result: SurrealSchema.Int,
		});
		expect(statement.name).toBe('statement');
		expect(statement.slots).toBe(1);
		expect(() =>
			make({ sql: ' ', params: {}, result: Schema.Undefined }),
		).toThrow(/has no text/);
		// Finding 10: every text ends with exactly one terminator, so texts
		// joined in a transaction always parse.
		const text = (sql: string) =>
			make({ sql, params: {}, result: Schema.Unknown }).sql;
		expect(text('RETURN 1')).toBe('RETURN 1;');
		expect(text('RETURN 1;')).toBe('RETURN 1;');
		expect(text('RETURN 1 ; ;\n')).toBe('RETURN 1;');
		expect(() => text(' ; ')).toThrow(/has no text/);
		expect(() =>
			make({
				sql: 'RETURN 1;',
				params: {},
				result: Schema.Undefined,
				slots: 0,
			}),
		).toThrow(/at least one/);
		expect(() =>
			make({
				sql: 'RETURN $token;',
				params: { token: Schema.Unknown },
				result: Schema.Undefined,
			}),
		).toThrow(/set by SurrealDB/);
	});

	it('take what the text writes from the analyzer, and check a declared one', () => {
		expect(part({ sql: 'CREATE t:one;' }).writes).toBe(true);
		// Finding no write is not proof of none: a function may write.
		expect(part({ sql: 'RETURN 1;' }).writes).toBeUndefined();
		expect(part({ sql: 'RETURN 1;', writes: false }).writes).toBe(false);
		expect(part({ sql: 'CREATE t:one;', writes: true }).writes).toBe(true);
		expect(() => part({ sql: 'CREATE t:one;', writes: false })).toThrow(
			/writes: declared false, the text writes/,
		);
		expect(() => part({ sql: 'RETURN 1;', writes: true })).toThrow(
			/writes: declared true, the text does not write/,
		);
		const statement = sequence(
			part({
				name: 'guard',
				source: 'a/guard.surql',
				sql: "IF $n < 0 { THROW 'Negative' };",
				free: ['n'],
				throws: ['Negative'],
				throwsAt: { Negative: [1] },
			}),
			{
				sql: 'UPDATE t:one SET n = $n;',
				free: ['n'],
				defines: [],
				slots: 1,
				throws: [],
			},
		).define({
			name: 'set',
			params: { n: SurrealSchema.Int },
			result: Schema.Unknown,
		});
		expect(statement.writes).toBe(true);
		expect(statement.sources).toEqual(['a/guard.surql']);
		expect(statement.throwSites).toEqual({
			Negative: { source: 'a/guard.surql', lines: [1] },
		});
		const made = make({
			sql: 'RETURN 1;',
			params: {},
			result: Schema.Unknown,
		});
		expect(made).toMatchObject({ nonEmpty: false });
		expect('writes' in made).toBe(false);
		expect(
			make({ ...made, writes: false, params: {}, result: Schema.Unknown })
				.writes,
		).toBe(false);
	});
});

describe('select by a unique index', () => {
	it('reads one row through an equality on every field of the index', () => {
		const byHandle = select(Members.MemberTable, 'member_handle', {
			handle: param('handle', SurrealSchema.String),
		}).build();
		expect(byHandle.sql).toBe(
			'SELECT * FROM ONLY ⟨member⟩ WITH INDEX member_handle WHERE handle = $handle LIMIT 1;',
		);
		expect(Object.keys(byHandle.params)).toEqual(['handle']);
		const composite = select(Members.MemberTable, 'member_region_number', {
			number: param('number', SurrealSchema.Int),
			region: 'north',
		}).build();
		// In the index's own column order, whatever the order of the values.
		expect(composite.sql).toBe(
			'SELECT * FROM ONLY ⟨member⟩ WITH INDEX member_region_number WHERE region = s"north" AND number = $number LIMIT 1;',
		);
		const nested = select(Members.MemberTable, 'member_bio', {
			'profile.bio': param('bio', SurrealSchema.String),
		})
			.where((row) => eq(row.region, 'north'))
			.build();
		expect(nested.sql).toBe(
			'SELECT * FROM ONLY ⟨member⟩ WITH INDEX member_bio WHERE profile.bio = $bio AND region = s"north" LIMIT 1;',
		);
		const decode = Schema.decodeUnknownSync(byHandle.result);
		expect(decode(undefined)).toBeUndefined();
	});

	it('refuses at runtime what the types refuse', () => {
		const untyped = select as unknown as (
			table: unknown,
			index: string,
			values: Record<string, unknown>,
		) => unknown;
		const { MemberTable } = Members;
		expect(() =>
			untyped(MemberTable, 'member_region', { region: 'a' }),
		).toThrow(
			'`member_region` is a standard index of `member`, not a unique one',
		);
		expect(() => untyped(MemberTable, 'nope', {})).toThrow(
			'`member` has no index `nope`',
		);
		expect(() =>
			untyped(MemberTable, 'member_region_number', { region: 'a' }),
		).toThrow(
			'the unique index `member_region_number` also needs `number`',
		);
		expect(() =>
			untyped(MemberTable, 'member_handle', { handle: 'a', x: 1 }),
		).toThrow('`x` is not a field of the unique index `member_handle`');
		expect(() =>
			untyped(
				{
					...MemberTable,
					indexes: {
						tagged: { type: 'unique', columns: [['tags', '*']] },
					},
				},
				'tagged',
				{ 'tags.*': 'a' },
			),
		).toThrow('`tagged` indexes array elements');
	});
});

describe('record ids from their key parts, and id ranges', () => {
	const { IdArrayTable, IdIntTable, IdObjectTable } = Corpus;

	it('build an id with type::record from a scalar, an array or an object of parts', () => {
		const scalar = recordId(
			PersonTable,
			param('key', SurrealSchema.String),
		);
		expect(sqlOf(scalar)).toBe('type::record(s"person", $key)');
		expect(Object.keys(paramsOf(scalar))).toEqual(['key']);
		expect(
			sqlOf(
				recordId(IdArrayTable, [
					'a',
					param('at', SurrealSchema.DateTime),
				]),
			),
		).toBe('type::record(s"id_array", [s"a", $at])');
		expect(
			sqlOf(
				recordId(IdObjectTable, {
					region: param('region', SurrealSchema.String),
					n: 1,
				}),
			),
		).toBe('type::record(s"id_object", { "region": $region, "n": 1 })');
		// The id decodes with the table's id codec.
		expect(codecOf(scalar)).toBe(PersonTable.id);
	});

	it('are the subject of every builder that takes a record id', () => {
		const key = recordId(IdIntTable, param('n', SurrealSchema.Int));
		expect(select(IdIntTable, key).only().build().sql).toBe(
			'SELECT * FROM ONLY type::record(s"id_int", $n);',
		);
		expect(create(IdIntTable, key).build().sql).toBe(
			'CREATE ONLY type::record(s"id_int", $n) RETURN AFTER;',
		);
		expect(
			update(IdIntTable, key)
				.merge(param('changes', Corpus.IdIntMerge))
				.build().sql,
		).toBe(
			'UPDATE ONLY type::record(s"id_int", $n) MERGE $changes RETURN AFTER;',
		);
		expect(deleteFrom(IdIntTable, key).build().sql).toBe(
			'DELETE ONLY type::record(s"id_int", $n) RETURN NONE;',
		);
		// RELATE parses only a binding or a record literal as an endpoint.
		expect(
			relate(
				MemberOfTable,
				recordId(PersonTable, param('person', SurrealSchema.String)),
				param('team', TeamId),
			).build().sql,
		).toBe(
			'RELATE ONLY (type::record(s"person", $person))->⟨member_of⟩->$team RETURN AFTER;',
		);
	});

	it('span a range of keys: both bounds, an open side, an excluded start, an included end', () => {
		const low = param('low', SurrealSchema.Int);
		const high = param('high', SurrealSchema.Int);
		const untyped = idRange as unknown as (
			table: typeof IdIntTable,
			bounds: unknown,
		) => IdRange<'id_int', NoParams>;
		const text = (bounds: unknown) =>
			select(IdIntTable, untyped(IdIntTable, bounds)).build().sql;
		expect(text({ from: low, to: high })).toBe(
			'SELECT * FROM type::record(s"id_int", $low..$high);',
		);
		expect(text({ from: low, through: high })).toBe(
			'SELECT * FROM type::record(s"id_int", $low..=$high);',
		);
		expect(text({ after: low })).toBe(
			'SELECT * FROM type::record(s"id_int", $low>..);',
		);
		expect(text({ through: high })).toBe(
			'SELECT * FROM type::record(s"id_int", ..=$high);',
		);
		expect(text({})).toBe('SELECT * FROM type::record(s"id_int", ..);');
		const week = select(
			IdArrayTable,
			idRange(IdArrayTable, {
				from: ['a', param('start', SurrealSchema.DateTime)],
				to: ['a', param('end', SurrealSchema.DateTime)],
			}),
		);
		expect(week.build().sql).toBe(
			'SELECT * FROM type::record(s"id_array", [s"a", $start]..[s"a", $end]);',
		);
		// A range reads many rows, so ONLY reads one of them.
		expect(week.only().build().sql).toBe(
			'SELECT * FROM ONLY type::record(s"id_array", [s"a", $start]..[s"a", $end]) LIMIT 1;',
		);
		expect(() => week.only().limit(2)).toThrow(/at most one row/);
	});

	it('refuse at runtime what the types refuse', () => {
		const untypedRange = idRange as unknown as (
			table: unknown,
			bounds: unknown,
		) => unknown;
		expect(() => untypedRange(IdIntTable, { from: 1, after: 2 })).toThrow(
			/not both/,
		);
		expect(() => untypedRange(IdIntTable, { to: 1, through: 2 })).toThrow(
			/not both/,
		);
		const untypedId = recordId as unknown as (
			table: unknown,
			key: unknown,
		) => unknown;
		expect(() => untypedId(IdArrayTable, [['nested'], 1])).toThrow(
			/nest no array or object inside a key/,
		);
		expect(() => untypedId(IdArrayTable, [])).toThrow(/at least one part/);
		const untypedSelect = select as unknown as (
			table: unknown,
			subject: unknown,
		) => unknown;
		expect(() =>
			untypedSelect(PersonTable, idRange(IdIntTable, {})),
		).toThrow('a range of `id_int` is not a subject of `person`');
	});
});

describe('UPSERT, INSERT and every RETURN mode', () => {
	const id = param('id', PersonId);
	// What an UPSERT may send: what CREATE requires, and nothing MERGE refuses.
	const Upserted = Schema.Struct({
		address: PersonCreate.fields.address,
		bio: PersonCreate.fields.bio,
		email: PersonCreate.fields.email,
		name: PersonCreate.fields.name,
		score: PersonCreate.fields.score,
	});
	const changes = param('changes', Upserted);
	const person = param('person', PersonCreate);
	const decode = (statement: { readonly result: Schema.Top }) =>
		Schema.decodeUnknownSync(
			statement.result as Schema.Codec<unknown, unknown>,
		);

	it('UPSERT one record, MERGE or CONTENT, with each RETURN mode', () => {
		expect(upsert(PersonTable, id).merge(changes).build().sql).toBe(
			'UPSERT ONLY $id MERGE $changes RETURN AFTER;',
		);
		// SurrealDB 3.3 checks `RETURN NONE` data as if the record were new.
		expect(
			upsert(PersonTable, id)
				.content(param('content', PersonContent))
				.returning('none')
				.build().sql,
		).toBe('UPSERT ONLY $id CONTENT $content RETURN VALUE NONE;');
		const before = upsert(PersonTable, id)
			.merge(changes)
			.returning('before')
			.build();
		expect(before.sql).toBe(
			'UPSERT ONLY $id MERGE $changes RETURN BEFORE;',
		);
		expect(decode(before)(undefined)).toBeUndefined();
		expect(before.nonEmpty).toBe(false);
		expect(upsert(PersonTable, id).merge(changes).build().nonEmpty).toBe(
			true,
		);
		expect(
			upsert(PersonTable, id).merge(changes).returning('diff').build()
				.nonEmpty,
		).toBe(false);
	});

	it('UPSERT a table by condition, returning every record', () => {
		const statement = upsert(PersonTable)
			.merge(changes)
			.where((row) => eq(row.email, param('email', SurrealSchema.String)))
			.returning('none')
			.build();
		// With `RETURN NONE`, SurrealDB 3.3 also creates a record when the
		// condition matches; `RETURN VALUE NONE` answers one NONE per record.
		expect(statement.sql).toBe(
			'UPSERT ⟨person⟩ MERGE $changes WHERE email = $email RETURN VALUE NONE;',
		);
		expect(decode(statement)([])).toEqual([]);
		expect(decode(statement)([undefined, undefined])).toEqual([]);
		expect(() => decode(statement)([{ name: 'x' }])).toThrow();
	});

	it('INSERT one row or many, ON DUPLICATE KEY UPDATE from $input, INSERT RELATION', () => {
		const people = param('people', Schema.Array(PersonCreate));
		expect(insert(PersonTable, people).build().sql).toBe(
			'INSERT INTO ⟨person⟩ $people RETURN AFTER;',
		);
		const upserting = insert(PersonTable, param('person', PersonCreate))
			.onDuplicate((input) => ({
				name: input.name,
				bio: param('bio', SurrealSchema.String),
				score: 1,
			}))
			.returning('before')
			.build();
		expect(upserting.sql).toBe(
			'INSERT INTO ⟨person⟩ $person ON DUPLICATE KEY UPDATE name = $input.name, bio = $bio, score = 1 RETURN BEFORE;',
		);
		expect(Object.keys(upserting.params).sort()).toEqual(['bio', 'person']);
		expect(decode(upserting)([undefined])).toEqual([undefined]);
		const edges = param(
			'edges',
			Schema.Array(
				Schema.Struct({
					in: PersonId,
					out: TeamId,
					...MemberOfTable.create.fields,
				}),
			),
		);
		expect(
			insertRelation(MemberOfTable, edges).returning('none').build().sql,
		).toBe('INSERT RELATION INTO ⟨member_of⟩ $edges RETURN NONE;');
	});

	it('DIFF on every write, decoded as JSON Patch operations', () => {
		const diff = update(PersonTable, id)
			.merge(param('patch', PersonMerge))
			.returning('diff')
			.build();
		expect(diff.sql).toBe('UPDATE ONLY $id MERGE $patch RETURN DIFF;');
		expect(
			decode(diff)([
				{ op: 'add', path: '/nickname', value: 'an' },
				{ op: 'remove', path: '/age' },
				{ op: 'replace', path: '/score', value: 2 },
				{ op: 'change', path: '/name', value: '@@ -1 +1 @@\n-a\n+b\n' },
			]),
		).toHaveLength(4);
		expect(decode(diff)(undefined)).toBeUndefined();
		expect(() =>
			decode(diff)([{ op: 'move', path: '/a', from: '/b' }]),
		).toThrow();
		expect(
			decode(deleteFrom(PersonTable, id).returning('diff').build())([
				{ op: 'replace', path: '', value: undefined },
			]),
		).toEqual([{ op: 'replace', path: '', value: undefined }]);
		for (const statement of [
			create(PersonTable).returning('diff').build(),
			relate(MemberOfTable, param('p', PersonId), param('t', TeamId))
				.returning('diff')
				.build(),
			insert(PersonTable, person).returning('diff').build(),
		]) {
			expect(statement.sql).toMatch(/ RETURN DIFF;$/u);
		}
	});

	it('DELETE returns NONE after, and reads the deleted record through $before', () => {
		const gone = deleteFrom(PersonTable)
			.where((row) => eq(row.role, 'admin'))
			.returning((row) => ({ id: row.id, name: row.name }))
			.build();
		expect(gone.sql).toBe(
			'DELETE ⟨person⟩ WHERE role = s"admin" RETURN $before.id AS id, $before.name AS name;',
		);
		const value = deleteFrom(PersonTable, id)
			.returningValue((row) => row.email)
			.build();
		expect(value.sql).toBe('DELETE ONLY $id RETURN VALUE $before.email;');
		const after = deleteFrom(PersonTable).returning('after').build();
		expect(decode(after)([undefined, undefined])).toEqual([
			undefined,
			undefined,
		]);
	});

	it('RETURN VALUE on every write', () => {
		expect(
			create(PersonTable)
				.content(person)
				.returningValue((row) => row.id)
				.build().sql,
		).toBe('CREATE ONLY ⟨person⟩ CONTENT $person RETURN VALUE id;');
		expect(
			update(PersonTable)
				.merge(param('patch', PersonMerge))
				.returningValue((row) => row.age)
				.build().sql,
		).toBe('UPDATE ⟨person⟩ MERGE $patch RETURN VALUE age;');
		const created = create(PersonTable)
			.returningValue((row) => row.tags)
			.build();
		// A value may be NONE or an empty list itself, so it is not checked.
		expect(created.nonEmpty).toBe(false);
	});

	it('refuses at runtime what the types refuse', () => {
		const untyped = insert(PersonTable, person) as unknown as {
			onDuplicate: (set: (input: unknown) => unknown) => {
				onDuplicate: (set: (input: unknown) => unknown) => unknown;
			};
		};
		expect(() => untyped.onDuplicate(() => ({}))).toThrow(
			/at least one assignment/,
		);
		expect(() => untyped.onDuplicate(() => ({ created_at: 1 }))).toThrow(
			'`created_at` is not a field `person` accepts in ON DUPLICATE KEY UPDATE',
		);
		expect(() =>
			untyped
				.onDuplicate(() => ({ name: 'a' }))
				.onDuplicate(() => ({ name: 'b' })),
		).toThrow(/one ON DUPLICATE KEY UPDATE clause/);
	});
});

describe('GROUP BY and GROUP ALL', () => {
	it('projects every key and the aggregates, and groups by the key paths', () => {
		const statement = select(PersonTable)
			.where((row) => gte(row.score, 10))
			.groupBy((row) => ({ role: row.role, city: row.address.city }))
			.aggregate((row) => ({
				people: count(),
				total: sum(row.score),
				oldest: max(row.age),
				names: arrayGroup(row.name),
			}))
			.orderBy('people', 'DESC')
			.limit(5)
			.build();
		expect(statement.sql).toBe(
			'SELECT role AS role, address.city AS city, count() AS people, math::sum(score) AS total, math::max(age) AS oldest, array::group(name) AS names FROM ⟨person⟩ WHERE score >= 10 GROUP BY role, address.city ORDER BY people DESC LIMIT 5;',
		);
		const decode = Schema.decodeUnknownSync(statement.result);
		// `age` is optional, so its maximum over the group may be -Infinity.
		expect(
			decode([
				{
					role: 'admin',
					city: 'Leeds',
					people: 2,
					total: 30.5,
					oldest: Number.NEGATIVE_INFINITY,
					names: ['Ann', 'Bob'],
				},
			]),
		).toHaveLength(1);
	});

	it('GROUP ALL is one row of aggregates', () => {
		const statement = select(PersonTable)
			.groupAll()
			.aggregate((row) => ({
				people: count(),
				average: mean(row.score),
				median: median(row.score),
				first: timeMin(row.created_at),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT count() AS people, math::mean(score) AS average, math::median(score) AS median, time::min(created_at) AS first FROM ⟨person⟩ GROUP ALL;',
		);
		const decode = Schema.decodeUnknownSync(statement.result);
		expect(
			decode([
				{
					people: 0,
					average: Number.NaN,
					median: undefined,
					first: undefined,
				},
			]),
		).toHaveLength(1);
		expect(() => decode([])).toThrow();
	});

	it('picks the codec of math::sum from the field kind, without its checks', () => {
		const statement = select(PersonTable)
			.groupBy((row) => ({ role: row.role }))
			.aggregate((row) => ({ ages: sum(row.age) }))
			.build();
		// `age` is checked to be at most 150; a sum of ages is not.
		expect(
			Schema.decodeUnknownSync(statement.result)([
				{ role: 'admin', ages: 400 },
			]),
		).toEqual([{ role: 'admin', ages: 400 }]);
	});

	it('refuses at runtime what the types refuse', () => {
		const untyped = select(PersonTable) as unknown as {
			groupBy: (keys: (row: unknown) => unknown) => {
				aggregate: (fields: (row: unknown) => unknown) => unknown;
				orderBy: (alias: string) => unknown;
			};
		};
		expect(() => untyped.groupBy(() => ({ role: literal(1) }))).toThrow(
			/GROUP BY takes a field path/,
		);
		const grouped = untyped.groupBy((row) => ({
			role: (row as { role: unknown }).role,
		}));
		expect(() =>
			grouped.aggregate((row) => ({
				name: (row as { name: unknown }).name,
			})),
		).toThrow('`name` is a field that is neither grouped nor aggregated');
		expect(() => grouped.aggregate(() => ({ role: count() }))).toThrow(
			'`role` is already a key of this GROUP BY',
		);
		expect(() => grouped.orderBy('nope')).toThrow(
			'`nope` is not a group key or an aggregate of this SELECT',
		);
		expect(() =>
			select(PersonTable)
				.project((row) => ({ name: row.name }))
				.groupAll(),
		).toThrow(/group a SELECT before projecting/);
	});
});

describe('SPLIT and OMIT', () => {
	const { PersonTable: CorpusPerson } = Corpus;

	it('SPLIT an array field, after the conditions', () => {
		const statement = select(CorpusPerson)
			.where((row) => eq(row.role, 'admin'))
			.split('pins')
			.orderBy((row) => row.pins)
			.limit(10)
			.build();
		expect(statement.sql).toBe(
			'SELECT * FROM ⟨person⟩ WHERE role = s"admin" SPLIT pins ORDER BY pins ASC LIMIT 10;',
		);
		const projected = select(CorpusPerson)
			.split('pins')
			.project((row) => ({ name: row.name, pins: row.pins }))
			.build();
		expect(projected.sql).toBe(
			'SELECT name AS name, pins AS pins FROM ⟨person⟩ SPLIT pins;',
		);
		const decode = Schema.decodeUnknownSync(projected.result);
		// One element per row, or the array itself when it is empty.
		expect(
			decode([
				{ name: 'a', pins: 'x' },
				{ name: 'b', pins: [] },
			]),
		).toEqual([
			{ name: 'a', pins: 'x' },
			{ name: 'b', pins: [] },
		]);
	});

	it('OMIT top-level fields of SELECT *, before or after SPLIT', () => {
		const statement = select(CorpusPerson)
			.omit('ssn', 'notes')
			.orderBy((row) => row.ssn)
			.build();
		expect(statement.sql).toBe(
			'SELECT * OMIT ssn, notes FROM ⟨person⟩ ORDER BY ssn ASC;',
		);
		expect(select(CorpusPerson).split('pins').omit('id').build().sql).toBe(
			'SELECT * OMIT id FROM ⟨person⟩ SPLIT pins;',
		);
	});

	it('refuses at runtime what the types refuse', () => {
		const untyped = select(CorpusPerson) as unknown as {
			split: (field: string) => {
				where: (predicate: () => unknown) => unknown;
				project: (
					fields: (row: { name: unknown; pins: unknown }) => unknown,
				) => {
					build: () => unknown;
				};
				omit: (...fields: ReadonlyArray<string>) => unknown;
			};
			omit: (...fields: ReadonlyArray<string>) => {
				split: (field: string) => unknown;
				project: (fields: () => unknown) => unknown;
			};
		};
		expect(() => untyped.split('name')).toThrow(
			'`name` is not an array field of `person`: SPLIT takes one',
		);
		expect(() => untyped.split('tags')).toThrow(/is not an array field/);
		expect(() =>
			untyped.split('pins').where(() => eq(literal(1), 1)),
		).toThrow(/add conditions before `.split/);
		expect(() =>
			untyped
				.split('pins')
				.project((row) => ({ name: row.name, pin: row.pins }))
				.build(),
		).toThrow(
			/the SPLIT field `pins` must be projected under its own name/,
		);
		expect(() => untyped.split('pins').omit('pins')).toThrow(
			/is the SPLIT field/,
		);
		expect(() => untyped.omit('nope')).toThrow(
			'`nope` is not a field of `person`',
		);
		expect(() => untyped.omit('ssn').split('pins')).toThrow(
			/split a SELECT before/,
		);
		expect(() => untyped.omit('ssn').project(() => ({}))).toThrow(
			/a projection names the fields it keeps/,
		);
	});
});
