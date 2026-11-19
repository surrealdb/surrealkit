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
	at,
	call,
	cast,
	codecOf,
	contains,
	create,
	deleteFrom,
	eq,
	gt,
	gte,
	inside,
	isNone,
	isNotNone,
	isStatement,
	literal,
	lt,
	lte,
	make,
	ne,
	not,
	or,
	type Part,
	param,
	paramsOf,
	part,
	readAs,
	relate,
	rowOf,
	select,
	sequence,
	sqlOf,
	unsafeExpr,
	update,
} from '../src/statement/index.js';
import {
	fn_greet,
	MemberOfTable,
	Person,
	PersonCreate,
	PersonId,
	PersonMerge,
	PersonTable,
	TeamId,
} from './golden/example/index.js';

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
});
