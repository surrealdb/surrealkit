/**
 * Positive type fixtures for `effect-surrealkit/statement` and
 * `effect-surrealkit/client`: what the builders derive from the golden
 * generated modules. Compiled by `tsc -p test/types`.
 */
import { type Effect, Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import type { DateTime } from 'surrealdb';

import {
	bind,
	run,
	type StatementConflict,
	type StatementDecodeError,
	type StatementEmptyResult,
	type StatementEncodeError,
	type StatementError,
	type StatementRequestError,
	type StatementScopeError,
	type StatementThrown,
	type StatementUnexpectedThrow,
	type SurrealClient,
	transaction,
} from '../../src/client/index.js';
import {
	and,
	at,
	call,
	cast,
	contains,
	create,
	deleteFrom,
	eq,
	gte,
	type Input,
	inside,
	isNone,
	literal,
	not,
	or,
	param,
	part,
	type Result,
	readAs,
	relate,
	select,
	sequence,
	update,
} from '../../src/statement/index.js';
import {
	fn_greet,
	fn_optional_first,
	type MemberOfRow,
	MemberOfTable,
	Person,
	PersonCreate,
	PersonId,
	PersonMerge,
	type PersonRow,
	PersonTable,
	TeamId,
} from '../golden/example/index.js';

/** Fails to compile unless `A` and `B` are the same type. */
type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// ---- SELECT: stored rows decode with the descriptor's select schema.

export const everyone = select(PersonTable).build();
same<Result<typeof everyone>, ReadonlyArray<PersonRow>>(true);
same<Input<(typeof everyone)['params']>, Record<never, never>>(true);

// Bindings accumulate from every clause, and the input names them once.
export const adults = select(PersonTable)
	.where((row) => gte(row.age, param('minimum', SurrealSchema.Int)))
	.where((row) => eq(row.address.city, param('city', SurrealSchema.String)))
	.orderBy((row) => row.name)
	.limit(param('rows', SurrealSchema.Int))
	.build();
same<
	Input<(typeof adults)['params']>,
	{ readonly minimum: number; readonly city: string; readonly rows: number }
>(true);

// ORDER BY takes field paths: nested fields and array elements included.
export const ordered = select(PersonTable)
	.orderBy((row) => row.address.city)
	.orderBy((row) => at(row.phones, 0), 'DESC')
	.orderBy((row) => row['odd.name'])
	.value((row) => row.name)
	.build();
same<Input<(typeof ordered)['params']>, Record<never, never>>(true);

// An optional field, and a field read through one, may be NONE.
export const projected = select(PersonTable)
	.project((row) => ({
		name: row.name,
		nickname: row.nickname,
		city: row.address.city,
		odd: row['odd.name'],
		manager: row.manager,
		first: at(row.phones, 0),
		key: cast('string', row.id),
		greeting: call(
			fn_greet,
			row.name,
			param('greeting', SurrealSchema.String),
		),
	}))
	.build();
same<
	Result<typeof projected>,
	ReadonlyArray<{
		readonly name: string;
		readonly nickname: string | undefined;
		readonly city: string;
		readonly odd: string | undefined;
		readonly manager: PersonId | undefined;
		readonly first:
			| { readonly kind: 'home' | 'mobile' | 'work' }
			| undefined;
		readonly key: string;
		readonly greeting: string;
	}>
>(true);
same<Input<(typeof projected)['params']>, { readonly greeting: string }>(true);

export const names = select(PersonTable)
	.value((row) => row.name)
	.build();
same<Result<typeof names>, ReadonlyArray<string>>(true);

export const oneByEmail = select(PersonTable)
	.where((row) => eq(row.email, param('email', SurrealSchema.String)))
	.only()
	.build();
same<Result<typeof oneByEmail>, PersonRow | undefined>(true);

export const byId = select(PersonTable, param('id', PersonId)).only().build();
same<Input<(typeof byId)['params']>, { readonly id: PersonId }>(true);
same<Result<typeof byId>, PersonRow | undefined>(true);

// Conditions: primitives inline, refined and branded bindings compare with
// their column, literal unions take their members.
const Email = SurrealSchema.String.check(Schema.isPattern(/@/u));
declare const since: DateTime;
export const conditions = select(PersonTable)
	.where((row) =>
		and(
			or(eq(row.role, 'admin'), eq(row.role, 'owner')),
			not(isNone(row.manager)),
			eq(row.manager, param('manager', PersonId)),
			eq(row.email, param('email', Email)),
			eq(row.bio, null),
			inside(row.role, ['admin', 'member']),
			inside(
				row.name,
				param('names', SurrealSchema.SetOf(SurrealSchema.String)),
			),
			contains(row.tags, 'vip'),
			gte(row.created_at, literal(since, SurrealSchema.DateTime)),
			eq(call(fn_optional_first, row.age, row.score), 1),
			eq(call(fn_optional_first, undefined, 2), 1),
			eq(call(fn_greet, 'Ann'), 'Hello, Ann'),
		),
	)
	.build();

// A subquery is an expression with its own bindings.
export const withSubquery = select(PersonTable)
	.project((row) => ({
		name: row.name,
		peers: select(PersonTable)
			.where((peer) => eq(peer.role, param('role', SurrealSchema.String)))
			.value((peer) => peer.name)
			.toExpr(),
	}))
	.build();
same<
	Result<typeof withSubquery>,
	ReadonlyArray<{
		readonly name: string;
		readonly peers: ReadonlyArray<string>;
	}>
>(true);
same<Input<(typeof withSubquery)['params']>, { readonly role: string }>(true);

// ---- Writes: data fits the operation's schema; the result follows the
// target and the RETURN clause.

export const createPerson = create(PersonTable)
	.content(param('person', PersonCreate))
	.build();
same<Result<typeof createPerson>, PersonRow>(true);
same<Input<(typeof createPerson)['params']>, { readonly person: PersonCreate }>(
	true,
);

export const createQuietly = create(PersonTable, param('id', PersonId))
	.content(param('person', PersonCreate))
	.returning('none')
	.build();
same<Result<typeof createQuietly>, undefined>(true);

export const createKey = create(PersonTable)
	.content(param('person', PersonCreate))
	.returning((row) => ({ id: row.id, name: row.name }))
	.build();
same<
	Result<typeof createKey>,
	{ readonly id: PersonId; readonly name: string }
>(true);

// A narrower value than the operation's schema still fits.
export const mergeName = update(PersonTable, param('id', PersonId))
	.merge(param('changes', Schema.Struct({ name: PersonMerge.fields.name })))
	.build();

export const mergeOne = update(PersonTable, param('id', PersonId))
	.merge(param('changes', PersonMerge))
	.build();
same<Result<typeof mergeOne>, PersonRow | undefined>(true);

export const mergeMany = update(PersonTable)
	.merge(param('changes', PersonMerge))
	.where((row) => eq(row.role, 'member'))
	.returning('none')
	.build();
same<Result<typeof mergeMany>, readonly []>(true);

export const mergeNames = update(PersonTable)
	.merge(param('changes', PersonMerge))
	.returning((row) => ({ name: row.name }))
	.build();
same<Result<typeof mergeNames>, ReadonlyArray<{ readonly name: string }>>(true);

export const removeOne = deleteFrom(PersonTable, param('id', PersonId))
	.returning('before')
	.build();
same<Result<typeof removeOne>, PersonRow | undefined>(true);

export const removeMany = deleteFrom(PersonTable)
	.where((row) => eq(row.role, param('role', SurrealSchema.String)))
	.build();
same<Result<typeof removeMany>, readonly []>(true);

export const join = relate(
	MemberOfTable,
	param('person', PersonId),
	param('team', TeamId),
)
	.content(param('membership', MemberOfTable.create))
	.build();
same<Result<typeof join>, MemberOfRow>(true);

// ---- A session with narrower permissions declares the row it really gets.

const Visible = Schema.Struct({
	id: Person.fields.id,
	name: Person.fields.name,
	email: Schema.optionalKey(Person.fields.email),
});
export const visible = select(readAs(PersonTable, Visible))
	.where((row) => eq(row.name, param('name', SurrealSchema.String)))
	.build();
same<
	Result<typeof visible>,
	ReadonlyArray<{
		readonly id: PersonId;
		readonly name: string;
		readonly email?: string;
	}>
>(true);

// ---- Authored SurrealQL: the contract names exactly the unbound names.

const bindWrite = part({
	sql: 'LET $producer = $boundProducer; LET $principal = $auth.id;',
	free: ['boundProducer'],
	defines: ['producer', 'principal'],
	slots: 2,
});
const requireOwner = part({
	sql: "IF fn::can_access($producer) != true { THROW 'Not the owner' };",
	free: ['producer'],
	throws: ['Not the owner'],
});
const readCutover = part({
	sql: 'SELECT VALUE revision FROM ONLY cutover WHERE producer = $producer AND principal = $principal LIMIT 1;',
	free: ['producer', 'principal'],
});

export const guarded = sequence(bindWrite, requireOwner, readCutover).define({
	name: 'read cutover',
	params: { boundProducer: SurrealSchema.String },
	result: SurrealSchema.String,
});
same<Input<(typeof guarded)['params']>, { readonly boundProducer: string }>(
	true,
);
same<Result<typeof guarded>, string>(true);

// ---- Client: the error channel names the declared THROW messages.

export const ran = run(guarded, { boundProducer: 'a' });
same<
	typeof ran,
	Effect.Effect<
		string,
		| StatementEncodeError
		| StatementScopeError
		| StatementRequestError
		| StatementUnexpectedThrow
		| StatementConflict
		| StatementDecodeError
		| StatementEmptyResult
		| StatementThrown<'Not the owner'>,
		SurrealClient
	>
>(true);

// A statement with no required input runs without one.
export const ranWithoutInput = run(everyone);
same<
	typeof ranWithoutInput,
	Effect.Effect<ReadonlyArray<PersonRow>, StatementError, SurrealClient>
>(true);

export const atomic = transaction([
	bind(createPerson, { person: {} as PersonCreate }),
	bind(names),
	bind(guarded, { boundProducer: 'a' }),
]);
same<
	typeof atomic,
	Effect.Effect<
		readonly [PersonRow, ReadonlyArray<string>, string],
		StatementError<'Not the owner'>,
		SurrealClient
	>
>(true);

// Codecs stay values: a result codec is an ordinary schema.
export const resultCodec: Schema.Codec<
	ReadonlyArray<string>,
	unknown
> = names.result;
