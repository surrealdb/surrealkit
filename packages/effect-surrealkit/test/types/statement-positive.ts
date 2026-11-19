/**
 * Positive type fixtures for `effect-surrealkit/statement` and
 * `effect-surrealkit/client`: what the builders derive from the golden
 * generated modules. Compiled by `tsc -p test/types`.
 */
import { type Brand, type Effect, Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import type { DateTime } from 'surrealdb';

import {
	type Bound,
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
	type StatementWriteOutcomeError,
	type SurrealClient,
	transaction,
} from '../../src/client/index.js';
import {
	and,
	at,
	type Codec,
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
	type Params,
	param,
	part,
	type Result,
	readAs,
	relate,
	type Statement,
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
import * as Members from '../golden/members/index.js';

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

// An open object (`address` keeps undeclared keys) accepts extra keys at
// any depth; a declared child object accepts its own fields.
export const mergeAddress = update(PersonTable, param('id', PersonId))
	.merge(
		param(
			'changes',
			Schema.Struct({
				address: Schema.Struct({
					city: SurrealSchema.String,
					floor: Schema.Struct({ level: SurrealSchema.Int }),
				}),
				phones: Schema.Array(
					Schema.Struct({ kind: Schema.Literals(['home', 'work']) }),
				),
			}),
		),
	)
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
		| StatementWriteOutcomeError
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

// The input checks accept what the statement reads: a member of a literal
// union written inline, an omitted binding whose codec accepts NONE, and a
// value held in a variable that has more properties than the statement reads.
const byRole = select(PersonTable)
	.where((row) => eq(row.role, param('role', PersonTable.select.fields.role)))
	.where((row) =>
		eq(
			row.nickname,
			param('nickname', Schema.UndefinedOr(SurrealSchema.String)),
		),
	)
	.build();
same<
	Input<(typeof byRole)['params']>,
	{
		readonly role: 'owner' | 'member' | 'admin';
		readonly nickname?: string | undefined;
	}
>(true);
export const ranByRole = run(byRole, { role: 'admin' });
export const boundByRole = bind(byRole, { role: 'owner', nickname: 'x' });
const wider = { role: 'member' as const, unread: 1 };
export const ranWider = run(byRole, wider);
// A trailing optional argument of a function may be left out.
export const greeting = call(fn_greet, 'Ann', literal('Hi'));

// Generic code forwards a statement's input to `run` and `bind` unchanged:
// whatever checks the input of a concrete statement must accept `Input<P>`.
export const forwardRun = <P extends Params, R extends Codec, T extends string>(
	statement: Statement<P, R, T>,
	input: Input<P>,
): Effect.Effect<R['Type'], StatementError<T>, SurrealClient> =>
	run(statement, input);
export const forwardBind = <
	P extends Params,
	R extends Codec,
	T extends string,
>(
	statement: Statement<P, R, T>,
	input: Input<P>,
): Bound<R, T> => bind(statement, input);
export const forwardTransaction = <P extends Params, R extends Codec>(
	statement: Statement<P, R>,
	input: Input<P>,
) => transaction([bind(statement, input), bind(names)]);
same<
	Effect.Success<ReturnType<typeof forwardTransaction>>,
	readonly [unknown, ReadonlyArray<string>]
>(true);

// Codecs stay values: a result codec is an ordinary schema.
export const resultCodec: Schema.Codec<
	ReadonlyArray<string>,
	unknown
> = names.result;

// ---- readAs(table): the generated row a restricted session reads.

export const asSession = select(readAs(Members.MemberTable)).build();
same<
	Result<typeof asSession>,
	ReadonlyArray<typeof Members.MemberVisible.Type>
>(true);
export type SessionRow = Result<typeof asSession>[number];
// A field some session may not select may be absent; the others may not.
same<SessionRow['email'], string | undefined>(true);
same<SessionRow['handle'], string>(true);
same<SessionRow['profile']['phone'], string | undefined>(true);
same<SessionRow['profile']['bio'], string>(true);
// Conditions and projections read the same row.
export const sessionEmails = select(readAs(Members.MemberTable))
	.where((row) => isNone(row.email))
	.project((row) => ({ email: row.email, handle: row.handle }))
	.build();
same<
	Result<typeof sessionEmails>,
	ReadonlyArray<{
		readonly email: string | undefined;
		readonly handle: string;
	}>
>(true);
// A refinement reaches the visible row too, keeping each field's presence.
export const RefinedMembers = SurrealSchema.refine(Members.MemberTable, {
	email: (s) => s.pipe(Schema.brand('Email')),
});
export type RefinedVisible = (typeof RefinedMembers.visible)['Type'];
same<RefinedVisible['email'], (string & Brand.Brand<'Email'>) | undefined>(
	true,
);
same<
	(typeof RefinedMembers.select)['Type']['email'],
	string & Brand.Brand<'Email'>
>(true);

// ---- SELECT by a unique index: exactly its fields, at most one row.

export const byHandle = select(Members.MemberTable, 'member_handle', {
	handle: param('handle', SurrealSchema.String),
}).build();
same<Result<typeof byHandle>, Members.MemberRow | undefined>(true);
same<Input<(typeof byHandle)['params']>, { readonly handle: string }>(true);
// A composite index takes every field; a constant is inlined.
export const byRegionNumber = select(
	Members.MemberTable,
	'member_region_number',
	{ region: 'north', number: param('number', SurrealSchema.Int) },
).build();
same<Input<(typeof byRegionNumber)['params']>, { readonly number: number }>(
	true,
);
// A nested field by its path; through the row a restricted session reads.
export const byBio = select(readAs(Members.MemberTable), 'member_bio', {
	'profile.bio': param('bio', SurrealSchema.String),
})
	.project((row) => ({ handle: row.handle, email: row.email }))
	.build();
same<
	Result<typeof byBio>,
	{ readonly handle: string; readonly email: string | undefined } | undefined
>(true);
