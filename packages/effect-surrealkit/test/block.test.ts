/**
 * Blocks without an engine: every block the suites build agrees with the
 * analyzer (`consistent`), what a block renders, how its result decodes, and
 * what it refuses when it is built. `test/block-engine.test.ts` runs the
 * scripts against SurrealDB.
 */
import { Effect, Schema } from 'effect';
import { DateTime, RecordId } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import { StatementThrown } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	block,
	codecOf,
	eq,
	isStatement,
	literal,
	make,
	object,
	param,
	part,
	select,
	sequence,
	sqlOf,
	update,
} from '../src/statement/index.js';
import { fake, thrown } from '../src/testing/index.js';
import * as blocks from './blocks.js';
import { consistent } from './consistent.js';
import {
	InviteId,
	InviteTable,
	type MembershipId,
	type TeamId,
} from './golden/scripts/index.js';
import * as typeFixtures from './types/block-positive.js';

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
	Effect.runPromise(effect);
const failureOf = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
	Effect.runPromise(Effect.flip(effect));

const built = [
	...Object.entries(blocks),
	...Object.entries(typeFixtures).filter(([, value]) => isStatement(value)),
];

describe('every block the suites build', () => {
	it('includes the fixtures of the type tests', () => {
		expect(built.map(([name]) => name)).toEqual(
			expect.arrayContaining([
				'acceptInvite',
				'pay',
				'narrowing',
				'shaped',
			]),
		);
	});

	it.each(
		built,
	)('%s: the analyzer reads what the builder declares', (_, statement) => {
		consistent(statement as Parameters<typeof consistent>[0]);
	});
});

describe('what a block renders', () => {
	it('one statement: a block of the locals and guards, ending with the IF whose branches end with their value', () => {
		expect(blocks.acceptInvite.sql).toBe(
			[
				'{',
				'\tLET $invite = (SELECT * FROM ONLY $invite_id FOR UPDATE);',
				'\tIF $invite = NONE { THROW s"Invite not found" };',
				'\tIF $invite.status != s"open" { THROW s"Invite is not open" };',
				'\tIF $invite.expires_at < $now { THROW s"Invite has expired" };',
				'\tLET $team_id = $invite.team;',
				'\tLET $team = (SELECT * FROM ONLY $team_id FOR UPDATE);',
				'\tIF $team = NONE { THROW s"Team not found" };',
				'\tLET $key = fn::membership_key($team.id, $account);',
				'\tLET $existing = (SELECT * FROM ONLY ⟨membership⟩ WHERE key = $key LIMIT 1);',
				'\tIF $existing != NONE { { kind: s"member", membership: $existing.id, role: $existing.role } }' +
					' ELSE { IF fn::seats_taken($team.id) >= $team.seats { THROW s"Team is full" };' +
					' LET $created = (CREATE ONLY ⟨membership⟩ CONTENT { team: $team.id, account: $account, role: $invite.role, key: $key } RETURN AFTER);' +
					' UPDATE ONLY $invite.id MERGE { status: s"accepted" } RETURN NONE;' +
					' { kind: s"joined", membership: $created.id, role: $created.role } }',
				'};',
			].join('\n'),
		);
		expect(blocks.acceptInvite).toMatchObject({
			slots: 1,
			defines: [],
			throws: [
				'Invite has expired',
				'Invite is not open',
				'Invite not found',
				'Team is full',
				'Team not found',
			],
			writes: true,
			transactional: false,
		});
		expect(Object.keys(blocks.acceptInvite.params).sort()).toEqual([
			'account',
			'invite_id',
			'now',
		]);
	});

	it('never writes RETURN, so a block can be followed by other statements in a transaction', () => {
		for (const [, statement] of built) {
			if (!isStatement(statement)) continue;
			expect(statement.sql).not.toMatch(
				/\bRETURN\b(?! (?:AFTER|NONE|BEFORE))/u,
			);
		}
	});

	it('writes an empty branch as NONE, and an if without else as one', () => {
		expect(
			consistent(
				block()
					.if(
						() => eq(param('n', SurrealSchema.Int), 1),
						(arm) => arm,
					)
					.build(),
			).sql,
		).toBe('IF $n = 1 { NONE };');
		expect(blocks.unlessNegative.sql).toBe(
			'IF $n >= 0 { { sign: s"positive", n: $n } };',
		);
		expect(blocks.onlyThrows.sql).toBe('THROW s"always";');
	});

	it('nests branches inside a branch, and a block inside a block', () => {
		expect(blocks.nested.sql).toBe(
			[
				'{',
				'\tLET $first = $n;',
				'\tIF $first >= 10 { IF $first >= 100 { { size: s"huge" } } ELSE { { size: s"big" } } }' +
					' ELSE IF $first = 0 { THROW s"zero" }' +
					' ELSE { LET $second = fn::debit($first, 1); { size: s"small", less: $second } }',
				'};',
			].join('\n'),
		);
		expect(blocks.nested.defines).toEqual([]);
		expect(blocks.payAndRead.sql).toContain(
			`LET $outcome = (${blocks.pay.sql.slice(0, -1)});`,
		);
		expect(blocks.payAndRead.throws).toEqual(blocks.pay.throws);
		expect(Object.keys(blocks.payAndRead.params).sort()).toEqual(
			Object.keys(blocks.pay.params).sort(),
		);
	});

	it('knows what it writes: certainly, when a statement it runs does; otherwise as declared, or unknown', () => {
		expect(blocks.pay.writes).toBe(true);
		expect(blocks.inviteStatus.writes).toBe(false);
		expect(blocks.guarded.writes).toBeUndefined();
	});
});

describe('locals', () => {
	it('reads a row through its fields, each NONE while the row may be', () => {
		const status = block()
			.let('invite', () =>
				select(InviteTable, param('invite_id', InviteId))
					.only()
					.build(),
			)
			.return(($) => {
				expect(sqlOf($.invite)).toBe('$invite');
				expect(sqlOf($.invite.team)).toBe('$invite.team');
				const decode = Schema.decodeUnknownSync(codecOf($.invite.team));
				expect(decode(undefined)).toBeUndefined();
				expect(decode(new RecordId('team', 'a'))).toEqual(
					new RecordId('team', 'a'),
				);
				return $.invite.team;
			})
			.build();
		consistent(status);
		expect(
			Schema.decodeUnknownSync(status.result)(undefined),
		).toBeUndefined();
	});

	it('reads a row without NONE once a guard has refused NONE', () => {
		const team = block()
			.let('invite', () =>
				select(InviteTable, param('invite_id', InviteId))
					.only()
					.build(),
			)
			.throwIfNone('invite', 'missing')
			.return(($) => {
				const decode = Schema.decodeUnknownSync(codecOf($.invite.team));
				expect(() => decode(undefined)).toThrow();
				expect(() =>
					Schema.decodeUnknownSync(codecOf($.invite.expires_at))(
						new DateTime(),
					),
				).not.toThrow();
				return $.invite.team;
			})
			.build();
		consistent(team);
	});

	it('refuses a local defined twice, one named like a binding, and names SurrealDB sets', () => {
		const one = block().let('a', () => literal(1));
		expect(() => one.let('a' as never, () => literal(2))).toThrow(
			'local `$a` is already defined in this block',
		);
		const shadow = block()
			.let('x', () => literal(1))
			.throwIf(() => eq(param('x', SurrealSchema.Int), 1), 'one');
		expect(() => shadow.build('shadow')).toThrow(
			'block `shadow` reads `$x` as a binding and defines a local of the same name, which would shadow it; rename the local',
		);
		const inArm = block().if(
			() => eq(param('y', SurrealSchema.Int), 1),
			(arm) =>
				arm
					.let('z', () => literal(1))
					.return(() => ({ z: literal(1) })),
		);
		expect(() =>
			inArm
				.else((arm) =>
					arm.return(() => ({ z: param('z', SurrealSchema.Int) })),
				)
				.build('arm'),
		).toThrow('block `arm` reads `$z` as a binding');
		for (const name of [
			'auth',
			'session',
			'value',
			'this',
			'parent',
			'event',
		]) {
			expect(() => block().let(name as never, () => literal(1))).toThrow(
				`\`$${name}\``,
			);
		}
		expect(() => block().let('a-b' as never, () => literal(1))).toThrow(
			'is not a parameter name',
		);
	});

	it('takes an expression or a statement of one result that defines nothing', () => {
		const two = sequence(
			part({ sql: 'LET $q = 1;', defines: ['q'] }),
			part({ sql: 'RETURN $q;', free: ['q'] }),
		).define({
			params: {},
			result: SurrealSchema.Int,
		});
		expect(() => block().let('a', () => two as never)).toThrow(
			'the value of `$a` is a statement of one result that defines nothing; `sequence` returns 2 results, defines `$q`',
		);
		expect(() => block().let('a', () => 'text' as never)).toThrow(
			'the value of `$a` is an expression or a built statement',
		);
	});

	it('refuses a statement that writes RETURN, which would end the block', () => {
		for (const sql of [
			'RETURN 1;',
			'IF true { RETURN 1 } ELSE { 2 };',
			'/* RETURN */ IF true THEN RETURN 1 END;',
		]) {
			const authored = make({
				sql,
				params: {},
				result: SurrealSchema.Int,
			});
			expect(() => block().let('a', () => authored)).toThrow(
				'the value of `$a` is a statement without RETURN',
			);
		}
		const commented = make({
			sql: "SELECT VALUE 'RETURN' FROM ONLY $id -- RETURN\n;",
			params: { id: InviteId },
			result: SurrealSchema.String,
		});
		expect(
			consistent(
				block()
					.let('a', () => commented)
					.return(($) => $.a)
					.build(),
			).sql,
		).toBe(
			"{\n\tLET $a = (SELECT VALUE 'RETURN' FROM ONLY $id -- RETURN\n);\n\t$a\n};",
		);
	});

	it('refuses throwIfNone and ifNotNone of a name that is not a local', () => {
		expect(() => block().throwIfNone('nope' as never, 'x')).toThrow(
			'`$nope` is not a local of this block',
		);
		expect(() => block().ifNotNone('nope' as never, (arm) => arm)).toThrow(
			'`$nope` is not a local of this block',
		);
	});
});

describe('steps', () => {
	it('runs a statement for its effect only when its result is NONE', () => {
		const id = param('invite_id', InviteId);
		expect(() =>
			block().do(() => select(InviteTable).build() as never),
		).toThrow('a step run for its effect returns NONE');
		expect(
			consistent(
				block()
					.do(() =>
						update(InviteTable, id)
							.merge(object({ status: 'revoked' }))
							.returning('none')
							.build(),
					)
					.build(),
			).sql,
		).toBe(
			'{\n\tUPDATE ONLY $invite_id MERGE { status: s"revoked" } RETURN NONE\n};',
		);
	});

	it('refuses an empty block, an empty message and a constant returned on its own', () => {
		expect(() => block().build('empty')).toThrow(
			'block `empty` has no steps',
		);
		expect(() => block().throw('')).toThrow(
			'a THROW message is a non-empty string',
		);
		expect(() => block().return(() => 1 as never)).toThrow(
			'inline a constant with `literal`',
		);
	});

	it('refuses `writes: false` for a block that runs a write', () => {
		expect(() =>
			block()
				.do(() =>
					update(InviteTable, param('invite_id', InviteId))
						.merge(object({ status: 'revoked' }))
						.returning('none')
						.build(),
				)
				.build('revoke', { writes: false }),
		).toThrow(
			'block `revoke` is declared not to write, but a statement it runs writes',
		);
	});

	it('writes an object of identifiers, and refuses another key or no key', () => {
		expect(sqlOf(object({ a: 1, b: 'x', c: null }))).toBe(
			'{ a: 1, b: s"x", c: NULL }',
		);
		expect(() => object({})).toThrow('at least one field');
		expect(() => object({ 'a b': 1 })).toThrow(
			'`a b` is not an object key a block writes',
		);
	});
});

describe('FOR UPDATE', () => {
	it('reads one record from a binding or a local', () => {
		expect(
			select(InviteTable, param('invite_id', InviteId))
				.forUpdate()
				.only()
				.build().sql,
		).toBe('SELECT * FROM ONLY $invite_id FOR UPDATE;');
	});

	it('refuses any other target expression, which SurrealDB refuses too', () => {
		expect(() =>
			select(
				InviteTable,
				literal(new RecordId('invite', 'a') as InviteId, InviteId),
			).forUpdate(),
		).toThrow(
			'FOR UPDATE reads one record from a binding or a local (`$name`), and SurrealDB refuses `r"invite:a"` as its target; LET the id in a block first',
		);
		block()
			.let('invite', () =>
				select(InviteTable, param('invite_id', InviteId))
					.only()
					.build(),
			)
			.throwIfNone('invite', 'missing')
			.return(($) => {
				expect(() =>
					select(InviteTable, $.invite.id).forUpdate(),
				).toThrow('SurrealDB refuses `$invite.id` as its target');
				return $.invite.id;
			});
	});
});

describe('results', () => {
	const membership = new RecordId('membership', 'm');
	type Bindings = Readonly<Record<string, unknown>>;
	// A scripted reply is the last result, as the SDK returns it.
	const client = fake({
		'accept invite': (bindings: Bindings) =>
			bindings.account instanceof RecordId &&
			bindings.account.id === 'full'
				? thrown('Team is full')
				: { kind: 'joined', membership, role: 'member' },
		'unless negative': (bindings: Bindings) =>
			bindings.n === -1 ? undefined : { sign: 'positive', n: bindings.n },
	});
	const input = (account: string) => ({
		invite_id: new RecordId('invite', 'i') as InviteId,
		now: new DateTime(),
		account: new RecordId('account', account) as never,
	});

	it('decode with the union of what the branches return', async () => {
		expect(
			await run(client.run(blocks.acceptInvite, input('ann'))),
		).toEqual({
			kind: 'joined',
			membership,
			role: 'member',
		});
		const decode = Schema.decodeUnknownSync(blocks.acceptInvite.result);
		expect(
			decode({
				kind: 'member',
				membership: new RecordId('membership', 'x') as MembershipId,
				role: 'admin',
			}),
		).toMatchObject({ kind: 'member' });
		expect(() =>
			decode({ kind: 'paid', membership, role: 'member' }),
		).toThrow();
		expect(() => decode(undefined)).toThrow();
	});

	it('include NONE when a branch neither returns nor throws', async () => {
		expect(await run(client.run(blocks.unlessNegative, { n: -1 }))).toBe(
			undefined,
		);
		expect(await run(client.run(blocks.unlessNegative, { n: 2 }))).toEqual({
			sign: 'positive',
			n: 2,
		});
		expect(
			Schema.decodeUnknownSync(blocks.onlyLocals.result)(undefined),
		).toBe(undefined);
		expect(() =>
			Schema.decodeUnknownSync(blocks.onlyThrows.result)(undefined),
		).toThrow();
	});

	it('raise a declared message as a typed error', async () => {
		const error = await failureOf(
			client.run(blocks.acceptInvite, input('full')),
		);
		expect(error).toBeInstanceOf(StatementThrown);
		expect(error).toMatchObject({
			statement: 'accept invite',
			reason: 'Team is full',
		});
	});

	it('keep a projected id branded as its table', () => {
		const team = new RecordId('team', 't') as TeamId;
		expect(
			Schema.decodeUnknownSync(blocks.inviteStatus.result)({
				status: 'open',
				team,
			}),
		).toEqual({ status: 'open', team });
	});
});
