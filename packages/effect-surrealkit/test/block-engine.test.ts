/**
 * The two scripts of `test/blocks.ts` against a real engine: the schema of
 * `test/fixtures/scripts/schema` is applied to a disposable database, each
 * branch's result is decoded strictly by the block's derived codec, and each
 * guard's message arrives as a typed `StatementThrown`. Runs on whichever
 * engine `engine.ts` provides (SurrealDB 4.0.1 embedded, or 3.3.4 with
 * `SURREAL_BIN`).
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Effect, Schema } from 'effect';
import { DateTime, RecordId } from 'surrealdb';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	bind,
	type Client,
	make as makeClient,
	StatementScopeError,
	StatementThrown,
} from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import { eq, param, part, select, sequence } from '../src/statement/index.js';
import { acceptInvite, pay, payAndRead, withdraw } from './blocks.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';
import {
	AccountId,
	InviteId,
	InviteTable,
	MembershipTable,
	PaymentId,
	PaymentTable,
	WalletId,
	WalletTable,
} from './golden/scripts/index.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] block engine suite skipped: ${engineSkipReason}`,
	);
}

const schema = readFileSync(
	join(import.meta.dirname, 'fixtures', 'scripts', 'schema', 'scripts.surql'),
	'utf8',
);

const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
	within(Effect.runPromise(effect), 20_000);
const failure = <A, E>(effect: Effect.Effect<A, E>): Promise<E> =>
	within(Effect.runPromise(Effect.flip(effect)), 20_000);

const account = (key: string) =>
	Schema.decodeUnknownSync(AccountId)(new RecordId('account', key));
const invite = (key: string) =>
	Schema.decodeUnknownSync(InviteId)(new RecordId('invite', key));
const wallet = (key: string) =>
	Schema.decodeUnknownSync(WalletId)(new RecordId('wallet', key));

const readInvite = select(InviteTable, param('id', InviteId)).only().build();
const readWallet = select(WalletTable, param('id', WalletId)).only().build();
const membershipsOf = select(MembershipTable)
	.where((row) => eq(row.account, param('member', AccountId)))
	.build();
const paymentByKey = select(PaymentTable)
	.where((row) => eq(row.key, param('key', SurrealSchema.String)))
	.build();
const readPayment = select(PaymentTable, param('payment', PaymentId))
	.only()
	.build();

describe.skipIf(!engineConfigured)('blocks on a disposable engine', () => {
	let engine: Engine;
	let client: Client;

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		await engine.db.query(schema);
		await engine.db.query(`
			CREATE account:ann SET name = 'Ann';
			CREATE account:bob SET name = 'Bob';
			CREATE account:cy SET name = 'Cy';
			CREATE account:dee SET name = 'Dee';
			CREATE team:red SET name = 'Red', seats = 2;
			CREATE team:blue SET name = 'Blue', seats = 1;
			CREATE membership:seed SET team = team:blue, account = account:cy,
				role = 'member', key = fn::membership_key(team:blue, account:cy);
			CREATE invite:open SET team = team:red, role = 'admin', status = 'open',
				expires_at = d'2100-01-01T00:00:00Z';
			CREATE invite:again SET team = team:red, role = 'member', status = 'open',
				expires_at = d'2100-01-01T00:00:00Z';
			CREATE invite:revoked SET team = team:red, role = 'member', status = 'revoked',
				expires_at = d'2100-01-01T00:00:00Z';
			CREATE invite:expired SET team = team:red, role = 'member', status = 'open',
				expires_at = d'2000-01-01T00:00:00Z';
			CREATE invite:ghost SET team = team:ghost, role = 'member', status = 'open',
				expires_at = d'2100-01-01T00:00:00Z';
			CREATE invite:blue SET team = team:blue, role = 'member', status = 'open',
				expires_at = d'2100-01-01T00:00:00Z';
			CREATE wallet:ann SET owner = account:ann, balance = 100, frozen = false;
			CREATE wallet:ice SET owner = account:ann, balance = 100, frozen = true;
			CREATE wallet:low SET owner = account:ann, balance = 5, frozen = false;
		`);
		console.info(
			`[effect-surrealkit] blocks: ${engine.mode} engine ${engine.version}`,
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	const accepting = (inviteKey: string, accountKey: string) => ({
		invite_id: invite(inviteKey),
		now: new DateTime(),
		account: account(accountKey),
	});

	describe('accepting an invite', () => {
		it('joins the team from an open invite and closes the invite, atomically', async () => {
			const [joined, closed] = await ok(
				client.transaction([
					bind(acceptInvite, accepting('open', 'ann')),
					bind(readInvite, { id: invite('open') }),
				]),
			);
			expect(joined).toMatchObject({ kind: 'joined', role: 'admin' });
			expect(
				joined.kind === 'joined' && joined.membership,
			).toBeInstanceOf(RecordId);
			expect(closed?.status).toBe('accepted');
			const memberships = await ok(
				client.run(membershipsOf, { member: account('ann') }),
			);
			expect(memberships).toHaveLength(1);
			expect(memberships[0]).toMatchObject({
				id: joined.membership,
				key: 'team:red/account:ann',
				role: 'admin',
			});
		});

		it('reports the membership that already exists, and leaves the invite open', async () => {
			const [existing] = await ok(
				client.transaction([
					bind(acceptInvite, accepting('again', 'ann')),
				]),
			);
			const [membership] = await ok(
				client.run(membershipsOf, { member: account('ann') }),
			);
			expect(existing).toStrictEqual({
				kind: 'member',
				membership: membership?.id,
				role: 'admin',
			});
			expect(
				(await ok(client.run(readInvite, { id: invite('again') })))
					?.status,
			).toBe('open');
		});

		it.each([
			['nope', 'ann', 'Invite not found'],
			['revoked', 'ann', 'Invite is not open'],
			['expired', 'ann', 'Invite has expired'],
			['ghost', 'ann', 'Team not found'],
			['blue', 'dee', 'Team is full'],
		] as const)('invite:%s for account:%s throws %j as a typed error, and writes nothing', async (inviteKey, accountKey, message) => {
			const error = await failure(
				client.transaction([
					bind(acceptInvite, accepting(inviteKey, accountKey)),
				]),
			);
			expect(error).toBeInstanceOf(StatementThrown);
			expect(error).toMatchObject({
				_tag: 'StatementThrown',
				statement: 'accept invite',
				reason: message,
			});
			if (error._tag === 'StatementThrown') {
				// The reason is typed as the block's messages.
				const reason: (typeof acceptInvite.throws)[number] =
					error.reason;
				expect(acceptInvite.throws).toContain(reason);
			}
			expect(
				await ok(
					client.run(membershipsOf, { member: account(accountKey) }),
				),
			).toHaveLength(accountKey === 'ann' ? 1 : 0);
		});

		it('raises the same typed error from a single run', async () => {
			expect(
				await failure(
					client.run(acceptInvite, accepting('revoked', 'bob')),
				),
			).toMatchObject({
				_tag: 'StatementThrown',
				reason: 'Invite is not open',
			});
		});
	});

	const paying = (
		walletKey: string,
		request: string,
		amount: number,
		payer = 'ann',
	) => ({
		wallet_id: wallet(walletKey),
		payer: account(payer),
		request,
		amount,
	});

	describe('paying from a wallet', () => {
		it('records a payment and debits the wallet, then replays it for the same request', async () => {
			const [paid, after] = await ok(
				client.transaction([
					bind(pay, paying('ann', 'r1', 30)),
					bind(readWallet, { id: wallet('ann') }),
				]),
			);
			expect(paid).toMatchObject({ kind: 'paid', balance: 70 });
			expect(after?.balance).toBe(70);
			const [replayed] = await ok(
				client.transaction([bind(pay, paying('ann', 'r1', 30))]),
			);
			expect(replayed).toStrictEqual({
				kind: 'replayed',
				payment: paid.kind === 'paid' ? paid.payment : undefined,
				amount: 30,
			});
			expect(
				(await ok(client.run(readWallet, { id: wallet('ann') })))
					?.balance,
			).toBe(70);
		});

		it('declines when the balance is short, writing nothing', async () => {
			expect(
				await ok(client.run(pay, paying('low', 'r2', 10))),
			).toStrictEqual({ kind: 'declined', balance: 5 });
			expect(
				await ok(client.run(paymentByKey, { key: 'wallet:low/r2' })),
			).toStrictEqual([]);
		});

		it.each([
			['ice', 'ann', 'Wallet is frozen'],
			['ann', 'bob', 'Not your wallet'],
			['nope', 'ann', 'Wallet not found'],
		] as const)('wallet:%s paid by account:%s throws %j as a typed error', async (walletKey, payer, message) => {
			expect(
				await failure(
					client.transaction([
						bind(pay, paying(walletKey, 'r3', 1, payer)),
					]),
				),
			).toMatchObject({
				_tag: 'StatementThrown',
				statement: 'pay',
				reason: message,
			});
		});
	});

	describe('on its own', () => {
		it('a block is one statement, all or nothing: a guard after a write undoes the write', async () => {
			const before = (
				await ok(client.run(readWallet, { id: wallet('low') }))
			)?.balance;
			expect(
				await failure(
					client.run(withdraw, {
						wallet_id: wallet('low'),
						amount: 100,
					}),
				),
			).toMatchObject({
				_tag: 'StatementThrown',
				statement: 'withdraw',
				reason: 'Insufficient funds',
			});
			expect(
				(await ok(client.run(readWallet, { id: wallet('low') })))
					?.balance,
			).toBe(before);
			expect(
				await ok(
					client.run(withdraw, {
						wallet_id: wallet('low'),
						amount: 1,
					}),
				),
			).toStrictEqual({ balance: (before ?? 0) - 1 });
		});

		it('a block can be a local of another block', async () => {
			const result = await ok(
				client
					.run(payAndRead, paying('ice', 'r9', 1))
					.pipe(Effect.flip),
			);
			expect(result).toMatchObject({ reason: 'Wallet is frozen' });
			const read = await ok(
				client.run(payAndRead, paying('low', 'r9', 1000)),
			);
			expect(read).toStrictEqual({
				outcome: { kind: 'declined', balance: read.balance },
				balance: read.balance,
			});
		});
	});

	describe('inside a transaction', () => {
		it('a block can be followed by another statement, and by a block that LETs the same names: it never writes RETURN, at which SurrealDB 3 would end the transaction', async () => {
			expect(pay.sql).toContain('LET $key = ');
			expect(acceptInvite.sql).toContain('LET $key = ');
			const results = await ok(
				client.transaction([
					bind(pay, paying('low', 'r4', 10)),
					bind(readWallet, { id: wallet('low') }),
					bind(acceptInvite, accepting('again', 'bob')),
				]),
			);
			expect(results[0]).toStrictEqual({
				kind: 'declined',
				balance: results[1]?.balance,
			});
			expect(results[2]).toMatchObject({
				kind: 'joined',
				role: 'member',
			});
			const [membership] = await ok(
				client.run(membershipsOf, { member: account('bob') }),
			);
			expect(membership?.key).toBe('team:red/account:bob');
		});

		it('refuses the same block bound twice to different values, as for any statement', async () => {
			expect(
				await failure(
					client.transaction([
						bind(pay, paying('low', 'r8', 1)),
						bind(pay, paying('low', 'r9', 1)),
					]),
				),
			).toMatchObject({
				_tag: 'StatementScopeError',
				binding: 'request',
			});
		});

		it('shares none of its locals: a statement may bind a name the block LETs, and each reads its own value', async () => {
			expect(pay.defines).toEqual([]);
			const [before, paid, after] = await ok(
				client.transaction([
					bind(paymentByKey, { key: 'wallet:ann/r1' }),
					bind(pay, paying('ann', 'r6', 1)),
					bind(paymentByKey, { key: 'wallet:ann/r1' }),
				]),
			);
			expect(before).toHaveLength(1);
			expect(before[0]?.amount).toBe(30);
			expect(paid).toMatchObject({ kind: 'paid', balance: 69 });
			// The block's `$key` (wallet:ann/r6) did not replace the binding.
			expect(after).toStrictEqual(before);
		});

		it('refuses a block whose binding another statement LETs, sending nothing', async () => {
			const setsAmount = sequence(
				part({ sql: 'LET $amount = 1000;', defines: ['amount'] }),
			).define({
				name: 'sets amount',
				params: {},
				result: Schema.Undefined,
			});
			const error = await failure(
				client.transaction([
					bind(setsAmount),
					bind(pay, paying('ann', 'r7', 1)),
				]),
			);
			expect(error).toBeInstanceOf(StatementScopeError);
			expect(error).toMatchObject({
				statement: 'pay',
				binding: 'amount',
			});
			expect(
				await ok(client.run(paymentByKey, { key: 'wallet:ann/r7' })),
			).toStrictEqual([]);
		});

		it('shares no name the block LETs only inside a branch: each statement reads its own value', async () => {
			const [first] = await ok(
				client.run(paymentByKey, { key: 'wallet:ann/r1' }),
			);
			const earlier = first?.id;
			expect(earlier).toBeInstanceOf(RecordId);
			expect(pay.defines).not.toContain('payment');
			const [before, paid, after] = await ok(
				client.transaction([
					bind(readPayment, { payment: earlier as PaymentId }),
					bind(pay, paying('ann', 'r8', 5)),
					bind(readPayment, { payment: earlier as PaymentId }),
				]),
			);
			expect(before?.id).toEqual(earlier);
			expect(paid).toMatchObject({ kind: 'paid', balance: 64 });
			expect(paid.kind === 'paid' && paid.payment).not.toEqual(earlier);
			// The branch's `$payment` did not replace the binding.
			expect(after?.id).toEqual(earlier);
		});
	});
});
