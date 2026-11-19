/**
 * Every block the test suites build, in one place, so `test/block.test.ts`
 * checks each against the analyzer (`consistent`): the two scripts ported
 * from authored SurrealQL, and the small blocks the unit, client and
 * transaction tests use.
 */
import * as SurrealSchema from '../src/index.js';
import {
	block,
	call,
	create,
	eq,
	gte,
	lt,
	ne,
	object,
	param,
	select,
	update,
} from '../src/statement/index.js';
import {
	AccountId,
	fn_debit,
	fn_membership_key,
	fn_payment_key,
	fn_seats_taken,
	InviteId,
	InviteTable,
	MembershipTable,
	PaymentTable,
	TeamTable,
	WalletId,
	WalletTable,
} from './golden/scripts/index.js';

const account = param('account', AccountId);
const amount = param('amount', SurrealSchema.Int);

/**
 * Accepting a team invite: lock the invite and read it, check it, lock the
 * team, compute the membership key, and either report the membership that
 * already exists or check the seats, create the membership, close the
 * invite and report it.
 */
export const acceptInvite = block()
	.let('invite', () =>
		select(InviteTable, param('invite_id', InviteId))
			.forUpdate()
			.only()
			.build(),
	)
	.throwIfNone('invite', 'Invite not found')
	.throwIf(($) => ne($.invite.status, 'open'), 'Invite is not open')
	.throwIf(
		($) => lt($.invite.expires_at, param('now', SurrealSchema.DateTime)),
		'Invite has expired',
	)
	.let('team_id', ($) => $.invite.team)
	.let('team', ($) => select(TeamTable, $.team_id).forUpdate().only().build())
	.throwIfNone('team', 'Team not found')
	.let('key', ($) => call(fn_membership_key, $.team.id, account))
	.let('existing', ($) =>
		select(MembershipTable)
			.where((row) => eq(row.key, $.key))
			.only()
			.build(),
	)
	.ifNotNone('existing', (arm) =>
		arm.return(($) => ({
			kind: 'member',
			membership: $.existing.id,
			role: $.existing.role,
		})),
	)
	.else((arm) =>
		arm
			.throwIf(
				($) => gte(call(fn_seats_taken, $.team.id), $.team.seats),
				'Team is full',
			)
			.let('created', ($) =>
				create(MembershipTable)
					.content(
						object({
							team: $.team.id,
							account,
							role: $.invite.role,
							key: $.key,
						}),
					)
					.build(),
			)
			.do(($) =>
				update(InviteTable, $.invite.id)
					.merge(object({ status: 'accepted' }))
					.returning('none')
					.build(),
			)
			.return(($) => ({
				kind: 'joined',
				membership: $.created.id,
				role: $.created.role,
			})),
	)
	.build('accept invite');

/**
 * Paying from a wallet with an idempotency key: lock the wallet and read
 * it, check the payer, compute the key, and either replay the payment
 * already made, refuse a frozen wallet, decline when the balance is short,
 * or record the payment and debit the wallet.
 */
export const pay = block()
	.let('wallet', () =>
		select(WalletTable, param('wallet_id', WalletId))
			.forUpdate()
			.only()
			.build(),
	)
	.throwIfNone('wallet', 'Wallet not found')
	.throwIf(
		($) => ne($.wallet.owner, param('payer', AccountId)),
		'Not your wallet',
	)
	.let('key', ($) =>
		call(
			fn_payment_key,
			$.wallet.id,
			param('request', SurrealSchema.String),
		),
	)
	.let('previous', ($) =>
		select(PaymentTable)
			.where((row) => eq(row.key, $.key))
			.only()
			.build(),
	)
	.ifNotNone('previous', (arm) =>
		arm.return(($) => ({
			kind: 'replayed',
			payment: $.previous.id,
			amount: $.previous.amount,
		})),
	)
	.elseIf(
		($) => $.wallet.frozen,
		(arm) => arm.throw('Wallet is frozen'),
	)
	.elseIf(
		($) => lt($.wallet.balance, amount),
		(arm) =>
			arm.return(($) => ({
				kind: 'declined',
				balance: $.wallet.balance,
			})),
	)
	.else((arm) =>
		arm
			.let('balance', ($) => call(fn_debit, $.wallet.balance, amount))
			.let('payment', ($) =>
				create(PaymentTable)
					.content(
						object({ wallet: $.wallet.id, amount, key: $.key }),
					)
					.build(),
			)
			.do(($) =>
				update(WalletTable, $.wallet.id)
					.merge(object({ balance: $.balance }))
					.returning('none')
					.build(),
			)
			.return(($) => ({
				kind: 'paid',
				payment: $.payment.id,
				balance: $.balance,
			})),
	)
	.build('pay');

const n = param('n', SurrealSchema.Int);

/** A guard, then a value: the smallest script. */
export const guarded = block()
	.throwIf(() => eq(n, 0), 'zero')
	.return(() => ({ n }))
	.build('guarded');

/** An `if` without `else`: NONE when its condition fails. */
export const unlessNegative = block()
	.if(
		() => gte(n, 0),
		(arm) => arm.return(() => ({ sign: 'positive', n })),
	)
	.build('unless negative');

/** A block that only binds a local: NONE. */
export const onlyLocals = block()
	.let('debited', () => call(fn_debit, n, n))
	.build('only locals');

/** A block that always throws. */
export const onlyThrows = block().throw('always').build('only throws');

/** Branches inside a branch, and a branch's own local. */
export const nested = block()
	.let('first', () => n)
	.if(
		($) => gte($.first, 10),
		(arm) =>
			arm
				.if(
					($) => gte($.first, 100),
					(inner) => inner.return(() => ({ size: 'huge' })),
				)
				.else((inner) => inner.return(() => ({ size: 'big' }))),
	)
	.elseIf(
		($) => eq($.first, 0),
		(arm) => arm.throw('zero'),
	)
	.else((arm) =>
		arm
			.let('second', ($) => call(fn_debit, $.first, 1))
			.return(($) => ({ size: 'small', less: $.second })),
	)
	.build('nested');

/** A read whose fields are NONE until a guard refuses NONE. */
export const inviteStatus = block()
	.let('invite', () =>
		select(InviteTable, param('invite_id', InviteId)).only().build(),
	)
	.return(($) => ({ status: $.invite.status, team: $.invite.team }))
	.build('invite status', { writes: false });

/**
 * Debiting a wallet, then refusing a negative balance: a guard after a
 * write, which undoes the write even when the block runs on its own.
 */
export const withdraw = block()
	.let('wallet', () =>
		select(WalletTable, param('wallet_id', WalletId))
			.forUpdate()
			.only()
			.build(),
	)
	.throwIfNone('wallet', 'Wallet not found')
	.let('debited', ($) =>
		update(WalletTable, $.wallet.id)
			.merge(
				object({ balance: call(fn_debit, $.wallet.balance, amount) }),
			)
			.build(),
	)
	.throwIfNone('debited', 'Wallet not found')
	.throwIf(($) => lt($.debited.balance, 0), 'Insufficient funds')
	.return(($) => ({ balance: $.debited.balance }))
	.build('withdraw');

/** A block as a local of another: `pay`, then the wallet as it is after. */
export const payAndRead = block()
	.let('outcome', () => pay)
	.let('wallet_after', () =>
		select(WalletTable, param('wallet_id', WalletId)).only().build(),
	)
	.return(($) => ({ outcome: $.outcome, balance: $.wallet_after.balance }))
	.build('pay and read');
