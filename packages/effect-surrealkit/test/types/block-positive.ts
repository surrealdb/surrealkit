/**
 * Positive type fixtures for blocks (`block` in
 * `effect-surrealkit/statement`): what a block's result, bindings and
 * messages are, derived from the golden `scripts` modules. Compiled by
 * `tsc -p test/types`.
 */
import type { Effect } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import type { DateTime } from 'surrealdb';

import {
	run,
	type StatementError,
	type SurrealClient,
} from '../../src/client/index.js';
import {
	block,
	type CodecOf,
	eq,
	type Input,
	isNone,
	literal,
	object,
	param,
	type Result,
	select,
	type Thrown,
} from '../../src/statement/index.js';
import { acceptInvite, type pay } from '../blocks.js';
import {
	type AccountId,
	InviteId,
	InviteTable,
	type MembershipId,
	type PaymentId,
	type TeamId,
	type WalletId,
} from '../golden/scripts/index.js';

/** Fails to compile unless `A` and `B` are the same type. */
type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// ---- the ported scripts: a tagged union of what their branches return

type Role = 'admin' | 'member';

same<
	Result<typeof acceptInvite>,
	| {
			readonly kind: 'member';
			readonly membership: MembershipId;
			readonly role: Role;
	  }
	| {
			readonly kind: 'joined';
			readonly membership: MembershipId;
			readonly role: Role;
	  }
>(true);
same<
	Thrown<typeof acceptInvite>,
	| 'Invite not found'
	| 'Invite is not open'
	| 'Invite has expired'
	| 'Team not found'
	| 'Team is full'
>(true);
same<
	Input<(typeof acceptInvite)['params']>,
	{
		readonly invite_id: InviteId;
		readonly now: DateTime;
		readonly account: AccountId;
	}
>(true);

// A branch that throws contributes no result.
same<
	Result<typeof pay>,
	| {
			readonly kind: 'replayed';
			readonly payment: PaymentId;
			readonly amount: number;
	  }
	| { readonly kind: 'declined'; readonly balance: number }
	| {
			readonly kind: 'paid';
			readonly payment: PaymentId;
			readonly balance: number;
	  }
>(true);
same<
	Thrown<typeof pay>,
	'Wallet not found' | 'Not your wallet' | 'Wallet is frozen'
>(true);
same<
	Input<(typeof pay)['params']>,
	{
		readonly wallet_id: WalletId;
		readonly payer: AccountId;
		readonly request: string;
		readonly amount: number;
	}
>(true);

// The client's error channel carries each declared message.
export const ranInvite = run(acceptInvite, {
	invite_id: undefined as unknown as InviteId,
	now: undefined as unknown as DateTime,
	account: undefined as unknown as AccountId,
});
same<
	typeof ranInvite,
	Effect.Effect<
		Result<typeof acceptInvite>,
		StatementError<Thrown<typeof acceptInvite>>,
		SurrealClient
	>
>(true);

// ---- locals

const invite = param('invite_id', SurrealSchema.String);

// A read that may find nothing is NONE in every field until a guard refuses
// NONE; then its fields are what the row holds.
export const narrowing = block()
	.let('invite', () =>
		select(InviteTable, param('id', InviteId)).only().build(),
	)
	.let('maybeTeam', ($) => {
		same<CodecOf<typeof $.invite.team>['Type'], TeamId | undefined>(true);
		return $.invite.team;
	})
	.throwIfNone('invite', 'missing')
	.return(($) => {
		same<CodecOf<typeof $.invite.team>['Type'], TeamId>(true);
		return $.invite.team;
	})
	.build();
same<Result<typeof narrowing>, TeamId>(true);

// An `if` without `else` yields NONE when its condition fails.
export const withoutElse = block()
	.if(
		() => eq(invite, 'x'),
		(arm) => arm.return(() => literal(1)),
	)
	.build();
same<Result<typeof withoutElse>, 1 | undefined>(true);

// An open block or branch yields NONE.
export const open = block()
	.let('a', () => literal(1))
	.build();
same<Result<typeof open>, undefined>(true);
export const openBranch = block()
	.if(
		() => eq(invite, 'x'),
		(arm) => arm.let('b', () => literal(2)),
	)
	.else((arm) => arm.return(() => literal('other')))
	.build();
same<Result<typeof openBranch>, undefined | 'other'>(true);
same<Input<(typeof openBranch)['params']>, { readonly invite_id: string }>(
	true,
);

// A block that only throws never returns.
export const throwing = block().throw('always').build();
same<Result<typeof throwing>, never>(true);
same<Thrown<typeof throwing>, 'always'>(true);

// An object of expressions returns a struct; NONE reads as `undefined`.
export const shaped = block()
	.let('found', () =>
		select(InviteTable)
			.where((row) => isNone(row.team))
			.only()
			.build(),
	)
	.return(($) => object({ id: $.found.id, tag: 'x' }))
	.build();
same<
	Result<typeof shaped>,
	{ readonly id: InviteId | undefined; readonly tag: 'x' }
>(true);
