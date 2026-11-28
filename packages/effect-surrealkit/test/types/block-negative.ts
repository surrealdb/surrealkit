/**
 * Negative type fixtures for blocks: each `@ts-expect-error` must be an
 * actual error, or `tsc -p test/types` fails, and `test/diagnostics.test.ts`
 * pins its message.
 */
import * as SurrealSchema from 'effect-surrealkit';

import {
	block,
	create,
	eq,
	literal,
	object,
	param,
	select,
} from '../../src/statement/index.js';
import {
	AccountId,
	InviteId,
	InviteTable,
	MembershipTable,
} from '../golden/scripts/index.js';

const inviteId = param('invite_id', InviteId);
const readInvite = () => select(InviteTable, inviteId).only().build();

export const rejectedLocals = () => {
	block()
		.let('a', () => literal(1))
		// @ts-expect-error [block/local-redefined] `$a` is already a local
		.let('a', () => literal(2));
	block()
		.throwIf(() => eq(param('x', SurrealSchema.String), 'y'), 'no')
		// @ts-expect-error [block/local-shadows-binding] `$x` is a binding the block reads
		.let('x', () => literal(1));
	// @ts-expect-error [block/local-engine-name] SurrealDB sets `$value`
	block().let('value', () => literal(1));
	// @ts-expect-error [block/local-not-a-value] a local holds an expression or a built statement
	block().let('a', () => 'text');
	block()
		.let('invite', readInvite)
		// @ts-expect-error [block/unknown-local] the block has no local `$nope`
		.return(($) => $.nope);
	block()
		.let('invite', readInvite)
		// @ts-expect-error [block/unknown-field] an invite has no field `nope`
		.return(($) => $.invite.nope);
	block()
		.if(
			() => eq(param('x', SurrealSchema.String), 'y'),
			(arm) => arm.let('inner', () => literal(1)),
		)
		// @ts-expect-error [block/arm-local-outside] a branch's local is visible only in that branch
		.else((arm) => arm.return(($) => $.inner));
	// @ts-expect-error [block/throw-if-none-unknown] `throwIfNone` takes a local's name
	block().let('invite', readInvite).throwIfNone('nope', 'missing');
	block()
		.let('invite', readInvite)
		// @ts-expect-error [block/if-not-none-unknown] `ifNotNone` takes a local's name
		.ifNotNone('nope', (arm) => arm);
};

export const rejectedSteps = () => {
	block()
		.let('invite', readInvite)
		// @ts-expect-error [block/condition-not-boolean] a guard's condition is a boolean expression
		.throwIf(($) => $.invite.team, 'no team');
	// @ts-expect-error [block/do-with-result] a step run for its effect returns NONE
	block().do(() => select(InviteTable).build());
	// @ts-expect-error [block/return-constant] a constant is returned as a `literal`
	block().return(() => 1);
	block()
		.let('invite', readInvite)
		.throwIfNone('invite', 'missing')
		.let('created', ($) =>
			create(MembershipTable)
				.content(
					// @ts-expect-error [block/object-extra-field] `bogus` is not a field membership accepts in CREATE
					object({
						team: $.invite.team,
						account: param('account', AccountId),
						role: $.invite.role,
						key: 'k',
						bogus: 1,
					}),
				)
				.build(),
		);
	// @ts-expect-error [object/not-an-expression] an object's fields are expressions or primitives
	object({ nested: { a: 1 } });
};

export const rejectedLocks = () => {
	// @ts-expect-error [select/for-update-table] FOR UPDATE reads one record by id
	select(InviteTable).forUpdate();
	select(InviteTable, inviteId)
		.only()
		// @ts-expect-error [select/for-update-late] `forUpdate` comes first, on `select(table, id)`
		.forUpdate();
};
