/**
 * Writes the block budget fixture: `count` copies of a block shaped like the
 * `pay` script of `test/blocks.ts` (a locked read and a guard that narrows
 * it, a guard on a field, a computed key, a second read, and four branches:
 * one narrowed, one that throws, one that returns, one that binds two
 * locals, writes and returns), each with names of its own. The text is a
 * pure function of `count`, so the compiler's counters for it are
 * deterministic.
 */
const header = `import * as SurrealSchema from 'effect-surrealkit';

import {
	block,
	call,
	create,
	eq,
	lt,
	ne,
	object,
	param,
	select,
	update,
} from '../../src/statement/index.js';
import {
	AccountId,
	fn_debit,
	fn_payment_key,
	PaymentTable,
	WalletId,
	WalletTable,
} from '../golden/scripts/index.js';

export const unused = [SurrealSchema, block, call, create, eq, lt, ne, object, param, select, update, AccountId, fn_debit, fn_payment_key, PaymentTable, WalletId, WalletTable] as const;
`;

const pay = (
	i: number,
): string => `const amount${i} = param('amount${i}', SurrealSchema.Int);
export const pay${i} = block()
	.let('wallet', () => select(WalletTable, param('wallet${i}', WalletId)).forUpdate().only().build())
	.throwIfNone('wallet', 'Wallet not found ${i}')
	.throwIf(($) => ne($.wallet.owner, param('payer${i}', AccountId)), 'Not your wallet ${i}')
	.let('key', ($) => call(fn_payment_key, $.wallet.id, param('request${i}', SurrealSchema.String)))
	.let('previous', ($) => select(PaymentTable).where((row) => eq(row.key, $.key)).only().build())
	.ifNotNone('previous', (arm) => arm.return(($) => ({ kind: 'replayed', payment: $.previous.id, amount: $.previous.amount })))
	.elseIf(($) => $.wallet.frozen, (arm) => arm.throw('Wallet is frozen ${i}'))
	.elseIf(($) => lt($.wallet.balance, amount${i}), (arm) => arm.return(($) => ({ kind: 'declined', balance: $.wallet.balance })))
	.else((arm) =>
		arm
			.let('balance', ($) => call(fn_debit, $.wallet.balance, amount${i}))
			.let('payment', ($) => create(PaymentTable).content(object({ wallet: $.wallet.id, amount: amount${i}, key: $.key })).build())
			.do(($) => update(WalletTable, $.wallet.id).merge(object({ balance: $.balance })).returning('none').build())
			.return(($) => ({ kind: 'paid', payment: $.payment.id, balance: $.balance })),
	)
	.build('pay${i}');
`;

/** The fixture's text: the imports, then `count` blocks. */
export const blockBudgetFixture = (count: number): string =>
	[header, ...Array.from({ length: count }, (_, i) => pay(i))].join('\n');
