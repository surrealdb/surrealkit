import { Effect } from 'effect';

import type { AnyStatement } from '../../statement/statement.js';
import { sameValue } from '../equal.js';
import { StatementScopeError } from '../errors.js';
import type { Request, RequestPart } from './transport.js';

interface Entry<B> {
	readonly statement: AnyStatement;
	readonly bindings: B;
}

type Bindings = Request['bindings'];

interface JoinOptions {
	readonly mode: Request['mode'];
	/** One extra slot per side, for example BEGIN and COMMIT. */
	readonly wrap?: { readonly before: string; readonly after: string };
	readonly refuseTransactional: boolean;
}

/**
 * What a transaction refuses before encoding anything: a statement that
 * writes its own transaction control, and a name one statement binds and
 * another `LET`s, which would make one of them read the other's value.
 */
const refuseInTransaction = (
	statements: ReadonlyArray<Entry<unknown>>,
	refuseTransactional: boolean,
): StatementScopeError | undefined => {
	if (refuseTransactional) {
		for (const { statement } of statements) {
			if (statement.transactional) {
				return new StatementScopeError({
					statement: statement.name,
					message: `\`${statement.name}\` writes its own BEGIN, COMMIT or CANCEL, so it cannot run inside a transaction; run it on its own`,
				});
			}
		}
	}
	for (const [index, { statement }] of statements.entries()) {
		for (const binding of Object.keys(statement.params)) {
			const definer = statements.find(
				(other, at) =>
					at !== index && other.statement.defines.includes(binding),
			);
			if (definer !== undefined) {
				return new StatementScopeError({
					statement: statement.name,
					binding,
					message: `\`$${binding}\` is bound by \`${statement.name}\` and defined with LET by \`${definer.statement.name}\`, and statements in one transaction share one variable scope, so one would read the other's value; rename one of them`,
				});
			}
		}
	}
	return undefined;
};

export const requestPart = (
	statement: AnyStatement,
	bindings: Bindings,
	offset: number,
	slots: number,
): RequestPart => ({
	statement,
	bindings,
	sent: Object.fromEntries(Object.keys(bindings).map((name) => [name, name])),
	offset,
	slots,
});

/** Join with lazy bindings so a transaction still refuses scope before later codecs run. */
export const joinWith = <E>(
	statements: ReadonlyArray<Entry<Effect.Effect<Bindings, E>>>,
	options: JoinOptions,
): Effect.Effect<Request, E | StatementScopeError> =>
	Effect.gen(function* () {
		const refused = refuseInTransaction(
			statements,
			options.refuseTransactional,
		);
		if (refused !== undefined) return yield* Effect.fail(refused);
		const bindings: Record<string, unknown> = {};
		const boundBy = new Map<string, string>();
		const sql: Array<string> = [];
		const parts: Array<RequestPart> = [];
		let offset = 0;
		for (const { statement, bindings: encoded } of statements) {
			const mine = yield* encoded;
			for (const [key, value] of Object.entries(mine)) {
				const first = boundBy.get(key);
				if (first === undefined) {
					boundBy.set(key, statement.name);
				} else if (!sameValue(bindings[key], value)) {
					return yield* Effect.fail(
						new StatementScopeError({
							statement: statement.name,
							binding: key,
							message: `\`$${key}\` is bound by \`${first}\` and by \`${statement.name}\` to values that are not certainly equal, and statements in one transaction share one variable scope; rename the binding in one of them`,
						}),
					);
				}
				bindings[key] = value;
			}
			const wrap = options.wrap;
			const slots = statement.slots + (wrap === undefined ? 0 : 2);
			parts.push(requestPart(statement, mine, offset, slots));
			offset += slots;
			if (wrap === undefined) {
				sql.push(statement.sql);
			} else {
				sql.push(wrap.before, statement.sql, wrap.after);
			}
		}
		return { mode: options.mode, sql: sql.join('\n'), bindings, parts };
	});

export const join = (
	statements: ReadonlyArray<Entry<Bindings>>,
	options: JoinOptions,
): Effect.Effect<Request, StatementScopeError> =>
	joinWith(
		statements.map(({ statement, bindings }) => ({
			statement,
			bindings: Effect.succeed(bindings),
		})),
		options,
	);
