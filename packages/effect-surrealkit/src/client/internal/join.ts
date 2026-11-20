import type { Bound } from '../client.js';
import { StatementScopeError } from '../errors.js';

/**
 * What a transaction refuses before encoding anything: a statement that
 * writes its own transaction control, and a name one statement binds and
 * another `LET`s, which would make one of them read the other's value.
 */
export const refuseInTransaction = (
	statements: ReadonlyArray<Bound>,
): StatementScopeError | undefined => {
	for (const { statement } of statements) {
		if (statement.transactional) {
			return new StatementScopeError({
				statement: statement.name,
				message: `\`${statement.name}\` writes its own BEGIN, COMMIT or CANCEL, so it cannot run inside a transaction; run it on its own`,
			});
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
