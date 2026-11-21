import { Effect, Schema, SchemaAST } from 'effect';

import { codecIssueText } from '../../internal/codecIssue.js';
import type { AnyStatement } from '../../statement/statement.js';
import type { RunOptions } from '../client.js';
import { StatementEncodeError } from '../errors.js';
import { pathText, valueProblem } from '../values.js';

export const encode = (
	statement: AnyStatement,
	input: Readonly<Record<string, unknown>>,
): Effect.Effect<Record<string, unknown>, StatementEncodeError> =>
	Effect.forEach(Object.entries(statement.params), ([name, codec]) =>
		Schema.encodeUnknownEffect(codec)(input[name]).pipe(
			Effect.map((value) => [name, value] as const),
			Effect.mapError(
				(error) =>
					new StatementEncodeError({
						statement: statement.name,
						binding: name,
						message: codecIssueText(
							error.issue,
							SchemaAST.flip(codec.ast),
						),
					}),
			),
		),
	).pipe(Effect.map((entries) => Object.fromEntries(entries)));

/**
 * Refuses an encoded binding the SDK would not send as it is. `owner`
 * names the statement that binds each name.
 */
export const checkValues = (
	bindings: Readonly<Record<string, unknown>>,
	owner: (binding: string) => string,
	options: RunOptions | undefined,
): Effect.Effect<void, StatementEncodeError> => {
	if (options?.unsafeSkipValueCheck === true) return Effect.void;
	for (const [binding, value] of Object.entries(bindings)) {
		const found = valueProblem(value);
		if (found !== undefined) {
			return Effect.fail(
				new StatementEncodeError({
					statement: owner(binding),
					binding,
					path: found.path,
					message: `${pathText(binding, found.path)} is ${found.problem}; nothing was sent`,
				}),
			);
		}
	}
	return Effect.void;
};
