import { Effect, Schema } from 'effect';

import { codecIssueText } from '../../internal/codecIssue.js';
import type { AnyStatement } from '../../statement/statement.js';
import {
	StatementDecodeError,
	StatementEmptyResult,
	StatementWriteOutcomeError,
} from '../errors.js';

/**
 * What a request changed: it certainly wrote (a statement has `writes:
 * true`), it may have (one leaves `writes` unknown: authored text can call
 * a function that writes), or it did not (every one has `writes: false`).
 */
export type Changes = 'writes' | 'maybe' | 'none';

export const changesOf = (statements: ReadonlyArray<AnyStatement>): Changes =>
	statements.some((statement) => statement.writes === true)
		? 'writes'
		: statements.every((statement) => statement.writes === false)
			? 'none'
			: 'maybe';

export const committedNote = (changes: Changes): string =>
	changes === 'writes'
		? '. The request succeeded and its writes are committed, so do not send it again to retry'
		: changes === 'maybe'
			? '. The request succeeded and may have written: it is not known whether it writes, and any writes it made are committed; read the data back before sending it again'
			: '';

/**
 * A decode failure of a request that may have written is reported as a
 * {@link StatementWriteOutcomeError} around it; a read-only one as itself.
 */
export const decodeOutcome =
	(request: string, changes: Changes) =>
	<E>(error: E): E | StatementWriteOutcomeError =>
		changes !== 'none' && error instanceof StatementDecodeError
			? new StatementWriteOutcomeError({ request, cause: error })
			: error;

/** Decodes the results of one statement out of `results`, from `offset`. */
export const decode = (
	statement: AnyStatement,
	results: ReadonlyArray<unknown>,
	offset: number,
	writes: Changes,
): Effect.Effect<unknown, StatementDecodeError | StatementEmptyResult> => {
	const at = offset + statement.valueSlot - 1;
	for (let slot = offset; slot < offset + statement.slots; slot++) {
		if (slot !== at && results[slot] !== undefined) {
			return Effect.fail(
				new StatementDecodeError({
					statement: statement.name,
					slot: slot - offset + 1,
					committed: writes !== 'none',
					message: `result ${slot - offset + 1} of ${statement.slots} must be NONE; only result ${statement.valueSlot} of a statement is decoded${committedNote(writes)}`,
				}),
			);
		}
	}
	const value = results[at];
	if (
		statement.nonEmpty === true &&
		(value === undefined ||
			value === null ||
			(Array.isArray(value) && value.length === 0))
	) {
		return Effect.fail(
			new StatementEmptyResult({
				statement: statement.name,
				slot: statement.valueSlot,
				message: `\`${statement.name}\` returned nothing, although it returns a row whenever it applies: the session may lack permission for it (SurrealDB reports a denied write as an empty result, without an error), and it wrote nothing`,
			}),
		);
	}
	return Schema.decodeUnknownEffect(statement.result)(value, {
		onExcessProperty: 'error',
	}).pipe(
		Effect.mapError(
			(error) =>
				new StatementDecodeError({
					statement: statement.name,
					slot: statement.valueSlot,
					committed: writes !== 'none',
					message: `${codecIssueText(error.issue, statement.result.ast)}${committedNote(writes)}`,
				}),
		),
	);
};

export const countOf = (
	name: string,
	results: unknown,
	expected: number,
	writes: Changes,
): Effect.Effect<ReadonlyArray<unknown>, StatementDecodeError> =>
	Array.isArray(results) && results.length === expected
		? Effect.succeed(results)
		: Effect.fail(
				new StatementDecodeError({
					statement: name,
					slot: 0,
					committed: writes !== 'none',
					message: `expected ${expected} results, one per SurrealQL statement, and received ${
						Array.isArray(results) ? results.length : 'no list'
					}${committedNote(writes)}`,
				}),
			);
