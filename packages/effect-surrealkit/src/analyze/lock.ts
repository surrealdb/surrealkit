/**
 * `FOR UPDATE` at the end of a `SELECT`. SurrealDB 3.3 and 4.0 accept it
 * only on record-id targets ("SELECT ... FOR UPDATE requires record ids as
 * targets"), never with `GROUP`, `SPLIT` or `VERSION`, and followed by
 * nothing but `TEMPFILES` and `EXPLAIN [FULL]`, which cannot precede it. It locks the records and
 * changes neither the target nor the projection
 * (`test/projections-engine.test.ts`, both lanes).
 */
import type { Token } from './lexer.js';

/**
 * - `none`: no `FOR UPDATE` at the query's own level.
 * - `locked`: one, at `at`, in a form both engines accept when every target
 *   is a record id.
 * - `refused`: one in a form an engine refuses, or not certain.
 */
export type Lock =
	| { readonly kind: 'none' }
	| { readonly kind: 'locked'; readonly at: number }
	| { readonly kind: 'refused' };

const upper = (token: Token | undefined): string =>
	token?.kind === 'word' ? token.value.toUpperCase() : '';

const isPunct = (token: Token | undefined, char: string): boolean =>
	token?.kind === 'punct' && token.value === char;

const isOpen = (token: Token | undefined): boolean =>
	isPunct(token, '(') || isPunct(token, '[') || isPunct(token, '{');

const isClose = (token: Token | undefined): boolean =>
	isPunct(token, ')') || isPunct(token, ']') || isPunct(token, '}');

/**
 * Words `FOR UPDATE` cannot follow, on both engines: clauses it cannot be
 * combined with, and clauses that must come after it.
 */
const EXCLUDED: ReadonlySet<string> = new Set([
	'GROUP',
	'SPLIT',
	'VERSION',
	'TEMPFILES',
	'EXPLAIN',
]);

/** Words that may follow `FOR UPDATE`, on both engines. */
const AFTER: ReadonlySet<string> = new Set(['TEMPFILES', 'EXPLAIN', 'FULL']);

/** The `FOR UPDATE` of the `SELECT` from `from` to `end` (exclusive). */
export const lockOf = (
	tokens: ReadonlyArray<Token>,
	from: number,
	end: number,
): Lock => {
	const level: number[] = [];
	for (let j = from; j < end; j++) {
		if (isOpen(tokens[j])) {
			let depth = 0;
			for (; j < end; j++) {
				if (isOpen(tokens[j])) depth++;
				else if (isClose(tokens[j]) && --depth === 0) break;
			}
			continue;
		}
		if (!isPunct(tokens[j - 1], '.') && !isPunct(tokens[j - 1], ':'))
			level.push(j);
	}
	const at = level.findIndex(
		(j, k) =>
			upper(tokens[j]) === 'FOR' &&
			level[k + 1] === j + 1 &&
			upper(tokens[j + 1]) === 'UPDATE',
	);
	if (at === -1) return { kind: 'none' };
	const index = level[at] as number;
	for (const j of level.slice(at + 2))
		if (!AFTER.has(upper(tokens[j]))) return { kind: 'refused' };
	for (const j of level.slice(0, at))
		if (EXCLUDED.has(upper(tokens[j]))) return { kind: 'refused' };
	return { kind: 'locked', at: index };
};
