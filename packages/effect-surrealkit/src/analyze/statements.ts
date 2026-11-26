/**
 * Where the statements of an authored SurrealQL text are, for the readers
 * of projected fields and binding uses: the top-level statements, and the
 * statements of each `{…}` block (the body of `IF`, `ELSE`, `FOR`, a
 * function or a bare block), split at the block's own `;`. A `{…}` that is
 * an object literal is split the same way; see {@link queryAt} for why a
 * reader finds nothing in it.
 */
import type { Token } from './lexer.js';

const upper = (token: Token | undefined): string =>
	token?.kind === 'word' ? token.value.toUpperCase() : '';

const isPunct = (token: Token | undefined, char: string): boolean =>
	token?.kind === 'punct' && token.value === char;

const isOpen = (token: Token | undefined): boolean =>
	isPunct(token, '(') || isPunct(token, '[') || isPunct(token, '{');

const isClose = (token: Token | undefined): boolean =>
	isPunct(token, ')') || isPunct(token, ']') || isPunct(token, '}');

/** The index of the bracket closing the one at `at`. */
export const closingOf = (tokens: ReadonlyArray<Token>, at: number): number => {
	let depth = 0;
	for (let j = at; j < tokens.length; j++) {
		if (isOpen(tokens[j])) depth++;
		else if (isClose(tokens[j])) {
			depth--;
			if (depth === 0) return j;
		}
	}
	return tokens.length;
};

/** One statement: its tokens from `start` to `end` (exclusive). */
export interface StatementRange {
	readonly start: number;
	/**
	 * Its first token past `LET $name =` or `RETURN`: where a query that is
	 * the statement's value starts.
	 */
	readonly first: number;
	/** The index of its `;`, of the bracket that closes it, or the end. */
	readonly end: number;
}

/**
 * The statements from `start` to `stop` (exclusive) at one level, split at
 * the `;` of that level; brackets inside a statement are not entered.
 */
export const statementsIn = (
	tokens: ReadonlyArray<Token>,
	start: number,
	stop: number,
): ReadonlyArray<StatementRange> => {
	const out: StatementRange[] = [];
	let from = start;
	for (let i = start; i <= stop; i++) {
		const token = tokens[i];
		if (i < stop && isOpen(token)) {
			i = closingOf(tokens, i);
			continue;
		}
		if (i < stop && !isPunct(token, ';')) continue;
		let first = from;
		if (
			upper(tokens[first]) === 'LET' &&
			tokens[first + 1]?.kind === 'param' &&
			isPunct(tokens[first + 2], '=') &&
			!isPunct(tokens[first + 3], '=')
		)
			first += 3;
		else if (upper(tokens[first]) === 'RETURN') first += 1;
		if (from < i) out.push({ start: from, first, end: i });
		from = i + 1;
	}
	return out;
};

/**
 * Whether the statement's value is a query starting with one of `words`: a
 * statement word at `first`. A statement word followed by `:` there is a
 * key of an object literal (`{ select: 1, … }`; a record id with such a
 * table, `select:1`, is a parse error on SurrealDB 3.3 and 4.0) and is read
 * as a query too: the readers then find nothing, since every clause word
 * they read must not be followed by `:`, and a `FROM` in an entry's value
 * is followed by the entry's end or by an operator, never by a target.
 */
export const queryAt = (
	tokens: ReadonlyArray<Token>,
	statement: StatementRange,
	words: ReadonlySet<string>,
): boolean =>
	statement.first < statement.end &&
	words.has(upper(tokens[statement.first]));
