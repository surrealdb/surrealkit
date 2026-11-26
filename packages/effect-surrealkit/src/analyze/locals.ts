/**
 * Locals the text itself fixes to one table: `LET $x = type::record('t',
 * …);` makes `$x` a record of `t` whatever the key, on SurrealDB 3.3 and
 * 4.0 (`test/fields-engine.test.ts`), so a later `FROM $x`, `UPDATE $x`,
 * `UPSERT $x` or `DELETE $x` targets `t`. Only where nothing can make `$x`
 * hold something else: refuse rather than guess.
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

const adjacent = (a: Token | undefined, b: Token | undefined): boolean =>
	a !== undefined && b !== undefined && a.end === b.start;

const isPlainString = (token: Token | undefined): token is Token =>
	token?.kind === 'string' &&
	(token.prefix === undefined || token.prefix === 's');

/**
 * Names the engine sets itself, wherever they are read or inside some
 * clauses and definitions: a LET of one does not fix what a read sees.
 */
const ENGINE_NAMES: ReadonlySet<string> = new Set([
	'auth',
	'access',
	'session',
	'token',
	'this',
	'self',
	'parent',
	'value',
	'input',
	'before',
	'after',
	'event',
]);

/**
 * Each read of a local the text fixes to one table, by its token: the
 * table. A local counts only when all of this holds:
 *
 * - the text has no hole (a fill could LET it), no closure (whose
 *   parameters can shadow it) and no `DEFINE` (a function body reads the
 *   caller's LETs on 3.3 and not on 4.0);
 * - its one `LET` in the whole text, and no `FOR` of the same name, is a
 *   top-level statement exactly `LET $x = type::record('t', …);` (or
 *   `type::thing`) with a plain string literal for the table;
 * - it is not a name the engine sets;
 * - the read comes after that statement.
 */
export const recordLocals = (
	tokens: ReadonlyArray<Token>,
): ReadonlyMap<Token, string> => {
	const out = new Map<Token, string>();
	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i] as Token;
		if (token.kind === 'hole') return out;
		if (isPunct(token, '|') && tokens[i + 1]?.kind === 'param') return out;
		if (
			upper(token) === 'DEFINE' &&
			!isPunct(tokens[i - 1], '.') &&
			!isPunct(tokens[i - 1], ':') &&
			!isPunct(tokens[i + 1], ':')
		)
			return out;
	}
	/** Each name's LET and FOR sites, by the index of its parameter. */
	const lets = new Map<string, number[]>();
	const loops = new Set<string>();
	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i] as Token;
		if (token.kind !== 'param') continue;
		const before = upper(tokens[i - 1]);
		if (before === 'LET')
			lets.set(token.value, [...(lets.get(token.value) ?? []), i]);
		else if (before === 'FOR') loops.add(token.value);
	}
	/** Bracket depth before each token. */
	const depth: number[] = [];
	let level = 0;
	for (const token of tokens) {
		if (isClose(token)) level--;
		depth.push(level);
		if (isOpen(token)) level++;
	}
	const closing = (at: number): number => {
		let d = 0;
		for (let j = at; j < tokens.length; j++) {
			if (isOpen(tokens[j])) d++;
			else if (isClose(tokens[j]) && --d === 0) return j;
		}
		return tokens.length;
	};
	for (const [name, sites] of lets) {
		if (sites.length !== 1 || loops.has(name) || ENGINE_NAMES.has(name))
			continue;
		const at = sites[0] as number;
		const let_ = at - 1;
		// A top-level statement: at the start of the text or after a `;`.
		if (depth[let_] !== 0 || (let_ > 0 && !isPunct(tokens[let_ - 1], ';')))
			continue;
		// `= type::record(` or `= type::thing(`, a plain string, then `,`.
		const fn = tokens[at + 5];
		if (
			!isPunct(tokens[at + 1], '=') ||
			isPunct(tokens[at + 2], '=') ||
			tokens[at + 2]?.kind !== 'word' ||
			tokens[at + 2]?.value.toLowerCase() !== 'type' ||
			!isPunct(tokens[at + 3], ':') ||
			!isPunct(tokens[at + 4], ':') ||
			!adjacent(tokens[at + 3], tokens[at + 4]) ||
			fn?.kind !== 'word' ||
			!['record', 'thing'].includes(fn.value.toLowerCase()) ||
			!isPunct(tokens[at + 6], '(') ||
			!isPlainString(tokens[at + 7]) ||
			!isPunct(tokens[at + 8], ',')
		)
			continue;
		const end = closing(at + 6) + 1;
		// Nothing after the call but the statement's end.
		if (!isPunct(tokens[end], ';')) continue;
		const table = (tokens[at + 7] as Token).value;
		for (let j = end + 1; j < tokens.length; j++) {
			const read = tokens[j] as Token;
			if (read.kind === 'param' && read.value === name)
				out.set(read, table);
		}
	}
	return out;
};
