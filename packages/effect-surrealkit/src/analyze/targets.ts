/**
 * The tables and custom functions an authored SurrealQL text names, from its
 * tokens. A lexer cannot parse, so a target it cannot read with certainty as
 * a static table name sets `openTargets` and adds nothing: refuse rather
 * than guess.
 */
import type { Token } from './lexer.js';

/** The names a text uses, where each is first used, and what it defines. */
export interface Targets {
	readonly read: ReadonlyArray<string>;
	readonly written: ReadonlyArray<string>;
	readonly functions: ReadonlyArray<string>;
	readonly openTargets: boolean;
	/** The 1-based line of the first use of each name. */
	readonly readAt: ReadonlyMap<string, number>;
	readonly writtenAt: ReadonlyMap<string, number>;
	readonly functionsAt: ReadonlyMap<string, number>;
	/** Names the text itself creates with `DEFINE TABLE` or `DEFINE FUNCTION`. */
	readonly definedTables: ReadonlySet<string>;
	readonly definedFunctions: ReadonlySet<string>;
}

/** Statements that contribute nothing. */
const IGNORED: ReadonlySet<string> = new Set([
	'DEFINE',
	'REMOVE',
	'ALTER',
	'INFO',
	'USE',
	'SHOW',
	'REBUILD',
]);

const WRITES: ReadonlySet<string> = new Set([
	'CREATE',
	'UPDATE',
	'UPSERT',
	'DELETE',
	'INSERT',
	'RELATE',
]);

/** Statement words a parenthesised subquery can start with. */
const SUBQUERY: ReadonlySet<string> = new Set([
	'SELECT',
	'CREATE',
	'UPDATE',
	'UPSERT',
	'DELETE',
	'INSERT',
	'RELATE',
]);

/** Words that end a target list. */
const CLAUSES: ReadonlySet<string> = new Set([
	'SET',
	'UNSET',
	'CONTENT',
	'MERGE',
	'PATCH',
	'REPLACE',
	'WHERE',
	'RETURN',
	'TIMEOUT',
	'PARALLEL',
	'VERSION',
	'WITH',
	'SPLIT',
	'GROUP',
	'ORDER',
	'LIMIT',
	'START',
	'FETCH',
	'EXPLAIN',
	'TEMPFILES',
	// The end of a statement inside `IF … THEN … ELSE … END`.
	'ELSE',
	'END',
]);

/** Words after which a statement can start, besides `;`, `{` and `(`. */
const VALUE_WORDS: ReadonlySet<string> = new Set(['RETURN', 'THEN', 'ELSE']);

const upper = (token: Token | undefined): string =>
	token?.kind === 'word' ? token.value.toUpperCase() : '';

const isPunct = (token: Token | undefined, char: string): boolean =>
	token?.kind === 'punct' && token.value === char;

const isOpen = (token: Token | undefined): boolean =>
	isPunct(token, '(') || isPunct(token, '[') || isPunct(token, '{');

const isClose = (token: Token | undefined): boolean =>
	isPunct(token, ')') || isPunct(token, ']') || isPunct(token, '}');

const isName = (token: Token | undefined): token is Token =>
	(token?.kind === 'word' && /^[A-Za-z_]/u.test(token.value)) ||
	token?.kind === 'ident';

/** A plain string literal, not a prefixed one (`r''`, `d''`, …). */
const isPlainString = (token: Token | undefined): token is Token =>
	token?.kind === 'string' &&
	(token.prefix === undefined || token.prefix === 's');

/** Two adjacent tokens, as one operator (`->`, `::`). */
const adjacent = (a: Token | undefined, b: Token | undefined): boolean =>
	a !== undefined && b !== undefined && a.end === b.start;

export const analyzeTargets = (tokens: ReadonlyArray<Token>): Targets => {
	const readAt = new Map<string, number>();
	const writtenAt = new Map<string, number>();
	const functionsAt = new Map<string, number>();
	const definedTables = new Set<string>();
	const definedFunctions = new Set<string>();
	let open = false;
	/** Tokens a rule has already read. */
	const consumed = new Set<number>();

	const add = (into: Map<string, number>, name: string, token: Token) => {
		if (!into.has(name)) into.set(name, token.line);
	};

	/** The index just past the bracket group opening at `at`. */
	const pastGroup = (at: number): number => {
		let depth = 0;
		for (let j = at; j < tokens.length; j++) {
			if (isOpen(tokens[j])) depth++;
			else if (isClose(tokens[j])) {
				depth--;
				if (depth === 0) return j + 1;
			}
		}
		return tokens.length;
	};

	/** `::` at `at`, as two adjacent colons. */
	const pathAt = (at: number): boolean =>
		isPunct(tokens[at], ':') &&
		isPunct(tokens[at + 1], ':') &&
		adjacent(tokens[at], tokens[at + 1]);

	/** A member of something else: `a.word`, `a:word`, `word:…`. */
	const member = (at: number): boolean =>
		isPunct(tokens[at - 1], '.') || isPunct(tokens[at - 1], ':');

	/** Where a statement can start: after nothing, `;`, `{`, `(`, … */
	const statementPosition = (at: number): boolean => {
		const previous = tokens[at - 1];
		if (previous === undefined) return true;
		if (
			isPunct(previous, ';') ||
			isPunct(previous, '{') ||
			isPunct(previous, '(')
		)
			return true;
		if (VALUE_WORDS.has(upper(previous))) return true;
		return (
			isPunct(previous, '=') &&
			tokens[at - 2]?.kind === 'param' &&
			upper(tokens[at - 3]) === 'LET'
		);
	};

	/** The end of the statement at `at`: its `;` or enclosing bracket. */
	const statementEnd = (at: number): number => {
		for (let j = at; j < tokens.length; j++) {
			const token = tokens[j];
			if (isPunct(token, ';') || isClose(token)) return j;
			if (isOpen(token)) j = pastGroup(j) - 1;
		}
		return tokens.length;
	};

	/**
	 * `type::<fn>(` at `at`: the index of its `(`, or -1.
	 */
	const typeCall = (at: number, fn: string): number =>
		tokens[at]?.kind === 'word' &&
		tokens[at]?.value.toLowerCase() === 'type' &&
		pathAt(at + 1) &&
		tokens[at + 3]?.kind === 'word' &&
		tokens[at + 3]?.value.toLowerCase() === fn &&
		isPunct(tokens[at + 4], '(')
			? at + 4
			: -1;

	type Target =
		| {
				readonly kind: 'table';
				readonly name: string;
				readonly next: number;
		  }
		| { readonly kind: 'subquery'; readonly next: number }
		| { readonly kind: 'open' };

	/**
	 * A `type::record('t', …)`, `type::thing('t', …)` or `type::table('t')`
	 * at `at`: its table and the index past it, `open` for any other
	 * argument, `undefined` when there is no such call.
	 */
	const typeTarget = (
		at: number,
		fns: ReadonlyArray<string>,
	): Target | undefined => {
		for (const fn of fns) {
			const paren = typeCall(at, fn);
			if (paren === -1) continue;
			const literal = tokens[paren + 1];
			const after = tokens[paren + 2];
			const next = pastGroup(paren);
			const certain =
				isPlainString(literal) &&
				(fn === 'table' ? isPunct(after, ')') : isPunct(after, ','));
			return certain
				? { kind: 'table', name: literal.value, next }
				: { kind: 'open' };
		}
		return undefined;
	};

	/**
	 * One static target at `at`: a bare or quoted name, a record id
	 * `table:key`, a `type::record` or `type::thing` with a literal table,
	 * or (when `subqueries`) a parenthesised subquery.
	 */
	const target = (at: number, subqueries: boolean): Target => {
		const token = tokens[at];
		const typed = typeTarget(at, ['record', 'thing']);
		if (typed !== undefined) return typed;
		// `type::table('t')`: a read wherever it stands, so a table of a
		// FROM list; as a write target it is another function call.
		const table = typeTarget(at, ['table']);
		if (table !== undefined) {
			if (!subqueries) {
				const paren = typeCall(at, 'table');
				for (let k = at; k < pastGroup(paren); k++) consumed.add(k);
				return { kind: 'open' };
			}
			return table;
		}
		if (isPunct(token, '(')) {
			return subqueries && SUBQUERY.has(upper(tokens[at + 1]))
				? { kind: 'subquery', next: pastGroup(at) }
				: { kind: 'open' };
		}
		if (!isName(token)) return { kind: 'open' };
		if (token.kind === 'word' && CLAUSES.has(upper(token)))
			return { kind: 'open' };
		const colon = tokens[at + 1];
		if (isPunct(colon, ':') && !isPunct(tokens[at + 2], ':')) {
			// A record id: the key is one token or one bracket group.
			const key = at + 2;
			const keyToken = tokens[key];
			if (keyToken === undefined) return { kind: 'open' };
			const next = isOpen(keyToken) ? pastGroup(key) : key + 1;
			if (!isOpen(keyToken) && keyToken.kind === 'punct')
				return { kind: 'open' };
			return { kind: 'table', name: token.value, next };
		}
		return { kind: 'table', name: token.value, next: at + 1 };
	};

	/** Whether the token at `at` ends a target. */
	const ends = (at: number, extra: ReadonlyArray<string> = []): boolean => {
		const token = tokens[at];
		return (
			token === undefined ||
			isPunct(token, ';') ||
			isClose(token) ||
			CLAUSES.has(upper(token)) ||
			extra.includes(upper(token))
		);
	};

	/**
	 * A comma-separated list of targets from `at` up to a clause: each
	 * static name is added to `into`; anything else sets `openTargets`.
	 */
	const targetList = (
		at: number,
		into: Map<string, number>,
		subqueries: boolean,
	): void => {
		let j = at;
		if (upper(tokens[j]) === 'ONLY') j++;
		const found: Array<{ name: string; token: Token }> = [];
		for (;;) {
			const item = target(j, subqueries);
			if (item.kind === 'open') {
				open = true;
				return;
			}
			if (item.kind === 'table') {
				found.push({ name: item.name, token: tokens[j] as Token });
				for (let k = j; k < item.next; k++) consumed.add(k);
			}
			j = item.next;
			if (isPunct(tokens[j], ',')) {
				j++;
				continue;
			}
			if (!ends(j)) {
				open = true;
				return;
			}
			break;
		}
		for (const { name, token } of found) add(into, name, token);
	};

	const select = (at: number): void => {
		const end = statementEnd(at + 1);
		const froms: number[] = [];
		for (let j = at + 1; j < end; j++) {
			if (isOpen(tokens[j])) {
				j = pastGroup(j) - 1;
				continue;
			}
			if (upper(tokens[j]) === 'FROM' && !member(j)) froms.push(j);
		}
		// No FROM, or a field named `from`: not certain which is the clause.
		if (froms.length !== 1) {
			open = true;
			return;
		}
		targetList((froms[0] as number) + 1, readAt, true);
	};

	const write = (at: number, word: string): void => {
		let j = at + 1;
		if (word === 'INSERT') {
			while (
				upper(tokens[j]) === 'IGNORE' ||
				upper(tokens[j]) === 'RELATION'
			)
				j++;
			if (upper(tokens[j]) !== 'INTO') {
				open = true;
				return;
			}
			j++;
			const token = tokens[j];
			const typed = typeTarget(j, ['table']);
			if (typed !== undefined) {
				if (typed.kind === 'table') {
					for (let k = j; k < typed.next; k++) consumed.add(k);
					add(writtenAt, typed.name, token as Token);
				} else open = true;
				return;
			}
			if (
				isName(token) &&
				!isPunct(tokens[j + 1], '.') &&
				!isPunct(tokens[j + 1], ':')
			) {
				consumed.add(j);
				add(writtenAt, token.value, token);
			} else open = true;
			return;
		}
		if (word === 'RELATE') {
			if (upper(tokens[j]) === 'ONLY') j++;
			const end = statementEnd(j);
			let arrow = -1;
			for (let k = j; k < end; k++) {
				if (isOpen(tokens[k])) {
					k = pastGroup(k) - 1;
					continue;
				}
				if (
					isPunct(tokens[k], '-') &&
					isPunct(tokens[k + 1], '>') &&
					adjacent(tokens[k], tokens[k + 1])
				) {
					arrow = k;
					break;
				}
			}
			if (arrow === -1) {
				open = true;
				return;
			}
			consumed.add(arrow);
			consumed.add(arrow + 1);
			const edge = target(arrow + 2, false);
			const after = edge.kind === 'table' ? edge.next : -1;
			if (
				edge.kind === 'table' &&
				isPunct(tokens[after], '-') &&
				isPunct(tokens[after + 1], '>') &&
				adjacent(tokens[after], tokens[after + 1])
			) {
				for (let k = arrow + 2; k < after + 2; k++) consumed.add(k);
				add(writtenAt, edge.name, tokens[arrow + 2] as Token);
			} else open = true;
			return;
		}
		if (word === 'DELETE' && upper(tokens[j]) === 'FROM') j++;
		targetList(j, writtenAt, false);
	};

	/** `DEFINE TABLE name` or `DEFINE FUNCTION fn::name`: what it creates. */
	const definition = (at: number): void => {
		const kind = upper(tokens[at + 1]);
		let j = at + 2;
		if (upper(tokens[j]) === 'OVERWRITE') j++;
		if (
			upper(tokens[j]) === 'IF' &&
			upper(tokens[j + 1]) === 'NOT' &&
			upper(tokens[j + 2]) === 'EXISTS'
		)
			j += 3;
		if (kind === 'TABLE' && isName(tokens[j]))
			definedTables.add((tokens[j] as Token).value);
		if (kind === 'FUNCTION') {
			const name = functionName(j);
			if (name !== undefined) definedFunctions.add(name.name);
		}
	};

	/** `fn::a::b` at `at`: its name as written and the index past it. */
	const functionName = (
		at: number,
	): { readonly name: string; readonly next: number } | undefined => {
		const token = tokens[at];
		if (token?.kind !== 'word' || token.value.toLowerCase() !== 'fn')
			return undefined;
		let name = token.value;
		let j = at + 1;
		while (pathAt(j) && isName(tokens[j + 2])) {
			name += `::${(tokens[j + 2] as Token).value}`;
			j += 3;
		}
		return j === at + 1 ? undefined : { name, next: j };
	};

	/** A graph arrow at `at` (`->`, `<-`, `<->`): the index past it, or -1. */
	const arrowEnd = (at: number): number => {
		const a = tokens[at];
		const b = tokens[at + 1];
		const c = tokens[at + 2];
		if (isPunct(a, '-') && isPunct(b, '>') && adjacent(a, b)) return at + 2;
		if (isPunct(a, '<') && isPunct(b, '-') && adjacent(a, b)) {
			return isPunct(c, '>') && adjacent(b, c) ? at + 3 : at + 2;
		}
		return -1;
	};

	/** The names of a graph step after an arrow, at `at`. */
	const graphStep = (at: number): void => {
		const token = tokens[at];
		if (isName(token)) {
			add(readAt, token.value, token);
			return;
		}
		if (!isPunct(token, '(')) {
			// `->?` and anything else: any table, or not certain which.
			open = true;
			return;
		}
		// A subquery step: its own FROM is read where it stands.
		if (SUBQUERY.has(upper(tokens[at + 1]))) return;
		let j = at + 1;
		const names: Token[] = [];
		for (;;) {
			const name = tokens[j];
			if (!isName(name)) {
				open = true;
				return;
			}
			names.push(name);
			j++;
			if (isPunct(tokens[j], ',')) {
				j++;
				continue;
			}
			break;
		}
		if (!ends(j, ['AS'])) {
			open = true;
			return;
		}
		for (const name of names) add(readAt, name.value, name);
	};

	for (let i = 0; i < tokens.length; i++) {
		if (consumed.has(i)) continue;
		const token = tokens[i] as Token;
		if (token.kind === 'word') {
			const word = token.value.toUpperCase();
			if (
				IGNORED.has(word) &&
				statementPosition(i) &&
				!isPunct(tokens[i + 1], ':')
			) {
				if (word === 'DEFINE') definition(i);
				i = statementEnd(i) - 1;
				continue;
			}
			const fn = functionName(i);
			if (fn !== undefined && !member(i)) {
				add(functionsAt, fn.name, token);
				i = fn.next - 1;
				continue;
			}
			if (member(i) || isPunct(tokens[i + 1], ':')) {
				const typed = typeTarget(i, ['record', 'thing', 'table']);
				if (typed?.kind === 'table') {
					add(readAt, typed.name, token);
				} else if (typed?.kind === 'open') open = true;
				continue;
			}
			if (word === 'SELECT') {
				select(i);
				continue;
			}
			if (WRITES.has(word)) {
				if (statementPosition(i) || isPunct(tokens[i - 1], '[')) {
					write(i, word);
				} else if (
					!(
						tokens[i + 1]?.kind === 'punct' ||
						upper(tokens[i + 1]) === 'FROM' ||
						upper(tokens[i + 1]) === 'AS'
					)
				) {
					// Not certain whether this is a statement or a field.
					open = true;
				}
			}
			continue;
		}
		if (token.kind === 'punct') {
			const past = arrowEnd(i);
			if (past !== -1 && !isPunct(tokens[i - 1], '|')) {
				graphStep(past);
				i = past - 1;
			}
		}
	}

	const sortedKeys = (map: ReadonlyMap<string, number>) =>
		[...map.keys()].sort();
	return {
		read: sortedKeys(readAt),
		written: sortedKeys(writtenAt),
		functions: sortedKeys(functionsAt),
		openTargets: open,
		readAt,
		writtenAt,
		functionsAt,
		definedTables,
		definedFunctions,
	};
};
