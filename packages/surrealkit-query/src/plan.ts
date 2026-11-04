/**
 * How a SELECT becomes a live query.
 *
 * `LIVE SELECT` takes a projection (or `VALUE`), one target, `WHERE` and
 * `FETCH`, and nothing else: `ORDER BY`, `GROUP`, `LIMIT`, `START`, `SPLIT`,
 * `OMIT`, `TIMEOUT`, `PARALLEL`, `TEMPFILES`, `EXPLAIN`, `WITH`, `FROM ONLY`
 * and several targets are refused (the engine's E4009). So a SELECT is made
 * live in one of two ways:
 *
 * - **keyed**: the SELECT has only clauses `LIVE SELECT` accepts. It runs as
 *   `LIVE <text>`, and each notification is applied to the rows by record id.
 * - **window**: the SELECT has a clause `LIVE SELECT` refuses, or uses
 *   `VALUE`. A `LIVE SELECT * FROM <target> [WHERE …]` watches the same
 *   records, and each notification re-runs the SELECT itself, so the window
 *   (its order, offset, limit and grouping) is always the one the engine
 *   computes.
 *
 * A text that is not exactly one SELECT with one target, or that selects
 * `FROM ONLY`, cannot be live at all.
 */

/** The plan for one live SELECT. */
export type LivePlan =
	| {
			readonly mode: 'keyed' | 'window';
			/** The `LIVE SELECT …` statement that watches the records. */
			readonly liveText: string;
			/** The SELECT that produces the rows, without a trailing `;`. */
			readonly snapshot: string;
			/** The clauses that put the plan in window mode, upper-cased. */
			readonly windowed: readonly string[];
	  }
	| { readonly mode: 'unsupported'; readonly reason: string };

/** Clauses that end the target list of a SELECT. */
const CLAUSES = new Set([
	'WHERE',
	'SPLIT',
	'GROUP',
	'ORDER',
	'LIMIT',
	'START',
	'FETCH',
	'TIMEOUT',
	'PARALLEL',
	'TEMPFILES',
	'EXPLAIN',
	'WITH',
]);

/** Clauses `LIVE SELECT` accepts after its target. */
const LIVE_CLAUSES = new Set(['WHERE', 'FETCH']);

interface Token {
	/** Upper-cased word, or the punctuation character itself. */
	readonly value: string;
	readonly start: number;
	readonly end: number;
}

/**
 * The tokens of `text` that sit outside every string, comment, bracket and
 * escaped identifier: keywords and identifiers, `,` and `;`. Anything nested in `()`, `[]` or
 * `{}` is skipped whole, so a subquery's clauses are not the outer query's.
 */
export function topLevelTokens(text: string): Token[] {
	const tokens: Token[] = [];
	let depth = 0;
	let i = 0;
	while (i < text.length) {
		const c = text[i] as string;
		const next = text[i + 1];
		if (c === '-' && next === '-') {
			i = lineEnd(text, i);
		} else if (c === '/' && next === '/') {
			i = lineEnd(text, i);
		} else if (c === '#') {
			i = lineEnd(text, i);
		} else if (c === '/' && next === '*') {
			const close = text.indexOf('*/', i + 2);
			i = close < 0 ? text.length : close + 2;
		} else if (c === "'" || c === '"' || c === '`') {
			i = quoted(text, i, c);
		} else if (c === '⟨') {
			const close = text.indexOf('⟩', i + 1);
			i = close < 0 ? text.length : close + 1;
		} else if (c === '(' || c === '[' || c === '{') {
			depth += 1;
			i += 1;
		} else if (c === ')' || c === ']' || c === '}') {
			depth = Math.max(0, depth - 1);
			i += 1;
		} else if (depth === 0 && (c === ',' || c === ';')) {
			tokens.push({ value: c, start: i, end: i + 1 });
			i += 1;
		} else if (isWordStart(c)) {
			const start = i;
			while (i < text.length && isWordPart(text[i] as string)) i += 1;
			// `$name`, `a.name` and `table:name` are not keywords.
			const before = text[start - 1];
			const keyword = before !== '$' && before !== '.' && before !== ':';
			if (depth === 0 && keyword) {
				tokens.push({
					value: text.slice(start, i).toUpperCase(),
					start,
					end: i,
				});
			}
		} else {
			i += 1;
		}
	}
	return tokens;
}

/** Plans `text` as a live query. */
export function planLive(text: string): LivePlan {
	const snapshot = text.trim().replace(/;\s*$/, '').trimEnd();
	const tokens = topLevelTokens(snapshot);
	if (tokens[0]?.value !== 'SELECT') {
		return unsupported('a live query must be a SELECT');
	}
	if (tokens.some((token) => token.value === ';')) {
		return unsupported('a live query must be a single statement');
	}
	const from = tokens.findIndex((token) => token.value === 'FROM');
	if (from < 0) return unsupported('a live query must select FROM a table');
	const windowed = new Set<string>();
	if (tokens[1]?.value === 'VALUE') windowed.add('VALUE');
	if (tokens.slice(1, from).some((token) => token.value === 'OMIT')) {
		windowed.add('OMIT');
	}
	const rest = tokens.slice(from + 1);
	if (rest[0]?.value === 'ONLY') {
		return unsupported('a live query cannot select FROM ONLY');
	}
	const firstClause = rest.findIndex((token) => CLAUSES.has(token.value));
	const targetTokens = firstClause < 0 ? rest : rest.slice(0, firstClause);
	if (targetTokens.some((token) => token.value === ',')) {
		return unsupported('a live query must select FROM one target');
	}
	const fromToken = tokens[from] as Token;
	const targetEnd =
		firstClause < 0 ? snapshot.length : (rest[firstClause] as Token).start;
	const target = snapshot.slice(fromToken.end, targetEnd).trim();
	if (target === '') return unsupported('a live query must name its target');

	let where: string | undefined;
	const clauses = firstClause < 0 ? [] : rest.slice(firstClause);
	for (const [index, token] of clauses.entries()) {
		if (!CLAUSES.has(token.value)) continue;
		if (!LIVE_CLAUSES.has(token.value)) windowed.add(token.value);
		if (token.value === 'WHERE') {
			const end = clauses
				.slice(index + 1)
				.find((later) => CLAUSES.has(later.value));
			where = snapshot
				.slice(token.end, end?.start ?? snapshot.length)
				.trim();
		}
	}

	if (windowed.size === 0) {
		return {
			mode: 'keyed',
			liveText: `LIVE ${snapshot}`,
			snapshot,
			windowed: [],
		};
	}
	return {
		mode: 'window',
		liveText: `LIVE SELECT * FROM ${target}${where ? ` WHERE ${where}` : ''}`,
		snapshot,
		windowed: [...windowed],
	};
}

function unsupported(reason: string): LivePlan {
	return { mode: 'unsupported', reason };
}

function lineEnd(text: string, from: number): number {
	const end = text.indexOf('\n', from);
	return end < 0 ? text.length : end + 1;
}

function quoted(text: string, from: number, quote: string): number {
	let i = from + 1;
	while (i < text.length) {
		const c = text[i];
		if (c === '\\') i += 2;
		else if (c === quote) return i + 1;
		else i += 1;
	}
	return text.length;
}

function isWordStart(c: string): boolean {
	return /[A-Za-z_]/.test(c);
}

function isWordPart(c: string): boolean {
	return /[A-Za-z0-9_]/.test(c);
}
