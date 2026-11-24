/**
 * The fields a `SELECT` projects from the one static table it reads: each
 * bare field name and `*`, in the patterns a lexer can read with certainty.
 * Anything else records nothing: refuse rather than guess.
 */
import type { Token } from './lexer.js';
import { lockOf } from './lock.js';
import { queryAt, statementsIn } from './statements.js';

/** One projected field (`*` for all of them), at the 1-based line of its name. */
export interface ProjectedField {
	readonly table: string;
	readonly field: string;
	readonly line: number;
}

const upper = (token: Token | undefined): string =>
	token?.kind === 'word' ? token.value.toUpperCase() : '';

const isPunct = (token: Token | undefined, char: string): boolean =>
	token?.kind === 'punct' && token.value === char;

const isOpen = (token: Token | undefined): boolean =>
	isPunct(token, '(') || isPunct(token, '[') || isPunct(token, '{');

const isClose = (token: Token | undefined): boolean =>
	isPunct(token, ')') || isPunct(token, ']') || isPunct(token, '}');

const isPlainString = (token: Token | undefined): token is Token =>
	token?.kind === 'string' &&
	(token.prefix === undefined || token.prefix === 's');

/**
 * Words that may follow a `SELECT`'s target, each read through because it
 * changes neither the target nor the projection: both engines return the
 * same fields with it as without it (`test/projections-engine.test.ts`).
 * `PARALLEL` is not one: both engines refuse it as a parse error. `FOR
 * UPDATE` is read by {@link lockOf}, after record ids only.
 */
const AFTER_TARGET: ReadonlySet<string> = new Set([
	'WHERE',
	'SPLIT',
	'GROUP',
	'ORDER',
	'LIMIT',
	'START',
	'FETCH',
	'TIMEOUT',
	'EXPLAIN',
	'WITH',
	'VERSION',
	'TEMPFILES',
]);

/**
 * Bare words SurrealDB 3.3 and 4.0 read as values, not as field names: four
 * keywords in any case, and `NaN` and `Infinity` spelt exactly so
 * (`test/projections-engine.test.ts` tries every keyword of both parsers).
 */
const VALUES: ReadonlySet<string> = new Set(['NONE', 'NULL', 'TRUE', 'FALSE']);
const EXACT_VALUES: ReadonlySet<string> = new Set(['NaN', 'Infinity']);

export const analyzeProjections = (
	tokens: ReadonlyArray<Token>,
): ReadonlyArray<ProjectedField> => {
	const found = new Map<string, ProjectedField>();

	/** The index of the bracket closing the one at `at`. */
	const closing = (at: number): number => {
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

	/** A bare field name at `at`, as one token: a word or a quoted name. */
	const bareName = (at: number): string | undefined => {
		const token = tokens[at];
		if (token?.kind === 'ident') return token.value;
		if (token?.kind !== 'word') return undefined;
		if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(token.value)) return undefined;
		return VALUES.has(token.value.toUpperCase()) ||
			EXACT_VALUES.has(token.value)
			? undefined
			: token.value;
	};

	/**
	 * The table of the one static target from `at` up to `end`: a table
	 * name, a record id `table:key` (one token or one bracket group), or
	 * `type::record`, `type::thing` or `type::table` with a plain string
	 * literal, after an optional `ONLY`, and followed by the query's end or
	 * one of its clauses. Anything else, a second target included, is not
	 * certain. With a `FOR UPDATE` at `lock`, the target must be a record
	 * id, the only target both engines lock.
	 */
	const targetTable = (
		at: number,
		end: number,
		lock: number | undefined,
	): string | undefined => {
		let j = at;
		if (upper(tokens[j]) === 'ONLY') j++;
		const token = tokens[j];
		let table: string | undefined;
		let record = false;
		if (
			token?.kind === 'word' &&
			token.value.toLowerCase() === 'type' &&
			isPunct(tokens[j + 1], ':')
		) {
			const fn = tokens[j + 3];
			const name = fn?.kind === 'word' ? fn.value.toLowerCase() : '';
			if (
				!isPunct(tokens[j + 2], ':') ||
				!['record', 'thing', 'table'].includes(name) ||
				!isPunct(tokens[j + 4], '(') ||
				!isPlainString(tokens[j + 5]) ||
				!isPunct(tokens[j + 6], name === 'table' ? ')' : ',')
			)
				return undefined;
			table = (tokens[j + 5] as Token).value;
			record = name !== 'table';
			j = closing(j + 4) + 1;
		} else {
			table = bareName(j);
			if (table === undefined || AFTER_TARGET.has(upper(token)))
				return undefined;
			j++;
			if (isPunct(tokens[j], ':')) {
				const key = tokens[j + 1];
				if (key === undefined || (key.kind === 'punct' && !isOpen(key)))
					return undefined;
				j = isOpen(key) ? closing(j + 1) + 1 : j + 2;
				record = true;
			}
		}
		if (lock !== undefined && !record) return undefined;
		const next = tokens[j];
		return j >= end ||
			j === lock ||
			isPunct(next, ';') ||
			AFTER_TARGET.has(upper(next))
			? table
			: undefined;
	};

	const record = (table: string, field: string, line: number): void => {
		found.set(JSON.stringify([line, table, field]), { table, field, line });
	};

	/**
	 * Every parenthesised query in `start`..`stop`, each a query of its own,
	 * and every statement of a `{…}` block there.
	 */
	const nested = (start: number, stop: number): void => {
		for (let i = start; i < stop; i++) {
			if (!isOpen(tokens[i])) continue;
			const close = closing(i);
			if (isPunct(tokens[i], '(') && upper(tokens[i + 1]) === 'SELECT')
				select(i + 1, close);
			else if (isPunct(tokens[i], '{')) statements(i + 1, close);
			else nested(i + 1, close);
			i = close;
		}
	};

	/**
	 * The statements from `start` to `stop` at one level: a query when its
	 * value is a SELECT (possibly after `LET $name =` or `RETURN`); in any
	 * statement, the parenthesised queries and blocks inside it.
	 */
	const SELECT: ReadonlySet<string> = new Set(['SELECT']);
	const statements = (start: number, stop: number): void => {
		for (const statement of statementsIn(tokens, start, stop)) {
			if (queryAt(tokens, statement, SELECT))
				select(statement.first, statement.end);
			else nested(statement.start, statement.end);
		}
	};

	/** The `SELECT` from `from` (its first token) to `end` (exclusive). */
	const select = (from: number, end: number): void => {
		nested(from + 1, end);
		const froms: number[] = [];
		for (let j = from + 1; j < end; j++) {
			if (isOpen(tokens[j])) {
				j = closing(j);
				continue;
			}
			if (
				upper(tokens[j]) === 'FROM' &&
				!isPunct(tokens[j - 1], '.') &&
				!isPunct(tokens[j - 1], ':')
			)
				froms.push(j);
		}
		// No FROM, or a field named `from`: not certain where the list ends.
		if (froms.length !== 1) return;
		const at = froms[0] as number;
		const lock = lockOf(tokens, from, end);
		if (lock.kind === 'refused') return;
		// `PARALLEL`, a parse error on both engines, anywhere after FROM.
		for (let j = at + 1; j < end; j++) {
			if (isOpen(tokens[j])) j = closing(j);
			else if (
				upper(tokens[j]) === 'PARALLEL' &&
				!isPunct(tokens[j - 1], '.') &&
				!isPunct(tokens[j - 1], ':')
			)
				return;
		}
		const table = targetTable(
			at + 1,
			end,
			lock.kind === 'locked' ? lock.at : undefined,
		);
		if (table === undefined) return;
		const fields: Array<{ field: string; line: number }> = [];
		if (upper(tokens[from + 1]) === 'VALUE') {
			// `SELECT VALUE <one bare field> FROM`, and nothing else.
			const name = at === from + 3 ? bareName(from + 2) : undefined;
			if (name !== undefined)
				record(table, name, (tokens[from + 2] as Token).line);
			return;
		}
		let start = from + 1;
		for (let k = from + 1; k <= at; k++) {
			if (k < at && isOpen(tokens[k])) {
				k = closing(k);
				continue;
			}
			if (k < at && upper(tokens[k]) === 'OMIT') return;
			if (k !== at && !isPunct(tokens[k], ',')) continue;
			const item = tokens[start] as Token;
			// `*`, one bare field, or one bare field `AS` one name: the
			// field, never the alias.
			const name =
				k - start === 1
					? isPunct(item, '*')
						? '*'
						: bareName(start)
					: k - start === 3 &&
							upper(tokens[start + 1]) === 'AS' &&
							bareName(start + 2) !== undefined
						? bareName(start)
						: undefined;
			if (name !== undefined)
				fields.push({ field: name, line: item.line });
			start = k + 1;
		}
		for (const { field, line } of fields) record(table, field, line);
	};

	// Top-level statements, and in each block the statements of its body.
	statements(0, tokens.length);

	return [...found.values()].sort((a, b) =>
		a.line !== b.line
			? a.line - b.line
			: a.table !== b.table
				? a.table < b.table
					? -1
					: 1
				: a.field < b.field
					? -1
					: a.field > b.field
						? 1
						: 0,
	);
};
