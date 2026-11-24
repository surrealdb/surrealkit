/**
 * What each free binding of an authored SurrealQL text is used as, in three
 * patterns a lexer can read with certainty: an argument of a custom
 * function, the key of a `type::record`, and a comparison with (or an
 * assignment to) a field of the one table a query targets. Alongside, the
 * field names a query compares whatever the other operand, where that
 * comparison is not a binding use. Anything else records nothing: refuse
 * rather than guess.
 */
import type { Token } from './lexer.js';
import { queryAt, statementsIn } from './statements.js';
import { analyzeTargets } from './targets.js';

/** One use of a binding, at the 1-based line of the parameter. */
export type BindingUse =
	| {
			readonly kind: 'argument';
			readonly function: string;
			readonly index: number;
			readonly line: number;
	  }
	| { readonly kind: 'key'; readonly table: string; readonly line: number }
	| {
			readonly kind: 'field';
			readonly table: string;
			readonly field: string;
			/**
			 * A `WHERE` comparison or a `SET` assignment; the analyzer always
			 * says which, and a declared use may leave it out.
			 */
			readonly role?: 'compare' | 'assign';
			readonly line: number;
	  };

/** The uses of each binding, keyed by name without `$`. */
export type BindingUses = Readonly<Record<string, ReadonlyArray<BindingUse>>>;

/**
 * A field name a query compares in its `WHERE` (`compare`) or assigns in
 * its `SET` (`assign`), at the 1-based line of the name: a bare field of
 * the one static table the query targets, on either side of a comparison
 * whatever the other operand, or the target of `field = <literal>` where
 * the literal is never NONE.
 */
export interface FieldReference {
	readonly table: string;
	readonly field: string;
	readonly role: 'compare' | 'assign';
	readonly line: number;
}

/** What {@link analyzeUses} reads. */
export interface UsesAndFields {
	readonly uses: BindingUses;
	/**
	 * Sorted by line, table, field and role, without duplicates; a
	 * comparison or assignment recorded in `uses` is not repeated here.
	 */
	readonly fields: ReadonlyArray<FieldReference>;
}

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

/** Statement words a query starts with. */
const QUERIES: ReadonlySet<string> = new Set([
	'SELECT',
	'CREATE',
	'UPDATE',
	'UPSERT',
	'DELETE',
	'INSERT',
	'RELATE',
]);

/** Words that are values, not field names. */
const VALUES: ReadonlySet<string> = new Set(['NONE', 'NULL', 'TRUE', 'FALSE']);

/** Words that end a `WHERE` or `SET` clause. */
const CLAUSE_ENDS: ReadonlySet<string> = new Set([
	'WHERE',
	'SET',
	'UNSET',
	'CONTENT',
	'MERGE',
	'PATCH',
	'REPLACE',
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
	'OMIT',
]);

/** Words after which a comparison starts. */
const BEFORE: ReadonlySet<string> = new Set(['WHERE', 'AND', 'OR', 'SET']);

/** Words after which a comparison has ended. */
const AFTER: ReadonlySet<string> = new Set(['AND', 'OR', ...CLAUSE_ENDS]);

const COMPARISONS: ReadonlySet<string> = new Set([
	'=',
	'==',
	'!=',
	'<',
	'<=',
	'>',
	'>=',
]);

/** Characters an operator is spelt with. */
const OPERATOR = '=!<>?~*@&|+-^%/';

/** The names a text binds itself somewhere: never a use of a free binding. */
const localNames = (tokens: ReadonlyArray<Token>): ReadonlySet<string> => {
	const names = new Set<string>();
	tokens.forEach((token, i) => {
		if (token.kind !== 'param') return;
		const before = tokens[i - 1];
		const after = tokens[i + 1];
		if (
			upper(before) === 'LET' ||
			upper(before) === 'FOR' ||
			isPunct(before, '|') ||
			isPunct(after, '|') ||
			isPunct(after, ':')
		)
			names.add(token.value);
	});
	return names;
};

export const analyzeUses = (
	tokens: ReadonlyArray<Token>,
	free: ReadonlyArray<string>,
): UsesAndFields => {
	const local = localNames(tokens);
	const bindable = new Set(free.filter((name) => !local.has(name)));
	const found = new Map<string, Map<string, BindingUse>>();
	/** Records a use of the binding at `token`; false when it is not one. */
	const record = (token: Token | undefined, use: BindingUse): boolean => {
		if (token?.kind !== 'param' || !bindable.has(token.value)) return false;
		const uses = found.get(token.value) ?? new Map<string, BindingUse>();
		uses.set(JSON.stringify(use), use);
		found.set(token.value, uses);
		return true;
	};
	const named = new Map<string, FieldReference>();
	/** The field tokens a recorded binding use compares or assigns. */
	const used = new Set<number>();

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

	const pathAt = (at: number): boolean =>
		isPunct(tokens[at], ':') &&
		isPunct(tokens[at + 1], ':') &&
		adjacent(tokens[at], tokens[at + 1]);

	/** A single parameter token at `at`, not followed by `.` or `[`. */
	const loneParam = (at: number): boolean =>
		tokens[at]?.kind === 'param' &&
		!isPunct(tokens[at + 1], '.') &&
		!isPunct(tokens[at + 1], '[');

	// Arguments of custom functions and keys of `type::record`.
	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i] as Token;
		if (token.kind !== 'word') continue;
		if (isPunct(tokens[i - 1], '.') || isPunct(tokens[i - 1], ':'))
			continue;
		const word = token.value.toLowerCase();
		if (word === 'fn' && pathAt(i + 1)) {
			let name = token.value;
			let j = i + 1;
			while (
				pathAt(j) &&
				(tokens[j + 2]?.kind === 'word' ||
					tokens[j + 2]?.kind === 'ident')
			) {
				name += `::${(tokens[j + 2] as Token).value}`;
				j += 3;
			}
			if (!isPunct(tokens[j], '(')) continue;
			const end = closing(j);
			let start = j + 1;
			let index = 0;
			for (let k = j + 1; k <= end; k++) {
				if (isOpen(tokens[k])) {
					k = closing(k);
					continue;
				}
				if (k === end || isPunct(tokens[k], ',')) {
					if (k - start === 1 && tokens[start]?.kind === 'param')
						record(tokens[start], {
							kind: 'argument',
							function: name,
							index,
							line: (tokens[start] as Token).line,
						});
					index++;
					start = k + 1;
				}
			}
			continue;
		}
		if (
			word === 'type' &&
			pathAt(i + 1) &&
			tokens[i + 3]?.kind === 'word' &&
			['record', 'thing'].includes(
				(tokens[i + 3] as Token).value.toLowerCase(),
			) &&
			isPunct(tokens[i + 4], '(') &&
			isPlainString(tokens[i + 5]) &&
			isPunct(tokens[i + 6], ',') &&
			tokens[i + 7]?.kind === 'param' &&
			isPunct(tokens[i + 8], ')')
		) {
			record(tokens[i + 7], {
				kind: 'key',
				table: (tokens[i + 5] as Token).value,
				line: (tokens[i + 7] as Token).line,
			});
		}
	}

	/**
	 * The comparison operator spelt from `at`: its text and the index past
	 * it, when the run of operator characters is one of {@link COMPARISONS}.
	 */
	const operatorAt = (
		at: number,
	): { readonly text: string; readonly next: number } | undefined => {
		let text = '';
		let j = at;
		while (
			tokens[j]?.kind === 'punct' &&
			OPERATOR.includes((tokens[j] as Token).value) &&
			(j === at || adjacent(tokens[j - 1], tokens[j]))
		) {
			text += (tokens[j] as Token).value;
			j++;
		}
		return COMPARISONS.has(text) ? { text, next: j } : undefined;
	};

	/**
	 * The comparison operator spelt by the run of operator characters that
	 * ends just before `at`, when that run follows the end of an operand
	 * (a word, a name, a parameter, a literal or a closing bracket), so it
	 * is binary: its text, or undefined.
	 */
	const operatorBefore = (at: number): string | undefined => {
		let j = at - 1;
		let text = '';
		while (
			tokens[j]?.kind === 'punct' &&
			OPERATOR.includes((tokens[j] as Token).value) &&
			(j === at - 1 || adjacent(tokens[j], tokens[j + 1]))
		) {
			text = (tokens[j] as Token).value + text;
			j--;
		}
		const operand = tokens[j];
		const ends =
			operand !== undefined &&
			(operand.kind === 'ident' ||
				operand.kind === 'param' ||
				operand.kind === 'string' ||
				isClose(operand) ||
				(operand.kind === 'word' && !startsAfter(j)));
		return ends && COMPARISONS.has(text) ? text : undefined;
	};

	/** A bare field name at `at`: not a path, a call or a record id. */
	const bareField = (at: number): string | undefined => {
		const token = tokens[at];
		if (token === undefined) return undefined;
		if (token.kind === 'word') {
			if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(token.value))
				return undefined;
			const word = token.value.toUpperCase();
			if (VALUES.has(word) || AFTER.has(word)) return undefined;
			// Read as numbers, spelt exactly so; see `projections.ts`.
			if (token.value === 'NaN' || token.value === 'Infinity')
				return undefined;
		} else if (token.kind !== 'ident') return undefined;
		const after = tokens[at + 1];
		if (
			isPunct(after, '.') ||
			isPunct(after, '(') ||
			isPunct(after, '[') ||
			isPunct(after, ':') ||
			(isPunct(after, '-') && isPunct(tokens[at + 2], '>')) ||
			(isPunct(after, '<') && isPunct(tokens[at + 2], '-'))
		)
			return undefined;
		return token.value;
	};

	/** Whether a comparison can start just after the token at `at`. */
	const startsAfter = (at: number): boolean => {
		const token = tokens[at];
		return (
			BEFORE.has(upper(token)) ||
			isPunct(token, '(') ||
			isPunct(token, ',')
		);
	};

	/** `FOR UPDATE` at `at`, which ends a `SELECT` (`lock.ts`). */
	const forUpdateAt = (at: number): boolean =>
		upper(tokens[at]) === 'FOR' && upper(tokens[at + 1]) === 'UPDATE';

	/**
	 * Whether a comparison has ended at the token at `at`, in a query that
	 * ends at `end` (its `;`, the bracket closing it, or the text's end).
	 */
	const endsAt = (at: number, end: number): boolean => {
		const token = tokens[at];
		return (
			at >= end ||
			token === undefined ||
			isPunct(token, ';') ||
			isPunct(token, ')') ||
			isPunct(token, ',') ||
			AFTER.has(upper(token)) ||
			forUpdateAt(at)
		);
	};

	/**
	 * One query from `from` (its first token) to `end` (exclusive): its
	 * comparisons in `WHERE` and its assignments in `SET`, when its targets
	 * are exactly one static table. Subqueries inside it are queries of
	 * their own; any other bracket but `(` makes it uncertain which query a
	 * comparison belongs to, so nothing inside one is recorded. A statement
	 * in a block (`inBlock`) records field names only: it may run for some
	 * values of a binding and not others (`IF $x != NONE { … }`), so a use
	 * there cannot say what the binding must hold.
	 */
	const query = (from: number, end: number, inBlock = false): void => {
		const word = upper(tokens[from]);
		const targets = analyzeTargets(tokens.slice(from, end));
		const tables = new Set([...targets.read, ...targets.written]);
		const table = tables.size === 1 ? [...tables][0] : undefined;
		const certain =
			table !== undefined && !targets.openTargets && QUERIES.has(word);
		/** The clause at the query's own level: `WHERE`, `SET` or another. */
		let clause = '';
		let clauses = 0;
		const scan = (start: number, stop: number, depth: number) => {
			for (let i = start; i < stop; i++) {
				const token = tokens[i] as Token;
				if (isPunct(token, '(')) {
					const close = closing(i);
					if (QUERIES.has(upper(tokens[i + 1]))) query(i + 1, close);
					else scan(i + 1, close, depth + 1);
					i = close;
					continue;
				}
				if (isOpen(token)) {
					const close = closing(i);
					nested(i + 1, close);
					i = close;
					continue;
				}
				if (
					depth === 0 &&
					token.kind === 'word' &&
					CLAUSE_ENDS.has(token.value.toUpperCase()) &&
					!isPunct(tokens[i - 1], '.') &&
					!isPunct(tokens[i - 1], ':') &&
					!isPunct(tokens[i + 1], ':')
				) {
					clause = token.value.toUpperCase();
					if (clause === 'WHERE' || clause === 'SET') clauses++;
					continue;
				}
				if (depth === 0 && forUpdateAt(i)) {
					clause = 'FOR';
					continue;
				}
				if (!certain || table === undefined || clauses > 1) continue;
				const inWhere = clause === 'WHERE';
				const inSet =
					clause === 'SET' &&
					depth === 0 &&
					(word === 'UPDATE' ||
						word === 'UPSERT' ||
						word === 'CREATE');
				if (!inWhere && !inSet) continue;
				if (!inBlock) bindingUse(i, table, inWhere);
				if (inWhere) compared(i, table);
				else assigned(i, table);
			}
		};
		/** The use of a binding that a comparison or assignment at `i` is. */
		const bindingUse = (i: number, table: string, inWhere: boolean) => {
			const token = tokens[i] as Token;
			if (!startsAfter(i - 1)) return;
			// `IDENT OP $name`
			const field = bareField(i);
			if (field !== undefined) {
				const op = operatorAt(i + 1);
				if (
					op !== undefined &&
					(inWhere || op.text === '=') &&
					loneParam(op.next) &&
					endsAt(op.next + 1, end) &&
					record(tokens[op.next], {
						kind: 'field',
						table,
						field,
						role: inWhere ? 'compare' : 'assign',
						line: (tokens[op.next] as Token).line,
					})
				)
					used.add(i);
				return;
			}
			// `$name OP IDENT`
			if (inWhere && loneParam(i)) {
				const op = operatorAt(i + 1);
				if (op === undefined) return;
				const other = bareField(op.next);
				if (
					other !== undefined &&
					endsAt(op.next + 1, end) &&
					record(token, {
						kind: 'field',
						table,
						field: other,
						role: 'compare',
						line: token.line,
					})
				)
					used.add(op.next);
			}
		};
		/**
		 * A bare field at `i` that is a whole operand of a comparison: after
		 * the start of one and before a comparison operator, or after a
		 * binary comparison operator and before the comparison's end. The
		 * other operand can be anything.
		 */
		const compared = (i: number, table: string) => {
			const field = bareField(i);
			if (field === undefined || used.has(i)) return;
			const left = startsAfter(i - 1) && operatorAt(i + 1) !== undefined;
			const right = operatorBefore(i) !== undefined && endsAt(i + 1, end);
			if (!left && !right) return;
			name(i, table, field, 'compare');
		};
		/**
		 * A bare field at `i` that is the target of one assignment of `SET`
		 * (after `SET` or a `,` of the clause, then `=`), when the value is a
		 * literal that is never NONE and nothing follows it in the
		 * assignment: every record the statement writes then fails on a
		 * field the table does not declare. A value that can be NONE is not
		 * read, since assigning NONE to such a field is accepted (and clears
		 * a field a row kept from before its table became SCHEMAFULL).
		 */
		const assigned = (i: number, table: string) => {
			const after = upper(tokens[i - 1]);
			if (after !== 'SET' && !isPunct(tokens[i - 1], ',')) return;
			const field = bareField(i);
			const op = operatorAt(i + 1);
			if (field === undefined || op?.text !== '=') return;
			const past = neverNone(op.next);
			if (past === undefined || !assignmentEndsAt(past)) return;
			name(i, table, field, 'assign');
		};
		/** Whether an assignment of `SET` has ended at `at`. */
		const assignmentEndsAt = (at: number): boolean => {
			const token = tokens[at];
			return (
				at >= end ||
				token === undefined ||
				isPunct(token, ';') ||
				isPunct(token, ')') ||
				isPunct(token, '}') ||
				isPunct(token, ',') ||
				(CLAUSE_ENDS.has(upper(token)) && !isPunct(tokens[at + 1], ':'))
			);
		};
		scan(from, end, 0);
	};

	const name = (
		i: number,
		table: string,
		field: string,
		role: FieldReference['role'],
	) => {
		const line = (tokens[i] as Token).line;
		named.set(JSON.stringify([line, table, field, role]), {
			table,
			field,
			role,
			line,
		});
	};

	/** A word that is a number: digits first (`1`, `1d`, `10dec`, `0x1f`). */
	const numberAt = (at: number): boolean =>
		tokens[at]?.kind === 'word' && /^[0-9]/u.test(tokens[at]?.value ?? '');

	/**
	 * The index just past a literal at `at` that SurrealDB never evaluates
	 * to NONE, or undefined: a string of any prefix, a number (with a
	 * fraction or a minus sign written against it), `TRUE`, `FALSE` or
	 * `NULL`, an array literal, or a record id `table:key` whose key is one
	 * token or one `[…]`. Anything that follows it is the caller's to read.
	 */
	const neverNone = (at: number): number | undefined => {
		const token = tokens[at];
		if (token === undefined) return undefined;
		if (token.kind === 'string') return at + 1;
		if (isPunct(token, '[')) return closing(at) + 1;
		let j = at;
		if (isPunct(token, '-') && adjacent(token, tokens[at + 1])) j++;
		if (numberAt(j)) {
			if (
				isPunct(tokens[j + 1], '.') &&
				numberAt(j + 2) &&
				adjacent(tokens[j], tokens[j + 1]) &&
				adjacent(tokens[j + 1], tokens[j + 2])
			)
				return j + 3;
			return j + 1;
		}
		if (j !== at || (token.kind !== 'word' && token.kind !== 'ident'))
			return undefined;
		const word = token.value.toUpperCase();
		if (
			token.kind === 'word' &&
			(word === 'TRUE' || word === 'FALSE' || word === 'NULL') &&
			!isPunct(tokens[at + 1], ':')
		)
			return at + 1;
		// A record id: `table:key`, the key one token or one `[…]`.
		if (
			/^[A-Za-z_]/u.test(token.value) &&
			isPunct(tokens[at + 1], ':') &&
			!isPunct(tokens[at + 2], ':')
		) {
			const key = tokens[at + 2];
			if (isPunct(key, '[')) return closing(at + 2) + 1;
			if (
				key !== undefined &&
				(key.kind === 'word' ||
					key.kind === 'ident' ||
					key.kind === 'string')
			)
				return at + 3;
		}
		return undefined;
	};

	/**
	 * Tokens whose query is not certain: only subqueries inside count, and
	 * the statements of the blocks inside.
	 */
	const nested = (start: number, stop: number): void => {
		for (let i = start; i < stop; i++) {
			if (!isOpen(tokens[i])) continue;
			const close = closing(i);
			if (isPunct(tokens[i], '(') && QUERIES.has(upper(tokens[i + 1])))
				query(i + 1, close);
			else if (isPunct(tokens[i], '{')) statements(i + 1, close, true);
			else nested(i + 1, close);
			i = close;
		}
	};

	/**
	 * The statements from `start` to `stop` at one level: a query when its
	 * value starts with a query word (possibly after `LET $name =` or
	 * `RETURN`); in any statement, the subqueries and blocks inside it.
	 */
	const statements = (start: number, stop: number, inBlock: boolean) => {
		for (const statement of statementsIn(tokens, start, stop)) {
			if (queryAt(tokens, statement, QUERIES))
				query(statement.first, statement.end, inBlock);
			else nested(statement.start, statement.end);
		}
	};

	// Top-level statements, and in each block the statements of its body.
	statements(0, tokens.length, false);

	const uses: Record<string, ReadonlyArray<BindingUse>> = {};
	for (const name of [...found.keys()].sort()) {
		uses[name] = [...(found.get(name) as Map<string, BindingUse>).entries()]
			.sort(([a, x], [b, y]) =>
				x.line !== y.line
					? x.line - y.line
					: a < b
						? -1
						: a > b
							? 1
							: 0,
			)
			.map(([, use]) => use);
	}
	const fields = [...named.values()].sort((a, b) =>
		a.line !== b.line
			? a.line - b.line
			: a.table !== b.table
				? a.table < b.table
					? -1
					: 1
				: a.field !== b.field
					? a.field < b.field
						? -1
						: 1
					: a.role < b.role
						? -1
						: a.role > b.role
							? 1
							: 0,
	);
	return { uses, fields };
};
