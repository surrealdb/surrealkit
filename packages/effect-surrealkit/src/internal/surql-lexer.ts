/**
 * A SurrealQL lexer for statement analysis and binding renaming: enough of the language to
 * know, soundly, where strings, comments, identifiers, parameters, brackets
 * and statement boundaries are. Anything it cannot classify is an error,
 * never a guess.
 */
import { Data } from 'effect';

/** Why a text could not be analysed, and where (1-based line and column). */
export class AnalyzeError extends Data.TaggedError('AnalyzeError')<{
	readonly code: string;
	readonly message: string;
	readonly line: number;
	readonly column: number;
	readonly offset: number;
}> {}

export type TokenKind =
	| 'word'
	| 'param'
	| 'string'
	| 'ident'
	| 'regex'
	| 'hole'
	| 'punct';

export interface Token {
	readonly kind: TokenKind;
	/** The source text of the token. */
	readonly text: string;
	/**
	 * Words and punctuation: the text. Parameters: the name without `$`.
	 * Strings and identifiers: the content with escapes resolved. Holes: the
	 * whole hole.
	 */
	readonly value: string;
	/** The prefix of a prefixed string (`r`, `d`, `u`, `s`, `f`, `b`). */
	readonly prefix?: string;
	readonly start: number;
	readonly end: number;
	readonly line: number;
	readonly column: number;
}

const STRING_PREFIXES: ReadonlySet<string> = new Set([
	'r',
	'd',
	'u',
	's',
	'f',
	'b',
]);

const OPERATOR_CHARS = new Set([
	',',
	'.',
	':',
	'|',
	'<',
	'>',
	'=',
	'!',
	'+',
	'-',
	'*',
	'?',
	'@',
	'~',
	'&',
	'^',
	'%',
	// The Unicode operators SurrealQL accepts.
	'×',
	'÷',
	'≤',
	'≥',
	'∋',
	'∌',
	'⊇',
	'⊃',
	'⊅',
	'∈',
	'∉',
	'⊆',
	'⊂',
	'⊄',
]);

const OPEN = new Map([
	['(', ')'],
	['[', ']'],
	['{', '}'],
]);
const CLOSE = new Set([')', ']', '}']);

/**
 * Words after which an expression starts, so a `/` there opens a regex
 * literal rather than dividing.
 */
const EXPRESSION_WORDS: ReadonlySet<string> = new Set([
	'AND',
	'OR',
	'NOT',
	'IS',
	'IN',
	'INSIDE',
	'OUTSIDE',
	'INTERSECTS',
	'CONTAINS',
	'CONTAINSALL',
	'CONTAINSANY',
	'CONTAINSNONE',
	'CONTAINSNOT',
	'ALLINSIDE',
	'ANYINSIDE',
	'NONEINSIDE',
	'NOTINSIDE',
	'RETURN',
	'WHERE',
	'THEN',
	'ELSE',
	'IF',
	'VALUE',
	'SET',
	'WHEN',
	'THROW',
	'SELECT',
	'MATCHES',
	'LIKE',
	'ASSERT',
	'DEFAULT',
]);

const isWordChar = (c: string | undefined): boolean =>
	c !== undefined && /[A-Za-z0-9_]/u.test(c);

const isNameStart = (c: string | undefined): boolean =>
	c !== undefined && /[A-Za-z_]/u.test(c);

export interface Lexed {
	readonly tokens: ReadonlyArray<Token>;
	/** Where an offset is, as 1-based line and column. */
	readonly locate: (offset: number) => {
		readonly line: number;
		readonly column: number;
	};
}

/**
 * Splits SurrealQL into tokens, skipping whitespace and comments. Fails on an
 * unterminated string, identifier or comment, an invalid escape, unbalanced
 * brackets, or a character it cannot classify.
 */
export const lex = (
	sql: string,
	holeEndingAt?: (start: number) => number,
	options?: { readonly unambiguousLineComments?: boolean },
): Lexed => {
	const lineStarts = [0];
	for (let i = 0; i < sql.length; i++) {
		if (sql[i] === '\n') lineStarts.push(i + 1);
	}
	const locate = (offset: number) => {
		let low = 0;
		let high = lineStarts.length - 1;
		while (low < high) {
			const mid = (low + high + 1) >> 1;
			if ((lineStarts[mid] ?? 0) <= offset) low = mid;
			else high = mid - 1;
		}
		return { line: low + 1, column: offset - (lineStarts[low] ?? 0) + 1 };
	};
	const fail = (code: string, message: string, offset: number): never => {
		throw new AnalyzeError({ code, message, offset, ...locate(offset) });
	};

	const tokens: Token[] = [];
	const brackets: Array<{ readonly char: string; readonly offset: number }> =
		[];
	const push = (
		kind: TokenKind,
		start: number,
		end: number,
		value: string,
		prefix?: string,
	) => {
		tokens.push({
			kind,
			text: sql.slice(start, end),
			value,
			...(prefix !== undefined && { prefix }),
			start,
			end,
			...locate(start),
		});
	};

	/** Reads a quoted run from `open + 1`; returns the decoded value and end. */
	const quoted = (
		open: number,
		close: string,
		what: string,
		from = open,
	): { readonly value: string; readonly end: number } => {
		let value = '';
		let i = open + 1;
		while (i < sql.length) {
			const c = sql[i] as string;
			if (c === close) return { value, end: i + 1 };
			if (c === '\\') {
				const next = sql[i + 1];
				const simple: Record<string, string> = {
					'\\': '\\',
					"'": "'",
					'"': '"',
					'`': '`',
					[close]: close,
					'0': '\0',
					b: '\b',
					f: '\f',
					n: '\n',
					r: '\r',
					t: '\t',
				};
				if (next !== undefined && next in simple) {
					value += simple[next];
					i += 2;
					continue;
				}
				if (next === 'u') {
					const braced = /^\{([0-9A-Fa-f]{1,6})\}/u.exec(
						sql.slice(i + 2, i + 12),
					);
					const plain = /^[0-9A-Fa-f]{4}/u.exec(
						sql.slice(i + 2, i + 6),
					);
					const hex = braced?.[1] ?? plain?.[0];
					const point =
						hex === undefined
							? undefined
							: Number.parseInt(hex, 16);
					if (point !== undefined && point <= 0x10ffff) {
						value += String.fromCodePoint(point);
						i += 2 + (braced?.[0].length ?? 4);
						continue;
					}
				}
				fail(
					'invalidEscape',
					`invalid escape sequence \`\\${next ?? ''}\` in ${what}`,
					i,
				);
			}
			value += c;
			i++;
		}
		return fail('unterminated', `unterminated ${what}`, from);
	};

	const operandBefore = (): boolean => {
		const last = tokens[tokens.length - 1];
		if (last === undefined) return false;
		switch (last.kind) {
			case 'param':
			case 'string':
			case 'ident':
			case 'regex':
			case 'hole':
				return true;
			case 'word':
				return !EXPRESSION_WORDS.has(last.value.toUpperCase());
			case 'punct':
				return CLOSE.has(last.value);
		}
	};

	let i = 0;
	while (i < sql.length) {
		const c = sql[i] as string;
		const next = sql[i + 1];
		if (/\s/u.test(c)) {
			i++;
			continue;
		}
		// Comments.
		if (
			(c === '-' && next === '-') ||
			(c === '/' && next === '/') ||
			c === '#'
		) {
			const end = sql.indexOf('\n', i);
			const nextLine = end === -1 ? sql.length : end + 1;
			if (options?.unambiguousLineComments) {
				// Some engine versions also end comments at these separators.
				const text = sql.slice(i, nextLine);
				const separator = text.search(/[\r\u0085\u2028\u2029]/u);
				if (
					separator !== -1 &&
					text.slice(separator + 1).trim() !== ''
				) {
					fail(
						'ambiguousComment',
						'ambiguous line comment ending',
						i + separator,
					);
				}
			}
			i = nextLine;
			continue;
		}
		if (c === '/' && next === '*') {
			const end = sql.indexOf('*/', i + 2);
			if (end === -1) fail('unterminated', 'unterminated comment', i);
			i = end + 2;
			continue;
		}
		// A hole, wherever a token can start.
		const holeAt = holeEndingAt?.(i) ?? -1;
		if (
			holeAt > i &&
			!(isWordChar(sql[holeAt - 1]) && isWordChar(sql[holeAt])) &&
			!(isWordChar(c) && isWordChar(sql[i - 1]))
		) {
			push('hole', i, holeAt, sql.slice(i, holeAt));
			i = holeAt;
			continue;
		}
		if (c === "'" || c === '"') {
			const { value, end } = quoted(i, c, 'string');
			push('string', i, end, value);
			i = end;
			continue;
		}
		if (c === '`') {
			const { value, end } = quoted(i, '`', 'identifier');
			push('ident', i, end, value);
			i = end;
			continue;
		}
		if (c === '⟨') {
			const { value, end } = quoted(i, '⟩', 'identifier');
			push('ident', i, end, value);
			i = end;
			continue;
		}
		if (c === '$') {
			if (isNameStart(next)) {
				let end = i + 1;
				while (isWordChar(sql[end])) end++;
				push('param', i, end, sql.slice(i + 1, end));
				i = end;
				continue;
			}
			if (next === '`' || next === '⟨') {
				const { value, end } = quoted(
					i + 1,
					next === '`' ? '`' : '⟩',
					'parameter name',
					i,
				);
				push('param', i, end, value);
				i = end;
				continue;
			}
			fail(
				'invalidParameter',
				'`$` is not followed by a parameter name',
				i,
			);
		}
		if (isWordChar(c)) {
			let end = i;
			while (isWordChar(sql[end])) end++;
			const word = sql.slice(i, end);
			const quote = sql[end];
			if (STRING_PREFIXES.has(word) && (quote === "'" || quote === '"')) {
				const { value, end: close } = quoted(end, quote, 'string', i);
				push('string', i, close, value, word);
				i = close;
				continue;
			}
			push('word', i, end, word);
			i = end;
			continue;
		}
		if (OPEN.has(c)) {
			brackets.push({ char: c, offset: i });
			push('punct', i, i + 1, c);
			i++;
			continue;
		}
		if (CLOSE.has(c)) {
			const open = brackets.pop();
			if (open === undefined) {
				fail('unbalanced', `\`${c}\` closes nothing`, i);
			} else if (OPEN.get(open.char) !== c) {
				fail(
					'unbalanced',
					`\`${c}\` does not close the \`${open.char}\` at ${(() => {
						const at = locate(open.offset);
						return `${at.line}:${at.column}`;
					})()}`,
					i,
				);
			}
			push('punct', i, i + 1, c);
			i++;
			continue;
		}
		if (c === ';') {
			push('punct', i, i + 1, c);
			i++;
			continue;
		}
		if (c === '/') {
			if (operandBefore()) {
				push('punct', i, i + 1, c);
				i++;
				continue;
			}
			// A regex literal: opaque up to the next unescaped `/`.
			let end = i + 1;
			while (end < sql.length && sql[end] !== '/') {
				if (sql[end] === '\n') break;
				end += sql[end] === '\\' ? 2 : 1;
			}
			if (sql[end] !== '/') {
				fail('unterminated', 'unterminated regex literal', i);
			}
			push('regex', i, end + 1, sql.slice(i + 1, end));
			i = end + 1;
			continue;
		}
		if (OPERATOR_CHARS.has(c)) {
			push('punct', i, i + 1, c);
			i++;
			continue;
		}
		fail(
			'unexpectedCharacter',
			`cannot classify ${JSON.stringify(String.fromCodePoint(sql.codePointAt(i) ?? 0))} outside a string, identifier or comment`,
			i,
		);
	}
	const open = brackets.pop();
	if (open !== undefined) {
		fail('unbalanced', `\`${open.char}\` is never closed`, open.offset);
	}
	return { tokens, locate };
};
