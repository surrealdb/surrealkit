import {
	AnalyzeError,
	type Lexed,
	lex as tokenize,
} from '../internal/surql-lexer.js';

export {
	AnalyzeError,
	type Lexed,
	type Token,
	type TokenKind,
} from '../internal/surql-lexer.js';

/**
 * What a template hole looks like: a name of capitals, digits and
 * underscores, starting with a capital, between two literal delimiters
 * (`__NAME__` by default). Delimiters are matched as text, never compiled.
 */
export interface HoleDelimiters {
	readonly open: string;
	readonly close: string;
}

/** The default delimiters: `__NAME__`. */
export const DEFAULT_HOLE_DELIMITERS: HoleDelimiters = {
	open: '__',
	close: '__',
};

const isCapital = (c: string | undefined): boolean =>
	c !== undefined && c >= 'A' && c <= 'Z';

const isHoleNameChar = (c: string | undefined): boolean =>
	isCapital(c) || (c !== undefined && c >= '0' && c <= '9') || c === '_';

const isBlank = (c: string): boolean => c.trim().length === 0;

/**
 * Checks hole delimiters: each must be a non-empty run of text without
 * whitespace, so a hole is one token. `undefined` is the default.
 */
export const holeDelimiters = (
	holes: HoleDelimiters | undefined,
): HoleDelimiters => {
	if (holes === undefined) return DEFAULT_HOLE_DELIMITERS;
	for (const [which, text] of [
		['open', holes.open],
		['close', holes.close],
	] as const) {
		if (
			typeof text !== 'string' ||
			text.length === 0 ||
			[...text].some(isBlank)
		) {
			throw new AnalyzeError({
				code: 'invalidHoleDelimiters',
				message: `the ${which} delimiter of a hole must be non-empty text without whitespace`,
				line: 1,
				column: 1,
				offset: 0,
			});
		}
	}
	return { open: holes.open, close: holes.close };
};

/**
 * The end of the hole that starts at `at`, or -1 when none does. The name
 * is the longest run that the close delimiter follows, as the pattern
 * `open[A-Z][A-Z0-9_]*close` would match it. A scan, never a regular
 * expression, whatever the delimiters are.
 */
const holeEnd = (sql: string, at: number, holes: HoleDelimiters): number => {
	if (!sql.startsWith(holes.open, at)) return -1;
	const name = at + holes.open.length;
	if (!isCapital(sql[name])) return -1;
	let run = name + 1;
	while (run < sql.length && isHoleNameChar(sql[run])) run++;
	for (let end = run; end > name; end--) {
		if (sql.startsWith(holes.close, end)) return end + holes.close.length;
	}
	return -1;
};

export const lex = (sql: string, holes: HoleDelimiters): Lexed =>
	tokenize(sql, (start) => holeEnd(sql, start, holes));
