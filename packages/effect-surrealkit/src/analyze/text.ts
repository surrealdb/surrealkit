/**
 * Authored SurrealQL that is not a complete part: a fragment (a single
 * unterminated expression or block) or a template (a text with holes).
 * Neither runs on its own; {@link fill} puts fragments into a template's
 * holes, and the filled text is analysed again.
 */
import { Data, Effect } from 'effect';

import {
	AnalyzeError,
	type HoleDelimiters,
	holeDelimiters,
	lex,
	type Token,
} from './lexer.js';

/** What the text around one hole does with names; see `Analysis.holeScopes`. */
export interface TextHoleScope<
	Bound extends string = string,
	Binds extends string = string,
> {
	readonly bound: ReadonlyArray<Bound>;
	readonly binds: ReadonlyArray<Binds>;
}

/** The scope of each hole of a template, by hole. */
export type TextHoleScopes = { readonly [hole: string]: TextHoleScope };

/**
 * A fragment or template, declared with {@link text}. The names it reads,
 * defines and throws, and the scope of each hole, are kept as literal
 * types, so code that fills it can work out what the filled text reads.
 */
export interface Text<
	Holes extends string = string,
	Free extends string = string,
	Defines extends string = string,
	Throws extends string = string,
	Scopes extends TextHoleScopes = TextHoleScopes,
> {
	readonly kind: 'part' | 'fragment';
	readonly name?: string;
	readonly source?: string;
	readonly sql: string;
	readonly holes: ReadonlyArray<Holes>;
	/** As written; provisional while the text has holes. */
	readonly free: ReadonlyArray<Free>;
	/** As written; provisional while the text has holes. */
	readonly defines: ReadonlyArray<Defines>;
	readonly provisional: boolean;
	readonly slots: number;
	readonly throws: ReadonlyArray<Throws>;
	readonly throwsAt?: Readonly<Record<string, ReadonlyArray<number>>>;
	/** Per hole: see `Analysis.holeScopes`. Written for every template. */
	readonly scopes?: Scopes;
}

/** Declares a {@link Text}, keeping its names as literal types. */
export const text = <
	const Holes extends string = never,
	const Free extends string = never,
	const Defines extends string = never,
	const Throws extends string = never,
	const Scopes extends TextHoleScopes = Record<never, never>,
>(
	options: Text<Holes, Free, Defines, Throws, Scopes>,
): Text<Holes, Free, Defines, Throws, Scopes> => options;

/** Why holes could not be filled. */
export class FillError extends Data.TaggedError('FillError')<{
	readonly code: string;
	readonly message: string;
	readonly hole?: string;
}> {}

export interface FillOptions {
	/** The literal delimiters of a hole; default `__` and `__`. */
	readonly holes?: HoleDelimiters;
	/** Accept fills that still have holes of their own. Default false. */
	readonly allowHoles?: boolean;
}

/** The fills a template takes: one per hole when its holes are known. */
export type Fills<T> =
	T extends Text<infer Holes, string, string, string, TextHoleScopes>
		? string extends Holes
			? Readonly<Record<string, string | Text>>
			: { readonly [K in Holes]: string | Text }
		: Readonly<Record<string, string | Text>>;

const sqlOf = (value: string | Text): string =>
	typeof value === 'string' ? value : value.sql;

/**
 * Substitutes every hole of `template` and returns the new text. Holes are
 * only recognised outside strings and comments. Each fill must lex on its
 * own with balanced brackets, so it cannot close or open anything around
 * its hole; leading and trailing whitespace and comments of a fill are
 * dropped, so a trailing line comment cannot swallow the rest of the line.
 *
 * Fails on a fill for a hole the template does not have, a hole without a
 * fill, or a fill that still has holes (unless `allowHoles`). The result is
 * a new text: analyse it to know what it reads, defines and returns.
 */
export const fill = <const T extends string | Text>(
	template: T,
	fills: Fills<T>,
	options: FillOptions = {},
): Effect.Effect<string, FillError> =>
	Effect.suspend(() => {
		try {
			return Effect.succeed(fillSync(sqlOf(template), fills, options));
		} catch (error) {
			if (error instanceof FillError) return Effect.fail(error);
			throw error;
		}
	});

const lexed = (
	sql: string,
	holes: HoleDelimiters,
	what: string,
	hole?: string,
) => {
	try {
		return lex(sql, holes).tokens;
	} catch (error) {
		if (error instanceof AnalyzeError) {
			throw new FillError({
				code: error.code,
				message: `${what}: ${error.message} at ${error.line}:${error.column}`,
				...(hole !== undefined && { hole }),
			});
		}
		throw error;
	}
};

const fillSync = (
	template: string,
	fills: Readonly<Record<string, string | Text>>,
	options: FillOptions,
): string => {
	let holes: HoleDelimiters;
	try {
		holes = holeDelimiters(options.holes);
	} catch (error) {
		if (error instanceof AnalyzeError) {
			throw new FillError({ code: error.code, message: error.message });
		}
		throw error;
	}
	const tokens = lexed(template, holes, 'the template');
	const used = tokens.filter((token) => token.kind === 'hole');
	const names = new Set(used.map((token) => token.value));
	for (const name of Object.keys(fills).sort()) {
		if (!names.has(name)) {
			throw new FillError({
				code: 'unknownHole',
				message: `the template has no hole ${name}`,
				hole: name,
			});
		}
	}
	const texts = new Map<string, string>();
	for (const name of [...names].sort()) {
		const value = fills[name];
		if (value === undefined) {
			throw new FillError({
				code: 'missingFill',
				message: `no fill for the hole ${name}`,
				hole: name,
			});
		}
		const sql = sqlOf(value);
		const own: ReadonlyArray<Token> = lexed(
			sql,
			holes,
			`the fill for ${name}`,
			name,
		);
		const first = own[0];
		const last = own[own.length - 1];
		if (first === undefined || last === undefined) {
			throw new FillError({
				code: 'emptyFill',
				message: `the fill for ${name} is empty`,
				hole: name,
			});
		}
		const nested = own.find((token) => token.kind === 'hole');
		if (nested !== undefined && options.allowHoles !== true) {
			throw new FillError({
				code: 'nestedHole',
				message: `the fill for ${name} still has the hole ${nested.value}`,
				hole: name,
			});
		}
		texts.set(name, sql.slice(first.start, last.end));
	}
	let out = '';
	let at = 0;
	for (const token of used) {
		out += template.slice(at, token.start) + (texts.get(token.value) ?? '');
		at = token.end;
	}
	return out + template.slice(at);
};
