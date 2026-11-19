/**
 * What an authored SurrealQL text does with variables, results and `THROW`s,
 * derived from its tokens: the facts a {@link Part} declares.
 */
import { Effect } from 'effect';

import {
	AnalyzeError,
	type HoleDelimiters,
	holeDelimiters,
	type Lexed,
	lex,
	type Token,
} from './lexer.js';

export interface Diagnostic {
	readonly severity: 'error' | 'warning';
	readonly code: string;
	readonly message: string;
	readonly line: number;
	readonly column: number;
	readonly offset: number;
}

export interface AnalyzeOptions {
	/**
	 * The literal delimiters of a hole, outside strings and comments, around
	 * a name matching `[A-Z][A-Z0-9_]*`. Default `__` and `__`.
	 */
	readonly holes?: HoleDelimiters;
}

export interface Analysis {
	/**
	 * `part`: every statement ends with `;`. `fragment`: a single
	 * unterminated expression or block, which returns nothing on its own and
	 * may only fill a hole.
	 */
	readonly kind: 'part' | 'fragment';
	/** The text without whitespace and comments after its last token. */
	readonly sql: string;
	/** `$names` read and not defined earlier in scope, sorted, without `$`. */
	readonly free: ReadonlyArray<string>;
	/** Names a top-level `LET` defines, sorted. */
	readonly defines: ReadonlyArray<string>;
	/** Results: one per terminated top-level statement. */
	readonly slots: number;
	/**
	 * The 1-based position, among the results, of the last top-level
	 * statement that is not `BEGIN`, `COMMIT` or `CANCEL`: the result a
	 * client decodes. Equal to `slots` without transaction control; 0 for a
	 * fragment or a text of only control statements.
	 */
	readonly valueSlot: number;
	/** String-literal `THROW` messages, sorted. */
	readonly throws: ReadonlyArray<string>;
	/** The lines each message is thrown on. */
	readonly throwsAt: Readonly<Record<string, ReadonlyArray<number>>>;
	/** Hole names, sorted. A text with holes is a template. */
	readonly holes: ReadonlyArray<string>;
	/**
	 * True when the text has holes: what fills them can read or define
	 * variables, so `free` and `defines` hold only for the text as written.
	 */
	readonly provisional: boolean;
	/** The text writes its own `BEGIN`, `COMMIT` or `CANCEL`. */
	readonly transactional: boolean;
	/**
	 * The text can change data or schema: `CREATE`, `INSERT`, `UPDATE`,
	 * `UPSERT`, `DELETE`, `RELATE`, `DEFINE`, `REMOVE` or `ALTER` starts a
	 * statement or a subquery somewhere in it, outside strings and comments.
	 */
	readonly writes: boolean;
	readonly diagnostics: ReadonlyArray<Diagnostic>;
}

/** Set by the engine: never free, and a `LET` of one is refused. */
const PROTECTED: ReadonlySet<string> = new Set([
	'auth',
	'access',
	'session',
	'token',
]);

/** Set by the engine wherever they are read: never free. */
const IMPLICIT: ReadonlySet<string> = new Set(['this', 'self', 'parent']);

/**
 * Set by the engine inside field, event and other definitions, and inside
 * some clauses; ordinary names elsewhere in a script.
 */
const CONTEXTUAL: ReadonlySet<string> = new Set([
	'value',
	'input',
	'before',
	'after',
	'event',
]);

/** Words that start a statement, which a fragment cannot be. */
const STATEMENT_WORDS: ReadonlySet<string> = new Set([
	'LET',
	'BEGIN',
	'COMMIT',
	'CANCEL',
	'DEFINE',
	'REMOVE',
	'ALTER',
	'USE',
	'OPTION',
]);

/** Words that start a statement that can change data or schema. */
const WRITE_WORDS: ReadonlySet<string> = new Set([
	'CREATE',
	'INSERT',
	'UPDATE',
	'UPSERT',
	'DELETE',
	'RELATE',
	'DEFINE',
	'REMOVE',
	'ALTER',
]);

/** Words after which a statement can stand as a value. */
const VALUE_WORDS: ReadonlySet<string> = new Set(['RETURN', 'THEN', 'ELSE']);

const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/u;

type Role = 'root' | 'block' | 'function' | 'closure' | 'params' | 'other';

interface Frame {
	readonly open: string;
	readonly role: Role;
	readonly vars: Set<string>;
	statementStart: boolean;
	pendingLet?: string | undefined;
	pendingBody?:
		| { readonly vars: ReadonlyArray<string>; readonly role: Role }
		| undefined;
	awaitParams?: boolean | undefined;
	readonly params: string[];
}

const frame = (open: string, role: Role, vars: Iterable<string> = []) =>
	({
		open,
		role,
		vars: new Set(vars),
		statementStart: open === '{' || open === 'root',
		params: [],
	}) satisfies Frame as Frame;

const isWord = (token: Token | undefined, word: string): boolean =>
	token?.kind === 'word' && token.value.toUpperCase() === word;

const isPunct = (token: Token | undefined, char: string): boolean =>
	token?.kind === 'punct' && token.value === char;

const sorted = (values: Iterable<string>): ReadonlyArray<string> =>
	[...new Set(values)].sort();

const analyzeLexed = (sql: string, lexed: Lexed): Analysis => {
	const { tokens, locate } = lexed;
	const diagnostics: Diagnostic[] = [];
	const diagnose = (
		severity: Diagnostic['severity'],
		code: string,
		message: string,
		token: Token,
	) =>
		diagnostics.push({
			severity,
			code,
			message,
			line: token.line,
			column: token.column,
			offset: token.start,
		});
	const fail = (code: string, message: string, token: Token): never => {
		throw new AnalyzeError({
			code,
			message,
			line: token.line,
			column: token.column,
			offset: token.start,
		});
	};

	const first = tokens[0];
	const last = tokens[tokens.length - 1];
	if (first === undefined || last === undefined) {
		throw new AnalyzeError({
			code: 'empty',
			message: 'the text has no statements',
			...locate(0),
			offset: 0,
		});
	}

	const root = frame('root', 'root');
	const frames: Frame[] = [root];
	const closures: Array<{
		readonly depth: number;
		readonly vars: ReadonlySet<string>;
	}> = [];
	const free = new Set<string>();
	const warned = new Set<string>();
	const holes = new Set<string>();
	const throwsAt = new Map<string, Set<number>>();
	const skip = new Set<number>();

	let slots = 0;
	let valueSlot = 0;
	let control = false;
	let transactional = false;
	let writes = false;
	let statementFirst: Token | undefined;
	let inDefinition = false;
	let inTransaction = false;
	let returnInTransaction: Token | undefined;

	const top = () => frames[frames.length - 1] as Frame;
	const visible = (name: string) =>
		closures.some((scope) => scope.vars.has(name)) ||
		frames.some((f) => f.vars.has(name));
	const endClosures = (depth: number) => {
		while (
			closures.length > 0 &&
			(closures[closures.length - 1]?.depth ?? 0) >= depth
		)
			closures.pop();
	};
	const checkName = (name: string, token: Token) => {
		if (!identifier.test(name)) {
			diagnose(
				'error',
				'unbindableName',
				`\`$${name}\` is not a name a statement can bind or define: use letters, digits and underscores`,
				token,
			);
		}
	};

	const startStatement = (token: Token, current: Frame) => {
		if (current === root) {
			statementFirst = token;
			const word = token.kind === 'word' ? token.value.toUpperCase() : '';
			if (returnInTransaction !== undefined) {
				if (word !== 'COMMIT' && word !== 'CANCEL') {
					diagnose(
						'error',
						'returnInTransaction',
						'a RETURN inside BEGIN … COMMIT is followed by another statement: SurrealDB 3 skips the statements after it and SurrealDB 4 runs them',
						returnInTransaction,
					);
				}
				returnInTransaction = undefined;
			}
			control =
				word === 'BEGIN' || word === 'COMMIT' || word === 'CANCEL';
			if (word === 'BEGIN') {
				transactional = true;
				inTransaction = true;
			} else if (word === 'COMMIT' || word === 'CANCEL') {
				transactional = true;
				inTransaction = false;
			}
			if (word === 'DEFINE') {
				const kind = tokens[tokens.indexOf(token) + 1];
				inDefinition =
					!isWord(kind, 'FUNCTION') && !isWord(kind, 'PARAM');
			}
		}
		if (
			isWord(token, 'RETURN') &&
			inTransaction &&
			returnInTransaction === undefined &&
			frames.every((f) => f.role === 'root' || f.role === 'block')
		) {
			returnInTransaction = token;
		}
	};

	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i] as Token;
		const current = top();
		if (skip.has(i)) continue;
		let starting = false;
		if (!isPunct(token, ';')) {
			starting =
				current === root
					? statementFirst === undefined
					: current.statementStart;
			if (starting) startStatement(token, current);
			current.statementStart = false;
		}
		switch (token.kind) {
			case 'hole':
				holes.add(token.value);
				break;
			case 'param': {
				const name = token.value;
				if (current.role === 'params') {
					current.params.push(name);
					break;
				}
				if (visible(name) || PROTECTED.has(name) || IMPLICIT.has(name))
					break;
				if (inDefinition && CONTEXTUAL.has(name)) break;
				if (!free.has(name)) {
					free.add(name);
					checkName(name, token);
					if (CONTEXTUAL.has(name) && !warned.has(name)) {
						warned.add(name);
						diagnose(
							'warning',
							'contextualName',
							`\`$${name}\` is read here without a LET; SurrealDB sets it inside definitions and some clauses, and \`sequence\` refuses it as a binding, so LET it in an earlier part`,
							token,
						);
					}
				}
				break;
			}
			case 'word': {
				const word = token.value.toUpperCase();
				const next = tokens[i + 1];
				const previous = tokens[i - 1];
				const member =
					isPunct(previous, '.') ||
					isPunct(previous, ':') ||
					isPunct(next, ':');
				if (member) break;
				if (
					WRITE_WORDS.has(word) &&
					(starting ||
						isPunct(previous, '(') ||
						(previous?.kind === 'word' &&
							VALUE_WORDS.has(previous.value.toUpperCase())) ||
						(isPunct(previous, '=') &&
							tokens[i - 2]?.kind === 'param' &&
							isWord(tokens[i - 3], 'LET')))
				) {
					writes = true;
				}
				if (word === 'LET' && next?.kind === 'param') {
					skip.add(i + 1);
					if (PROTECTED.has(next.value)) {
						diagnose(
							'error',
							'protectedLet',
							`\`$${next.value}\` is set by SurrealDB and cannot be defined`,
							next,
						);
					} else {
						if (current === root) checkName(next.value, next);
						current.pendingLet = next.value;
					}
				} else if (
					word === 'FOR' &&
					next?.kind === 'param' &&
					isWord(tokens[i + 2], 'IN')
				) {
					skip.add(i + 1);
					current.pendingBody = { vars: [next.value], role: 'block' };
				} else if (word === 'DEFINE' && isWord(next, 'FUNCTION')) {
					current.awaitParams = true;
				} else if (
					(word === 'DEFINE' || word === 'REMOVE') &&
					isWord(next, 'PARAM')
				) {
					const at = tokens.findIndex(
						(t, j) => j > i && t.kind === 'param',
					);
					const param = tokens[at];
					if (param !== undefined && at <= i + 6) {
						skip.add(at);
						if (word === 'DEFINE') {
							diagnose(
								'warning',
								'defineParam',
								`DEFINE PARAM \`$${param.value}\` stores a database parameter; a binding of the same name overrides it in this request, and the analysis does not treat later reads as defined`,
								param,
							);
						}
					}
				} else if (word === 'THROW') {
					const after = tokens[i + 2];
					if (
						next?.kind === 'string' &&
						(next.prefix === undefined || next.prefix === 's') &&
						(after === undefined ||
							isPunct(after, ';') ||
							isPunct(after, '}') ||
							isPunct(after, ')'))
					) {
						const lines = throwsAt.get(next.value) ?? new Set();
						lines.add(token.line);
						throwsAt.set(next.value, lines);
					} else {
						diagnose(
							'warning',
							'dynamicThrow',
							'THROW of something other than a string literal: its message is not declared, so a client reports it as an unexpected throw',
							token,
						);
					}
				}
				break;
			}
			case 'punct': {
				const char = token.value;
				if (char === ';') {
					if (current === root) {
						if (statementFirst === undefined) {
							fail('emptyStatement', 'an empty statement', token);
						}
						slots++;
						if (!control) valueSlot = slots;
						statementFirst = undefined;
						inDefinition = false;
					} else {
						current.statementStart = current.open === '{';
					}
					if (current.pendingLet !== undefined) {
						current.vars.add(current.pendingLet);
						current.pendingLet = undefined;
					}
					current.pendingBody = undefined;
					current.awaitParams = false;
					endClosures(frames.length);
				} else if (char === ',') {
					endClosures(frames.length);
				} else if (char === '(' || char === '[' || char === '{') {
					const body = char === '{' ? current.pendingBody : undefined;
					if (char === '{') current.pendingBody = undefined;
					const params = char === '(' && current.awaitParams === true;
					if (params) current.awaitParams = false;
					frames.push(
						params
							? frame(char, 'params')
							: body !== undefined
								? frame(char, body.role, body.vars)
								: frame(char, char === '{' ? 'block' : 'other'),
					);
				} else if (char === ')' || char === ']' || char === '}') {
					const closed = frames.pop() as Frame;
					endClosures(frames.length + 1);
					if (closed.role === 'params') {
						top().pendingBody = {
							vars: closed.params,
							role: 'function',
						};
					}
				} else if (char === '|' && tokens[i + 1]?.kind === 'param') {
					// Closure parameters: `|$a, $b: type| body`.
					const names: string[] = [];
					let j = i + 1;
					for (;;) {
						const param = tokens[j];
						if (param?.kind !== 'param') {
							return fail(
								'unexpectedToken',
								'cannot read this closure parameter list',
								param ?? token,
							);
						}
						names.push(param.value);
						j++;
						if (isPunct(tokens[j], ':')) {
							let angle = 0;
							j++;
							while (j < tokens.length) {
								const t = tokens[j] as Token;
								if (isPunct(t, '<')) angle++;
								else if (isPunct(t, '>')) angle--;
								else if (
									angle === 0 &&
									(isPunct(t, ',') || isPunct(t, '|'))
								)
									break;
								else if (
									t.kind === 'punct' &&
									'()[]{};'.includes(t.value)
								)
									return fail(
										'unexpectedToken',
										'cannot read this closure parameter type',
										t,
									);
								j++;
							}
						}
						if (isPunct(tokens[j], ',')) {
							j++;
							continue;
						}
						if (isPunct(tokens[j], '|')) break;
						return fail(
							'unexpectedToken',
							'cannot read this closure parameter list',
							tokens[j] ?? token,
						);
					}
					closures.push({
						depth: frames.length,
						vars: new Set(names),
					});
					const after = tokens[j + 1];
					if (isPunct(after, '{') || isPunct(after, '-')) {
						current.pendingBody = { vars: [], role: 'closure' };
					}
					i = j;
				}
				break;
			}
			default:
				break;
		}
	}

	let kind: Analysis['kind'] = 'part';
	if (statementFirst !== undefined) {
		if (slots > 0) {
			fail(
				'unterminatedStatement',
				'the last statement is not terminated by `;`; only a single expression or block may be left unterminated (a fragment)',
				statementFirst,
			);
		}
		if (
			statementFirst.kind === 'word' &&
			STATEMENT_WORDS.has(statementFirst.value.toUpperCase()) &&
			!isPunct(tokens[1], ':')
		) {
			fail(
				'notAFragment',
				`an unterminated ${statementFirst.value.toUpperCase()} statement: terminate it with \`;\`, or a fragment must be a single expression or block`,
				statementFirst,
			);
		}
		kind = 'fragment';
	}
	if (root.pendingLet !== undefined) root.vars.add(root.pendingLet);

	const throwLines: Record<string, ReadonlyArray<number>> = {};
	for (const message of sorted(throwsAt.keys())) {
		throwLines[message] = [...(throwsAt.get(message) ?? [])].sort(
			(a, b) => a - b,
		);
	}
	diagnostics.sort((a, b) => a.offset - b.offset);
	return {
		kind,
		sql: sql.slice(0, last.end),
		free: sorted(free),
		defines: sorted(root.vars),
		slots,
		valueSlot: kind === 'fragment' ? 0 : valueSlot,
		throws: Object.keys(throwLines),
		throwsAt: throwLines,
		holes: sorted(holes),
		provisional: holes.size > 0,
		transactional,
		writes,
		diagnostics,
	};
};

/** {@link analyze}, throwing its {@link AnalyzeError}. */
export const analyzeSync = (
	sql: string,
	options: AnalyzeOptions = {},
): Analysis => analyzeLexed(sql, lex(sql, holeDelimiters(options.holes)));

/**
 * Analyses one SurrealQL text: the `$names` it reads and does not define,
 * the names it `LET`s, how many results it returns, what it can `THROW` and
 * which holes it leaves. Fails with the line and column of anything it
 * cannot classify. Errors in `diagnostics` (a `LET $auth`, a `RETURN` inside
 * `BEGIN … COMMIT` followed by another statement) do not fail the analysis;
 * the `statements` command refuses them.
 */
export const analyze = (
	sql: string,
	options: AnalyzeOptions = {},
): Effect.Effect<Analysis, AnalyzeError> =>
	Effect.suspend(() => {
		try {
			return Effect.succeed(analyzeSync(sql, options));
		} catch (error) {
			if (error instanceof AnalyzeError) return Effect.fail(error);
			throw error;
		}
	});
