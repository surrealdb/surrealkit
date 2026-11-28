/**
 * Authored `.surql` texts checked on a discarded in-memory engine: each
 * complete text is sent once with no bindings, after the optional schema
 * files. A parse error, a number of results other than the analyzer's
 * `slots`, or a text that does not finish within the timeout is a problem;
 * any other error a text meets without bindings is not. Backs
 * `effect-surrealkit statements --verify`.
 */
import { Data, Effect, type Scope } from 'effect';
import { Surreal } from 'surrealdb';

import { analyzeSync } from '../analyze/analyze.js';
import type { HoleDelimiters } from '../analyze/lexer.js';
import { nodeEngines } from './database.js';
import { TestingError } from './errors.js';

/** A file by the name its problems carry, and its text. */
export interface VerifyFile {
	readonly name: string;
	readonly sql: string;
}

export interface VerifyInput {
	/**
	 * The scopes as `renderStatementModules` takes them: each file is named
	 * `<scope>/<name>.surql` in what is reported.
	 */
	readonly scopes: ReadonlyArray<{
		readonly name: string;
		readonly files: ReadonlyArray<VerifyFile>;
	}>;
	/**
	 * SurrealQL schema files, run first in this order, each named as given.
	 * Optional: the parse and the result count do not depend on it.
	 */
	readonly schema?: ReadonlyArray<VerifyFile>;
	/** The literal delimiters of a hole; default `__` and `__`. */
	readonly holes?: HoleDelimiters;
	/** How long one text or schema file may run; default 10. */
	readonly timeoutSeconds?: number;
}

/**
 * What a problem is: a parse error the engine reports (`parse`), another
 * refusal of the whole text (`refused`), a result count other than `slots`
 * (`count`), a text or schema file that did not finish within the timeout
 * (`timeout`), a schema statement that failed (`schema`), or a text the
 * analyzer refused (`analyzer`).
 */
export type VerifyProblemKind =
	| 'parse'
	| 'refused'
	| 'count'
	| 'timeout'
	| 'schema'
	| 'analyzer';

/** One problem: where it is, when the engine says, and what it is. */
export interface VerifyProblem {
	readonly kind: VerifyProblemKind;
	readonly file: string;
	readonly line?: number;
	readonly column?: number;
	readonly text: string;
	/** `<file>[:<line>:<column>]: <text>`. */
	readonly message: string;
}

/** What a passing check sent, and the engine it ran on. */
export interface Verification {
	/** As `@surrealdb/node` reports it, e.g. `4.0.1`. */
	readonly engine: string;
	/** The complete texts sent, by file name. */
	readonly sent: ReadonlyArray<string>;
	/** The fragments and templates, which cannot be sent as they stand. */
	readonly notSent: ReadonlyArray<string>;
}

/** The engine refused or miscounted a text, or a schema file failed. */
export class StatementVerificationError extends Data.TaggedError(
	'StatementVerificationError',
)<{
	readonly engine: string;
	readonly problems: ReadonlyArray<VerifyProblem>;
	readonly message: string;
}> {}

const DEFAULT_TIMEOUT_SECONDS = 10;

/**
 * The engine's options: nothing a text runs reaches the network, a script
 * or a host function, so verifying an authored text has no effect outside
 * the discarded database. Network targets and scripting are denied by the
 * engine's defaults as well; they are stated so a change of default does not
 * change this.
 */
const engineOptions = (timeoutSeconds: number) => ({
	query_timeout: timeoutSeconds,
	capabilities: {
		scripting: false,
		guest_access: false,
		network_targets: false,
		functions: { allow: true, deny: ['http'] },
	},
});

const messageOf = (cause: unknown): string =>
	cause instanceof Error ? cause.message : String(cause);

const problem = (
	kind: VerifyProblemKind,
	file: string,
	text: string,
	at?: { readonly line: number; readonly column: number },
): VerifyProblem => ({
	kind,
	file,
	...(at !== undefined && at),
	text,
	message: `${file}${at === undefined ? '' : `:${at.line}:${at.column}`}: ${text}`,
});

/**
 * Whether `error`, or an error in its `cause` chain, is the engine's query
 * timeout: `kind: "Query"` with `details.kind: "TimedOut"`. The engine
 * reports one timeout in two ways, as the error of each statement it did not
 * finish or as the refusal of the whole text, with this same structure in
 * both; it is read from the structure, never from the message.
 */
const isTimeout = (error: unknown): boolean => {
	for (
		let at: unknown = error, depth = 0;
		typeof at === 'object' && at !== null && depth < 8;
		at = (at as { readonly cause?: unknown }).cause, depth++
	) {
		const { kind, details } = at as {
			readonly kind?: unknown;
			readonly details?: { readonly kind?: unknown } | null;
		};
		if (
			kind === 'Query' &&
			typeof details === 'object' &&
			details?.kind === 'TimedOut'
		)
			return true;
	}
	return false;
};

const timeoutProblem = (file: string, timeoutSeconds: number): VerifyProblem =>
	problem(
		'timeout',
		file,
		`the text did not finish within ${timeoutSeconds}s on an empty database with no bindings, so its result count was not verified`,
	);

/** A problem from the error the engine refused a whole text with. */
const refusal = (
	file: string,
	cause: unknown,
	timeoutSeconds: number,
): VerifyProblem => {
	if (isTimeout(cause)) return timeoutProblem(file, timeoutSeconds);
	const message = messageOf(cause);
	const first = message.split('\n', 1)[0]?.trim() ?? '';
	if (!message.startsWith('Parse error'))
		return problem(
			'refused',
			file,
			`the engine did not run the text: ${first}`,
		);
	const at = /-->\s*\[(\d+):(\d+)\]/u.exec(message);
	return problem(
		'parse',
		file,
		first,
		at === null
			? undefined
			: { line: Number(at[1]), column: Number(at[2]) },
	);
};

/** One statement's result, as the SDK returns it. */
type Response =
	| { readonly success: true }
	| { readonly success: false; readonly error: { readonly message: string } };

/** What the engine did with one text: its results, or the refusal. */
export type Sent =
	| { readonly responses: ReadonlyArray<Response> }
	| { readonly refused: unknown };

/**
 * The problem of one complete text, if any, from what the engine did with
 * it. A text that did not finish within the timeout has one outcome however
 * the engine reports it, per statement or as a refusal of the whole text: a
 * `timeout` problem, never a verified text and never a parse error.
 *
 * @internal
 */
export const judgeText = (
	text: { readonly file: string; readonly slots: number },
	sent: Sent,
	timeoutSeconds: number,
): VerifyProblem | undefined => {
	if ('refused' in sent)
		return refusal(text.file, sent.refused, timeoutSeconds);
	const { responses } = sent;
	if (responses.some((r) => !r.success && isTimeout(r.error)))
		return timeoutProblem(text.file, timeoutSeconds);
	if (responses.length !== text.slots)
		return problem(
			'count',
			text.file,
			`the engine returned ${responses.length} results, the analyzer derived ${text.slots}`,
		);
	return undefined;
};

interface Engine {
	readonly version: string;
	readonly send: (sql: string) => Promise<ReadonlyArray<Response>>;
}

const startEngine = (
	timeoutSeconds: number,
): Effect.Effect<Engine, TestingError, Scope.Scope> =>
	Effect.gen(function* () {
		const { createNodeEngines } = yield* nodeEngines;
		const surreal = yield* Effect.acquireRelease(
			Effect.tryPromise({
				try: async () => {
					const surreal = new Surreal({
						engines: createNodeEngines(
							engineOptions(timeoutSeconds),
						) as never,
					});
					try {
						await surreal.connect('mem://', {
							namespace: 'verify',
							database: 'verify',
						});
						return surreal;
					} catch (cause) {
						await surreal.close();
						throw cause;
					}
				},
				catch: (cause) =>
					new TestingError({
						message: `the in-memory engine did not start: ${messageOf(cause)}`,
						cause,
					}),
			}),
			(surreal) => Effect.promise(() => surreal.close()),
		);
		const version = yield* Effect.tryPromise({
			try: () => surreal.version(),
			catch: (cause) =>
				new TestingError({
					message: `the in-memory engine did not report its version: ${messageOf(cause)}`,
					cause,
				}),
		});
		return {
			version: version.version.replace(/^surrealdb-/u, ''),
			send: (sql) => surreal.query(sql).responses(),
		};
	});

/** An analysed complete text: its name, the text sent, and its `slots`. */
export interface AnalysedText {
	readonly file: string;
	readonly sql: string;
	readonly slots: number;
}

/**
 * Starts the engine, loads `schema` and sends each text, returning the
 * problems; the engine closes with the scope.
 *
 * @internal
 */
export const verifyAnalysed = (
	texts: ReadonlyArray<AnalysedText>,
	options: Pick<VerifyInput, 'schema' | 'timeoutSeconds'>,
): Effect.Effect<
	{
		readonly engine: string;
		readonly problems: ReadonlyArray<VerifyProblem>;
	},
	TestingError,
	Scope.Scope
> =>
	Effect.gen(function* () {
		const timeoutSeconds =
			options.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS;
		const engine = yield* startEngine(timeoutSeconds);
		const send = async (sql: string): Promise<Sent> => {
			try {
				return { responses: await engine.send(sql) };
			} catch (refused) {
				return { refused };
			}
		};
		const problems = yield* Effect.promise(async () => {
			for (const { name, sql } of options.schema ?? []) {
				const sent = await send(sql);
				if ('refused' in sent)
					return [refusal(name, sent.refused, timeoutSeconds)];
				const failed = sent.responses.findIndex((r) => !r.success);
				const response = sent.responses[failed];
				if (response !== undefined && !response.success)
					return [
						isTimeout(response.error)
							? timeoutProblem(name, timeoutSeconds)
							: problem(
									'schema',
									name,
									`statement ${failed + 1} failed: ${response.error.message.split('\n', 1)[0]}`,
								),
					];
			}
			const found: VerifyProblem[] = [];
			for (const text of texts) {
				const judged = judgeText(
					text,
					await send(text.sql),
					timeoutSeconds,
				);
				if (judged !== undefined) found.push(judged);
			}
			return found;
		});
		return { engine: engine.version, problems };
	});

/**
 * Checks authored texts on a discarded in-memory engine (`@surrealdb/node`,
 * an optional peer, imported only here): loads `schema`, then sends each
 * complete text once with no bindings, in scope and file order, on one
 * database. Fails with {@link StatementVerificationError} on a parse error,
 * a result count other than the analyzer's `slots`, a schema file that does
 * not apply, or a text that does not finish within `timeoutSeconds`. A text
 * that does not finish is a problem of kind `timeout` however the engine
 * reports it: it did not finish within the limit on an empty database with
 * no bindings, so its result count was not verified. It is never reported as
 * verified and never as a parse error. Fails with `TestingError` when the
 * engine is not installed or does not start.
 *
 * The texts run: writes land in the discarded database, and the engine
 * denies `http::` functions, network targets and scripting, so nothing a
 * text does reaches outside it.
 */
export const verifyStatements = (
	input: VerifyInput,
): Effect.Effect<Verification, StatementVerificationError | TestingError> =>
	Effect.gen(function* () {
		const sent: AnalysedText[] = [];
		const notSent: string[] = [];
		const early: VerifyProblem[] = [];
		for (const scope of input.scopes) {
			for (const { name, sql } of scope.files) {
				const file = `${scope.name}/${name}.surql`;
				let analysis: ReturnType<typeof analyzeSync>;
				try {
					analysis = analyzeSync(
						sql,
						input.holes === undefined ? {} : { holes: input.holes },
					);
				} catch (cause) {
					early.push(
						problem(
							'analyzer',
							file,
							`the analyzer refused the text: ${messageOf(cause)}`,
						),
					);
					continue;
				}
				if (analysis.kind === 'part' && analysis.holes.length === 0)
					sent.push({
						file,
						sql: analysis.sql,
						slots: analysis.slots,
					});
				else notSent.push(file);
			}
		}
		const { engine, problems } = yield* Effect.scoped(
			verifyAnalysed(sent, input),
		);
		const all = [...early, ...problems].sort((a, b) =>
			a.file < b.file ? -1 : a.file > b.file ? 1 : 0,
		);
		if (all.length > 0)
			return yield* new StatementVerificationError({
				engine,
				problems: all,
				message: all.map((p) => p.message).join('\n'),
			});
		return {
			engine,
			sent: sent.map((text) => text.file).sort(),
			notSent: notSent.sort(),
		};
	});
