/**
 * Authored `.surql` texts checked on a discarded in-memory engine: each
 * complete text is sent once with no bindings, after the optional schema
 * files. A parse error, or a number of results other than the analyzer's
 * `slots`, is a problem; any other error a text meets without bindings is
 * not. Backs `effect-surrealkit statements --verify`.
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

/** One problem: where it is, when the engine says, and what it is. */
export interface VerifyProblem {
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
	file: string,
	text: string,
	at?: { readonly line: number; readonly column: number },
): VerifyProblem => ({
	file,
	...(at !== undefined && at),
	text,
	message: `${file}${at === undefined ? '' : `:${at.line}:${at.column}`}: ${text}`,
});

/** A problem from the error the engine refused a whole text with. */
const refusal = (file: string, cause: unknown): VerifyProblem => {
	const message = messageOf(cause);
	const first = message.split('\n', 1)[0]?.trim() ?? '';
	if (!message.startsWith('Parse error'))
		return problem(file, `the engine did not run the text: ${first}`);
	const at = /-->\s*\[(\d+):(\d+)\]/u.exec(message);
	return problem(
		file,
		first,
		at === null
			? undefined
			: { line: Number(at[1]), column: Number(at[2]) },
	);
};

interface Engine {
	readonly version: string;
	readonly send: (
		sql: string,
	) => Promise<
		ReadonlyArray<
			| { readonly success: true }
			| { readonly success: false; readonly error: { message: string } }
		>
	>;
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
		const engine = yield* startEngine(
			options.timeoutSeconds ?? DEFAULT_TIMEOUT_SECONDS,
		);
		const problems = yield* Effect.promise(async () => {
			for (const { name, sql } of options.schema ?? []) {
				let responses: Awaited<ReturnType<Engine['send']>>;
				try {
					responses = await engine.send(sql);
				} catch (cause) {
					return [refusal(name, cause)];
				}
				const failed = responses.findIndex((r) => !r.success);
				const response = responses[failed];
				if (response !== undefined && !response.success)
					return [
						problem(
							name,
							`statement ${failed + 1} failed: ${response.error.message.split('\n', 1)[0]}`,
						),
					];
			}
			const found: VerifyProblem[] = [];
			for (const { file, sql, slots } of texts) {
				let responses: Awaited<ReturnType<Engine['send']>>;
				try {
					responses = await engine.send(sql);
				} catch (cause) {
					found.push(refusal(file, cause));
					continue;
				}
				if (responses.length !== slots)
					found.push(
						problem(
							file,
							`the engine returned ${responses.length} results, the analyzer derived ${slots}`,
						),
					);
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
 * a result count other than the analyzer's `slots`, a text that does not
 * finish within `timeoutSeconds`, or a schema file that does not apply;
 * with `TestingError` when the engine is not installed or does not start.
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
