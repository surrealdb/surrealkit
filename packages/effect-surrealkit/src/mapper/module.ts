/**
 * The module one `.surql` file yields when it is imported: what
 * `renderStatementModules` renders for a scope that holds only that file.
 * The scope is the file's parent directory and the constant is named after
 * the file, as with `effect-surrealkit statements`. Pure: the host reads the
 * file and passes its text.
 */
import type { HoleDelimiters } from '../analyze/lexer.js';
import {
	type DeclaredNames,
	renderStatementModulesSync,
	STATEMENTS_MARK,
	StatementModulesError,
	type StatementProblem,
} from '../analyze/render.js';

/** Where a `.surql` file sits: its scope (parent directory) and name. */
export interface SurqlFile {
	/** The name of the file's parent directory. */
	readonly scope: string;
	/** The file name without `.surql`. */
	readonly name: string;
	readonly sql: string;
}

export interface SurqlModuleOptions {
	/** The schema the file is checked against; see `StatementScope`. */
	readonly declared?: DeclaredNames;
	/** The literal delimiters of a hole; default `__` and `__`. */
	readonly holes?: HoleDelimiters;
}

/** A problem with the file, at a 1-based line and column of its text. */
export interface SurqlDiagnostic {
	readonly kind: StatementProblem['kind'] | 'schema';
	readonly line: number;
	readonly column: number;
	/** The UTF-16 offset of `line` and `column` in the text. */
	readonly offset: number;
	/** The analyzer's code, when it gives one. */
	readonly code?: string;
	readonly message: string;
	/** `<scope>/<name>.surql:<line>:<column>: <message>`. */
	readonly text: string;
}

/**
 * The module, or why there is none. With problems, `text` is a module that
 * exports nothing, so an import of the constant fails where it is written,
 * as the command writes nothing for such a file.
 */
export interface SurqlModule {
	readonly text: string;
	readonly problems: ReadonlyArray<SurqlDiagnostic>;
}

/** The module text of a file that has problems: it exports nothing. */
export const EMPTY_SURQL_MODULE = `${STATEMENTS_MARK}\n\nexport {};\n`;

/** The UTF-16 offset of a 1-based line and column, kept inside `sql`. */
export const offsetOf = (sql: string, line: number, column: number): number => {
	let start = 0;
	for (let at = 1; at < line; at++) {
		const next = sql.indexOf('\n', start);
		if (next === -1) return sql.length;
		start = next + 1;
	}
	const end = sql.indexOf('\n', start);
	const lineEnd = end === -1 ? sql.length : end;
	return Math.min(start + Math.max(column, 1) - 1, lineEnd);
};

/** A diagnostic at a line and column of `file`. */
export const surqlDiagnostic = (
	file: SurqlFile,
	kind: SurqlDiagnostic['kind'],
	message: string,
	at: {
		readonly line?: number;
		readonly column?: number;
		readonly code?: string;
	} = {},
): SurqlDiagnostic => {
	const line = at.line ?? 1;
	const column = at.column ?? 1;
	return {
		kind,
		line,
		column,
		offset: offsetOf(file.sql, line, column),
		...(at.code !== undefined && { code: at.code }),
		message,
		text: `${file.scope}/${file.name}.surql:${line}:${column}: ${message}`,
	};
};

/**
 * Renders the module of one `.surql` file through
 * `renderStatementModulesSync`: the content of `<scope>/statements.ts` for a
 * scope that holds only this file, byte for byte. The analyzer's warnings
 * are not returned: the type check has no warning category.
 */
export const renderSurqlModule = (
	file: SurqlFile,
	options: SurqlModuleOptions = {},
): SurqlModule => {
	try {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: file.scope,
					files: [{ name: file.name, sql: file.sql }],
					...(options.declared !== undefined && {
						declared: options.declared,
					}),
				},
			],
			...(options.holes !== undefined && { holes: options.holes }),
		});
		const module = files.find(
			(rendered) => rendered.path === `${file.scope}/statements.ts`,
		);
		if (module === undefined)
			throw new Error(`renderStatementModules rendered no ${file.scope}`);
		return { text: module.content, problems: [] };
	} catch (error) {
		if (!(error instanceof StatementModulesError)) throw error;
		return {
			text: EMPTY_SURQL_MODULE,
			problems: error.problems.map((problem) =>
				surqlDiagnostic(file, problem.kind, problem.message, {
					...(problem.line !== undefined && { line: problem.line }),
					...(problem.column !== undefined && {
						column: problem.column,
					}),
					...(problem.code !== undefined && { code: problem.code }),
				}),
			),
		};
	}
};

const CONSTANT_END = '\n} as const);\n';

/**
 * The module `text` as JavaScript, for a host that does not strip
 * TypeScript itself (Vite's dev server). The renderer writes one TypeScript
 * construct, the `as const` that closes each constant, and a one-file module
 * holds one constant, at its end: that `as const` is removed and nothing
 * else changes. A text in any other form is refused, never guessed at.
 */
export const surqlScript = (text: string): string => {
	if (text === EMPTY_SURQL_MODULE) return text;
	if (!text.endsWith(CONSTANT_END))
		throw new Error(
			'effect-surrealkit: not a one-file .surql module; cannot write it as JavaScript',
		);
	return `${text.slice(0, -CONSTANT_END.length)}\n});\n`;
};
