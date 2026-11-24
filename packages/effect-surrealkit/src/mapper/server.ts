/**
 * The content mapper TypeScript 7.1 starts for `.surql` files (`package.json`,
 * `typescript.contentMapper`): it answers `initialize`, `openProject`,
 * `transform` and `closeProject`. Each transform renders the file through
 * `renderStatementModules` (see `module.ts`); no engine, no cache and no
 * worker are involved.
 */
import { dirname } from 'node:path';
import { surqlModuleIn } from './load.js';
import { EMPTY_SURQL_MODULE, type SurqlDiagnostic } from './module.js';
import { openSurqlProject, type SurqlProject } from './project.js';
import type { RpcHandlers } from './rpc.js';

/** What TypeScript reports a mapper diagnostic under (`effect-surrealkit<code>`). */
export const DIAGNOSTIC_SOURCE = 'effect-surrealkit';

/** The code of each kind of problem. */
export const DIAGNOSTIC_CODES: Readonly<
	Record<SurqlDiagnostic['kind'] | 'option' | 'internal', number>
> = {
	analysis: 1,
	name: 2,
	scope: 3,
	undeclared: 4,
	schema: 5,
	option: 6,
	internal: 7,
};

/**
 * The `diagnosticDirectives` of a transform: one expect directive per
 * problem, over an empty range at the start of the module, where no
 * TypeScript diagnostic can land. TypeScript then reports the problem's
 * message at its place in the `.surql` file among the type errors. A
 * transform's own `diagnostics` are read as syntax errors, and a program
 * with a syntax error reports no type errors in any file, so they are not
 * used. Every range lies inside the file.
 */
export const directivesOf = (
	sql: string,
	problems: ReadonlyArray<SurqlDiagnostic>,
) =>
	problems.length === 0
		? {}
		: {
				diagnosticDirectives: {
					unusedExpectDirectiveDiagnostics: problems.map(
						(problem) => ({
							code: DIAGNOSTIC_CODES[problem.kind],
							messageText:
								problem.code === undefined
									? problem.message
									: `${problem.message} (${problem.code})`,
						}),
					),
					directives: problems.map((problem, index) => {
						const start = Math.min(problem.offset, sql.length);
						return [
							start,
							start < sql.length ? 1 : 0,
							0,
							0,
							1,
							index,
						];
					}),
				},
			};

interface OpenProject {
	readonly configFileName?: unknown;
	readonly projectHandle?: unknown;
	readonly options?: unknown;
}

interface Transform {
	readonly fileName?: unknown;
	readonly content?: unknown;
	readonly projectHandle?: unknown;
}

/** The mapper's methods; one process serves every project TypeScript opens. */
export const mapperHandlers = (): RpcHandlers => {
	const projects = new Map<string, SurqlProject>();
	return {
		initialize: (params) => {
			const offered = (
				params as { positionEncodings?: unknown } | undefined
			)?.positionEncodings;
			// The analyzer's columns count UTF-16 code units.
			const encoding =
				Array.isArray(offered) && !offered.includes('utf-16')
					? 'utf-8'
					: 'utf-16';
			if (encoding !== 'utf-16')
				throw new Error(
					'effect-surrealkit needs the utf-16 position encoding',
				);
			return {
				positionEncoding: encoding,
				diagnosticSource: DIAGNOSTIC_SOURCE,
			};
		},
		openProject: (params) => {
			const p = (params ?? {}) as OpenProject;
			const handle = String(p.projectHandle);
			const config =
				typeof p.configFileName === 'string' ? p.configFileName : '';
			// An inferred project (no tsconfig.json) has no directory for the
			// options' paths to be relative to, so it takes none.
			const inferred = config === '';
			const ignored =
				inferred &&
				typeof p.options === 'object' &&
				p.options !== null &&
				Object.keys(p.options).length > 0;
			const project = openSurqlProject(
				inferred ? process.cwd() : dirname(config),
				inferred ? undefined : p.options,
			);
			projects.set(handle, project);
			return {
				configIdentity: project.identity,
				watchedFiles: project.documents,
				optionDiagnostics: [
					...(ignored
						? [
								{
									path: [],
									message:
										'the options need a tsconfig.json for their paths; an inferred project takes none',
								},
							]
						: []),
					...project.problems,
				].map((problem) => ({
					path: problem.path,
					messageText: problem.message,
					code: DIAGNOSTIC_CODES.option,
				})),
			};
		},
		transform: (params) => {
			const p = (params ?? {}) as Transform;
			const fileName = String(p.fileName);
			const sql = typeof p.content === 'string' ? p.content : '';
			try {
				const project =
					projects.get(String(p.projectHandle)) ??
					openSurqlProject(dirname(fileName), undefined);
				const module = surqlModuleIn(project, fileName, sql);
				return {
					extension: '.ts',
					text: module.text,
					mappings: [],
					...directivesOf(sql, module.problems),
				};
			} catch (cause) {
				// Never fail a transform: TypeScript would report the file as
				// unreadable and hide the other files' type errors.
				const message =
					cause instanceof Error ? cause.message : String(cause);
				return {
					extension: '.ts',
					text: EMPTY_SURQL_MODULE,
					mappings: [],
					diagnosticDirectives: {
						unusedExpectDirectiveDiagnostics: [
							{
								code: DIAGNOSTIC_CODES.internal,
								messageText: `effect-surrealkit could not render this file: ${message}`,
							},
						],
						directives: [[0, sql.length > 0 ? 1 : 0, 0, 0, 1, 0]],
					},
				};
			}
		},
		closeProject: (params) => {
			projects.delete(
				String(
					(params as { projectHandle?: unknown } | undefined)
						?.projectHandle,
				),
			);
			return {};
		},
	};
};
