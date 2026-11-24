/**
 * One `.surql` file as its module, for the run-time loaders (the Node hook,
 * the Bun plugin and `vite-plugin-surrealkit`) and the content mapper: the
 * same function of the file, its path and its project.
 */
import { readFileSync } from 'node:fs';
import { basename, dirname, resolve } from 'node:path';
import { surqlLayout } from './layout.js';
import {
	EMPTY_SURQL_MODULE,
	renderSurqlModule,
	type SurqlFile,
	type SurqlModule,
	surqlDiagnostic,
} from './module.js';
import { type SurqlProject, surqlProjectFor } from './project.js';

/** The scope and name of the `.surql` file at `path`. */
export const surqlFileAt = (path: string, sql: string): SurqlFile => ({
	scope: basename(dirname(resolve(path))),
	name: basename(path).replace(/\.surql$/u, ''),
	sql,
});

/**
 * The module of the `.surql` file at `path` with text `sql` in `project`.
 * A project refused as a whole, a file outside the layout the command reads,
 * and a schema document that cannot be read are each a problem of the file,
 * which then exports nothing: nothing is rendered that the command or the
 * run time would not render the same way.
 */
export const surqlModuleIn = (
	project: SurqlProject,
	path: string,
	sql: string,
): SurqlModule => {
	const file = surqlFileAt(path, sql);
	const refuse = (kind: 'config' | 'layout' | 'schema', message: string) => ({
		text: EMPTY_SURQL_MODULE,
		problems: [surqlDiagnostic(file, kind, message)],
	});
	if (project.refusal !== undefined) return refuse('config', project.refusal);
	const layout = surqlLayout(project.inputs, path);
	if ('problems' in layout)
		return {
			text: EMPTY_SURQL_MODULE,
			problems: layout.problems.map((problem) =>
				surqlDiagnostic(file, 'layout', problem),
			),
		};
	const settings = project.settingsFor(path);
	if (settings.schemaProblem !== undefined)
		return refuse(
			'schema',
			`the schema document of this directory cannot be used: ${settings.schemaProblem}`,
		);
	return renderSurqlModule(
		{ scope: layout.scope, name: layout.name, sql },
		{
			...(settings.declared !== undefined && {
				declared: settings.declared,
			}),
			...(settings.holes !== undefined && { holes: settings.holes }),
		},
	);
};

/** A `.surql` file a loader cannot turn into a module. */
export class SurqlModuleError extends Error {
	override readonly name = 'SurqlModuleError';
	constructor(
		readonly file: string,
		readonly problems: ReadonlyArray<string>,
	) {
		super(
			`effect-surrealkit: ${file}:\n${problems.map((p) => `  ${p}`).join('\n')}`,
		);
	}
}

export interface LoadSurqlOptions {
	/**
	 * The `tsconfig.json` whose `effect-surrealkit` content mapper entry holds
	 * the options; default the nearest one above the file.
	 */
	readonly tsconfig?: string;
}

/**
 * The TypeScript module of the `.surql` file at `path`, exactly as the
 * content mapper describes it to the type check. Throws a
 * {@link SurqlModuleError} on an option that cannot be used or a problem
 * with the file, as the `statements` command refuses to write it.
 */
export const loadSurqlModule = (
	path: string,
	options: LoadSurqlOptions = {},
): string => {
	const project = surqlProjectFor(path, options.tsconfig);
	const sql = readFileSync(path, 'utf8');
	const module = surqlModuleIn(project, path, sql);
	const problems = [
		...project.problems.map(
			(problem) =>
				`the content mapper option ${problem.path.join('.') || 'options'}: ${problem.message}`,
		),
		...module.problems.map((problem) => problem.text),
	];
	if (problems.length > 0) throw new SurqlModuleError(path, problems);
	return module.text;
};
