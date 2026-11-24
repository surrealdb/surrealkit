/**
 * One `.surql` file as its module, for the run-time loaders (the Node hook,
 * the Bun plugin and `vite-plugin-surrealkit`) and the content mapper: the
 * same function of the file, its path and its project.
 */
import { readFileSync } from 'node:fs';
import { basename, dirname, resolve } from 'node:path';

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
 * A schema document the file's directory takes that cannot be read is a
 * problem of the file: the file is not rendered without its schema.
 */
export const surqlModuleIn = (
	project: SurqlProject,
	path: string,
	sql: string,
): SurqlModule => {
	const file = surqlFileAt(path, sql);
	const settings = project.settingsFor(path);
	if (settings.schemaProblem !== undefined)
		return {
			text: EMPTY_SURQL_MODULE,
			problems: [
				surqlDiagnostic(
					file,
					'schema',
					`the schema document of this directory cannot be used: ${settings.schemaProblem}`,
				),
			],
		};
	return renderSurqlModule(file, {
		...(settings.declared !== undefined && { declared: settings.declared }),
		...(settings.holes !== undefined && { holes: settings.holes }),
	});
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
