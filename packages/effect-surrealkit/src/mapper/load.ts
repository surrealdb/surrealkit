/**
 * One `.surql` file as its module, for the run-time loaders (the Node hook,
 * the Bun plugin and `vite-plugin-surrealkit`) and the content mapper: the
 * same function of the file, its path and its project.
 */
import { readFileSync, statSync } from 'node:fs';
import {
	basename,
	dirname,
	isAbsolute,
	join,
	relative,
	resolve,
	sep,
} from 'node:path';
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

const refusal = (
	file: SurqlFile,
	kind: 'config' | 'layout' | 'schema',
	message: string,
): SurqlModule => ({
	text: EMPTY_SURQL_MODULE,
	problems: [surqlDiagnostic(file, kind, message)],
});

const isFile = (path: string): boolean => {
	try {
		return statSync(path).isFile();
	} catch {
		return false;
	}
};

/** Whether `file` lies inside one of the project's input roots. */
const inInput = (project: SurqlProject, file: string): boolean =>
	project.inputs.some((root) => {
		const at = relative(root, file);
		return at !== '' && !isAbsolute(at) && at.split(sep)[0] !== '..';
	});

/**
 * The other entries that hold `file` in their input: the `effect-surrealkit`
 * entry written in each `tsconfig.json` at or above the file's directory,
 * besides the project's own. The type check renders a file with the project
 * `tsc` was started with and the loaders with the nearest `tsconfig.json`,
 * so either may be any of these.
 */
const otherEntries = (
	project: SurqlProject,
	file: string,
): ReadonlyArray<SurqlProject> => {
	const found: SurqlProject[] = [];
	for (let at = dirname(file); ; at = dirname(at)) {
		const config = join(at, 'tsconfig.json');
		if (config !== project.config && isFile(config)) {
			const other = surqlProjectFor(file, config);
			if (other.refusal === undefined && inInput(other, file))
				found.push(other);
		}
		if (dirname(at) === at) return found;
	}
};

/** The module of the file in `project` alone. */
const renderIn = (
	project: SurqlProject,
	path: string,
	file: SurqlFile,
): SurqlModule => {
	if (project.refusal !== undefined)
		return refusal(file, 'config', project.refusal);
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
		return refusal(
			file,
			'schema',
			`the schema document of this directory cannot be used: ${settings.schemaProblem}`,
		);
	return renderSurqlModule(
		{ scope: layout.scope, name: layout.name, sql: file.sql },
		{
			...(settings.declared !== undefined && {
				declared: settings.declared,
			}),
			...(settings.holes !== undefined && { holes: settings.holes }),
		},
	);
};

/**
 * The module of the `.surql` file at `path` with text `sql` in `project`.
 * A project refused as a whole, a file outside the layout the command reads,
 * a schema document that cannot be read, and a file that another entry also
 * holds in its input and renders differently are each a problem of the
 * file, which then exports nothing: nothing is rendered that the command or
 * the run time would not render the same way.
 */
export const surqlModuleIn = (
	project: SurqlProject,
	path: string,
	sql: string,
): SurqlModule => {
	const absolute = resolve(path);
	const file = surqlFileAt(absolute, sql);
	const module = renderIn(project, absolute, file);
	if (module.problems.length > 0) return module;
	for (const other of otherEntries(project, absolute)) {
		const theirs = renderIn(other, absolute, file);
		if (theirs.problems.length > 0 || theirs.text !== module.text)
			return refusal(
				file,
				'config',
				`two contentMappers entries hold this file in their input and render it differently: ${project.config ?? `the options of ${project.root}`} and ${other.config}. The type check renders it with the tsconfig.json tsc was started with and the run-time loaders with the nearest one above the file, so the module run could differ from the module checked; take the file out of one entry's input, or write the same options in both`,
			);
	}
	return module;
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
