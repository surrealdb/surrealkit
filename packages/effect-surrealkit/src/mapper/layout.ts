/**
 * Where a `.surql` file may sit, and what it is named: exactly the layouts
 * `effect-surrealkit statements --input <root>` accepts. A file is
 * `<root>/<scope>/<name>.surql`. Dot entries are ignored, a root holds only
 * scope directories, a scope holds only `.surql` files and at least one, and
 * scope and file names follow the renderer's rules. With several roots the
 * scopes of all of them are named together, so a scope name used in two
 * roots is refused. Anything the command refuses is a problem of the file;
 * a problem anywhere in its roots refuses it, as it stops the command.
 */
import { readdirSync } from 'node:fs';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';

import { renderStatementModulesSync } from '../analyze/render.js';

/** The file's scope and name, or why the command would not accept it. */
export type SurqlLayout =
	| { readonly scope: string; readonly name: string }
	| { readonly problems: ReadonlyArray<string> };

const entries = (directory: string) =>
	readdirSync(directory, { withFileTypes: true })
		.filter((entry) => !entry.name.startsWith('.'))
		.sort((a, b) => (a.name < b.name ? -1 : 1));

interface Listing {
	readonly problems: ReadonlyArray<string>;
	readonly scopes: ReadonlyArray<{
		readonly name: string;
		readonly files: ReadonlyArray<string>;
	}>;
}

/** The command's reading of one root, without the files' contents. */
const listRoot = (root: string): Listing => {
	const problems: string[] = [];
	const scopes: Array<{ name: string; files: string[] }> = [];
	let found: ReturnType<typeof entries>;
	try {
		found = entries(root);
	} catch (cause) {
		return {
			problems: [
				`cannot read the input ${root}: ${cause instanceof Error ? cause.message : String(cause)}`,
			],
			scopes,
		};
	}
	for (const scope of found) {
		if (!scope.isDirectory()) {
			problems.push(
				`${scope.name}: --input holds only scope directories`,
			);
			continue;
		}
		const files: string[] = [];
		let other = false;
		for (const entry of entries(join(root, scope.name))) {
			if (!entry.isFile() || !entry.name.endsWith('.surql')) {
				problems.push(
					`${scope.name}/${entry.name}: a scope holds only .surql files`,
				);
				other = true;
				continue;
			}
			files.push(entry.name.slice(0, -'.surql'.length));
		}
		if (files.length === 0 && !other)
			problems.push(`${scope.name}: has no .surql files`);
		scopes.push({ name: scope.name, files });
	}
	if (scopes.length === 0 && problems.length === 0)
		problems.push(`${root} has no scope directories`);
	return { problems, scopes };
};

const PLACEHOLDER = 'RETURN NONE;';

/**
 * The layout of `file` among `roots`. The names are checked by the renderer
 * itself, over every scope and file the roots hold, so a file is named as
 * the command names it, or refused where the command refuses it.
 */
export const surqlLayout = (
	roots: ReadonlyArray<string>,
	file: string,
): SurqlLayout => {
	if (roots.length === 0)
		return {
			problems: [
				'the content mapper options name no input; set "input" to the directory the statements command reads with --input',
			],
		};
	const absolute = resolve(file);
	const containing = roots.filter((root) => {
		const path = relative(root, absolute);
		return path !== '' && !isAbsolute(path) && path.split(sep)[0] !== '..';
	});
	if (containing.length === 0)
		return {
			problems: [
				`the file is outside the input (${roots.join(', ')}); the statements command reads only <input>/<scope>/<name>.surql`,
			],
		};
	if (containing.length > 1)
		return {
			problems: [
				`the file is inside more than one input (${containing.join(', ')})`,
			],
		};
	const root = containing[0] as string;
	const segments = relative(root, absolute).split(sep);
	if (segments.length !== 2 || !(segments[1] ?? '').endsWith('.surql'))
		return {
			problems: [
				`${segments.join('/')} is not <scope>/<name>.surql under the input ${root}; the statements command reads only that layout`,
			],
		};
	const [scope, base] = segments as [string, string];
	if (scope.startsWith('.') || base.startsWith('.'))
		return {
			problems: [
				`${scope}/${base}: the statements command ignores dot entries`,
			],
		};
	const name = base.slice(0, -'.surql'.length);
	const problems: string[] = [];
	const seen = new Map<string, string>();
	const scopes: Array<{
		name: string;
		files: Array<{ name: string; sql: string }>;
	}> = [];
	for (const each of roots) {
		const listing = listRoot(each);
		for (const problem of listing.problems)
			problems.push(
				`the statements command refuses the input ${each}: ${problem}`,
			);
		for (const listed of listing.scopes) {
			const other = seen.get(listed.name);
			if (other !== undefined) {
				problems.push(
					`the scope ${JSON.stringify(listed.name)} is in two inputs, ${other} and ${each}`,
				);
				continue;
			}
			seen.set(listed.name, each);
			scopes.push({
				name: listed.name,
				files: listed.files.map((file) => ({
					name: file,
					sql: PLACEHOLDER,
				})),
			});
		}
	}
	try {
		renderStatementModulesSync({ scopes });
	} catch (error) {
		const named = (
			error as {
				readonly problems?: ReadonlyArray<{
					readonly kind: string;
					readonly text: string;
				}>;
			}
		).problems;
		if (named === undefined) throw error;
		for (const problem of named)
			if (problem.kind === 'name' || problem.kind === 'scope')
				problems.push(`the statements command refuses ${problem.text}`);
	}
	if (seen.get(scope) !== root)
		problems.push(
			`the scope ${JSON.stringify(scope)} is not read from ${root}`,
		);
	return problems.length > 0 ? { problems } : { scope, name };
};
