/**
 * The settings every host of a `.surql` import reads: the content mapper from
 * the `options` of its `contentMappers` entry, and the run-time loaders from
 * the same entry in the nearest `tsconfig.json`. One place, so the type check
 * and the run time render the same module.
 *
 * ```jsonc
 * "contentMappers": [{
 *   "package": "effect-surrealkit",
 *   "extensions": [".surql"],
 *   "options": {
 *     "schema": "schema/document.json",                  // every directory
 *     "schemas": { "queries/earth": "earth/schema.json" }, // one directory
 *     "holes": { "open": "__", "close": "__" }
 *   }
 * }]
 * ```
 *
 * Paths are relative to the directory of the `tsconfig.json`. A directory
 * named in `schemas` takes that document, any other takes `schema`, and with
 * neither a file is not checked against a schema, as the command without
 * `--schema`.
 */
import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { dirname, isAbsolute, join, resolve } from 'node:path';

import { Effect } from 'effect';

import type { HoleDelimiters } from '../analyze/lexer.js';
import type { DeclaredNames } from '../analyze/render.js';
import { declaredNames, decodeDocument } from '../emitter/document.js';

/** The `options` of the `contentMappers` entry, as written. */
export interface SurqlMapperOptions {
	readonly schema?: string;
	readonly schemas?: Readonly<Record<string, string>>;
	readonly holes?: HoleDelimiters;
}

/** An option that cannot be used, at its path inside `options`. */
export interface SurqlOptionProblem {
	readonly path: ReadonlyArray<string>;
	readonly message: string;
}

/** What a file in one directory is rendered with. */
export interface SurqlSettings {
	readonly declared?: DeclaredNames;
	readonly holes?: HoleDelimiters;
	/** Why the directory's schema document could not be read. */
	readonly schemaProblem?: string;
}

/** A project: its options, read once, and what each directory takes. */
export interface SurqlProject {
	/** The directory paths are relative to. */
	readonly root: string;
	readonly problems: ReadonlyArray<SurqlOptionProblem>;
	/** The schema documents read, by absolute path. */
	readonly documents: ReadonlyArray<string>;
	/**
	 * A digest of everything a module depends on besides its own text: the
	 * options and the content of each schema document.
	 */
	readonly identity: string;
	/** The settings for a `.surql` file at this absolute path. */
	readonly settingsFor: (file: string) => SurqlSettings;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
	typeof value === 'object' && value !== null && !Array.isArray(value);

/** Reads `options` as written, with a problem for each it cannot use. */
export const decodeSurqlOptions = (
	raw: unknown,
): {
	readonly options: SurqlMapperOptions;
	readonly problems: ReadonlyArray<SurqlOptionProblem>;
} => {
	const problems: SurqlOptionProblem[] = [];
	if (raw === undefined || raw === null) return { options: {}, problems };
	if (!isRecord(raw))
		return {
			options: {},
			problems: [{ path: [], message: 'options must be an object' }],
		};
	let schema: string | undefined;
	let schemas: Record<string, string> | undefined;
	let holes: HoleDelimiters | undefined;
	for (const [key, value] of Object.entries(raw)) {
		if (key === 'schema') {
			if (typeof value === 'string' && value.length > 0) schema = value;
			else
				problems.push({
					path: ['schema'],
					message: 'schema must be the path of a schema document',
				});
		} else if (key === 'schemas') {
			if (!isRecord(value)) {
				problems.push({
					path: ['schemas'],
					message:
						'schemas must map a directory to the path of a schema document',
				});
				continue;
			}
			schemas = {};
			for (const [directory, path] of Object.entries(value)) {
				if (typeof path === 'string' && path.length > 0)
					schemas[directory] = path;
				else
					problems.push({
						path: ['schemas', directory],
						message:
							'a directory maps to the path of a schema document',
					});
			}
		} else if (key === 'holes') {
			if (
				isRecord(value) &&
				typeof value.open === 'string' &&
				typeof value.close === 'string' &&
				Object.keys(value).length === 2
			)
				holes = { open: value.open, close: value.close };
			else
				problems.push({
					path: ['holes'],
					message:
						'holes must be { "open": <text>, "close": <text> }',
				});
		} else
			problems.push({
				path: [key],
				message: `unknown option ${JSON.stringify(key)}; the options are schema, schemas and holes`,
			});
	}
	return {
		options: {
			...(schema !== undefined && { schema }),
			...(schemas !== undefined && { schemas }),
			...(holes !== undefined && { holes }),
		},
		problems,
	};
};

const messageOf = (cause: unknown): string =>
	cause instanceof Error ? cause.message : String(cause);

type Document =
	| { readonly declared: DeclaredNames; readonly text: string }
	| { readonly problem: string; readonly text: string };

const readDocument = (path: string): Document => {
	let text: string;
	try {
		text = readFileSync(path, 'utf8');
	} catch (cause) {
		return {
			problem: `cannot read ${path}: ${messageOf(cause)}`,
			text: '',
		};
	}
	let json: unknown;
	try {
		json = JSON.parse(text);
	} catch (cause) {
		return { problem: `${path}: ${messageOf(cause)}`, text };
	}
	const decoded = Effect.runSync(Effect.result(decodeDocument(json)));
	if (decoded._tag === 'Failure')
		return { problem: `${path}: ${decoded.failure.message}`, text };
	return { declared: declaredNames(decoded.success), text };
};

/**
 * Opens a project: decodes `raw` and reads each schema document it names,
 * relative to `root`. Never throws; what cannot be used is in `problems`,
 * and a directory whose document cannot be read carries `schemaProblem`.
 */
export const openSurqlProject = (root: string, raw: unknown): SurqlProject => {
	const { options, problems: optionProblems } = decodeSurqlOptions(raw);
	const problems = [...optionProblems];
	const documents = new Map<string, Document>();
	const document = (path: string, at: ReadonlyArray<string>) => {
		const absolute = resolve(root, path);
		let read = documents.get(absolute);
		if (read === undefined) {
			read = readDocument(absolute);
			documents.set(absolute, read);
		}
		if ('problem' in read)
			problems.push({ path: at, message: read.problem });
		return absolute;
	};
	const fallback =
		options.schema === undefined
			? undefined
			: document(options.schema, ['schema']);
	const byDirectory = new Map<string, string>();
	for (const [directory, path] of Object.entries(options.schemas ?? {}))
		byDirectory.set(
			resolve(root, directory),
			document(path, ['schemas', directory]),
		);
	const identity = createHash('sha256')
		.update(
			JSON.stringify([
				options,
				[...documents].map(([path, read]) => [path, read.text]),
			]),
		)
		.digest('hex');
	return {
		root,
		problems,
		documents: [...documents.keys()].sort(),
		identity,
		settingsFor: (file) => {
			const path = byDirectory.get(dirname(resolve(file))) ?? fallback;
			const read = path === undefined ? undefined : documents.get(path);
			return {
				...(options.holes !== undefined && { holes: options.holes }),
				...(read !== undefined &&
					('problem' in read
						? { schemaProblem: read.problem }
						: { declared: read.declared })),
			};
		},
	};
};

/**
 * The text of a `tsconfig.json` as JSON: comments and trailing commas
 * removed outside strings, as TypeScript reads it.
 */
export const stripJsonComments = (text: string): string => {
	let out = '';
	let at = 0;
	while (at < text.length) {
		const char = text[at] as string;
		if (char === '"') {
			let end = at + 1;
			while (end < text.length && text[end] !== '"')
				end += text[end] === '\\' ? 2 : 1;
			out += text.slice(at, end + 1);
			at = end + 1;
		} else if (char === '/' && text[at + 1] === '/') {
			while (at < text.length && text[at] !== '\n') at++;
		} else if (char === '/' && text[at + 1] === '*') {
			const end = text.indexOf('*/', at + 2);
			at = end === -1 ? text.length : end + 2;
		} else {
			out += char;
			at++;
		}
	}
	return out.replace(/,(\s*[}\]])/gu, '$1');
};

/** The package name a `contentMappers` entry names for this mapper. */
export const MAPPER_PACKAGE = 'effect-surrealkit';

/**
 * The `options` of the `effect-surrealkit` entry of `contentMappers` in this
 * `tsconfig.json`, or `undefined` when it has none. The entry is read from
 * this file only, not from a file it `extends`.
 */
export const mapperOptionsIn = (tsconfig: string): unknown => {
	const json = JSON.parse(
		stripJsonComments(readFileSync(tsconfig, 'utf8')),
	) as { readonly contentMappers?: unknown };
	if (!Array.isArray(json.contentMappers)) return undefined;
	const entry = json.contentMappers.find(
		(candidate: unknown) =>
			isRecord(candidate) && candidate.package === MAPPER_PACKAGE,
	) as { readonly options?: unknown } | undefined;
	return entry?.options;
};

/** The nearest `tsconfig.json` at or above `directory`, if any. */
export const nearestTsconfig = (directory: string): string | undefined => {
	for (let at = resolve(directory); ; at = dirname(at)) {
		const candidate = join(at, 'tsconfig.json');
		try {
			if (statSync(candidate).isFile()) return candidate;
		} catch {}
		if (dirname(at) === at) return undefined;
	}
};

const stamp = (path: string): string => {
	try {
		const info = statSync(path);
		return `${info.size}:${info.mtimeMs}`;
	} catch {
		return 'missing';
	}
};

const opened = new Map<
	string,
	{ readonly stamps: string; readonly project: SurqlProject }
>();

/**
 * The project a run-time loader renders `file` with: the `effect-surrealkit`
 * entry of `tsconfig` when given (relative to the working directory), else
 * of the nearest `tsconfig.json` above the file. With none, a project with
 * no options. Read again when the `tsconfig.json` or a schema document
 * changes.
 */
export const surqlProjectFor = (
	file: string,
	tsconfig?: string,
): SurqlProject => {
	const config =
		tsconfig === undefined
			? nearestTsconfig(dirname(resolve(file)))
			: isAbsolute(tsconfig)
				? tsconfig
				: resolve(tsconfig);
	if (config === undefined) return openSurqlProject(dirname(file), undefined);
	const cached = opened.get(config);
	if (
		cached !== undefined &&
		cached.stamps ===
			[config, ...cached.project.documents].map(stamp).join('|')
	)
		return cached.project;
	const project = openSurqlProject(dirname(config), mapperOptionsIn(config));
	opened.set(config, {
		stamps: [config, ...project.documents].map(stamp).join('|'),
		project,
	});
	return project;
};
