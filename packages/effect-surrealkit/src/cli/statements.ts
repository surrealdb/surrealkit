/**
 * `effect-surrealkit statements`: turns authored `.surql` files into typed
 * statement modules, one per scope directory, with the analyzer deriving
 * each part's free names, definitions, results and `THROW` messages.
 *
 * Shares the schema emitter's output guarantees (see `files.ts`); a file at
 * a generated path that this command did not write is refused, never
 * overwritten.
 */
import { readdir, readFile, stat } from 'node:fs/promises';
import { join, relative, resolve, sep } from 'node:path';
import { parseArgs } from 'node:util';

import { Effect } from 'effect';

import { type HoleDelimiters, holeDelimiters } from '../analyze/lexer.js';
import {
	type DeclaredNames,
	renderStatementModulesSync,
	STATEMENTS_MARK,
	StatementModulesError,
} from '../analyze/render.js';
import { declaredNames, decodeDocument } from '../emitter/document.js';
import { TestingError } from '../testing/errors.js';
import {
	StatementVerificationError,
	type VerifyFile,
	verifyStatements,
} from '../testing/verify.js';
import { apply, compareWith, type OutputFile, problemsOf } from './files.js';
import { type CliIo, UsageError } from './io.js';

export const STATEMENTS_USAGE = `Usage: effect-surrealkit statements --input <dir> --out <dir> [options]

Generates typed statement modules from SurrealQL files. --input holds one
directory per scope, each holding .surql files; each scope becomes
<out>/<scope>/statements.ts with one constant per file, and <out>/index.ts
re-exports every scope.

Options:
  --input <dir>              Directory of scope directories (required)
  --out <dir>                Output directory (required, outside --input)
  --check                    Write nothing; exit 1 if the output is not up to date
  --watch                    Regenerate whenever a file under --input changes
  --import-extension <ext>   .js (default) | .ts: extension of index.ts imports
  --hole-open <text>         The text that opens a template hole (default __)
  --hole-close <text>        The text that closes a template hole (default __);
                             a hole is open, a name matching [A-Z][A-Z0-9_]*,
                             then close
  --schema <file>            A SurrealKit schema document v2 (JSON): every table
                             and fn:: function a file names must be declared
                             in it or defined by the same file
  --verify                   Before writing, send each complete text once,
                             with no bindings, to a discarded in-memory
                             engine (needs @surrealdb/node): a parse error or
                             a result count other than the derived one fails
                             generation. The texts run; network, http:: and
                             scripting are denied
  --verify-schema <file>     SurrealQL loaded before the texts are sent, in
                             the order given (repeatable; needs --verify)
  --verify-timeout <seconds> How long each text or schema file may run, a
                             whole number of seconds; a text that does not
                             finish fails generation (default 10; needs --verify)
  --help                     Show this help

Exit codes: 0 success, 1 generation failed or output out of date, 2 bad arguments.
`;

/** First line of every file this command writes. */
export { STATEMENTS_MARK };

/** Why the files could not be generated; every problem, one per line. */
export class StatementsError extends Error {}

/** One `.surql` file: `<scope>/<name>.surql`. */
export interface Source {
	readonly scope: string;
	readonly name: string;
	readonly sql: string;
}

const readSources = async (input: string): Promise<ReadonlyArray<Source>> => {
	const problems: string[] = [];
	const sources: Source[] = [];
	let scopes: Array<import('node:fs').Dirent>;
	try {
		scopes = await readdir(input, { withFileTypes: true });
	} catch (error) {
		throw new StatementsError(
			`cannot read --input ${input}: ${(error as Error).message}`,
		);
	}
	for (const scope of scopes.sort((a, b) => (a.name < b.name ? -1 : 1))) {
		if (scope.name.startsWith('.')) continue;
		if (!scope.isDirectory()) {
			problems.push(
				`${scope.name}: --input holds only scope directories`,
			);
			continue;
		}
		const entries = await readdir(join(input, scope.name), {
			withFileTypes: true,
		});
		let count = 0;
		for (const entry of entries.sort((a, b) =>
			a.name < b.name ? -1 : 1,
		)) {
			if (entry.name.startsWith('.')) continue;
			const path = `${scope.name}/${entry.name}`;
			if (!entry.isFile() || !entry.name.endsWith('.surql')) {
				problems.push(`${path}: a scope holds only .surql files`);
				continue;
			}
			count++;
			sources.push({
				scope: scope.name,
				name: entry.name.slice(0, -'.surql'.length),
				sql: await readFile(join(input, path), 'utf8'),
			});
		}
		if (
			count === 0 &&
			!problems.some((p) => p.startsWith(`${scope.name}/`))
		)
			problems.push(`${scope.name}: has no .surql files`);
	}
	if (problems.length > 0) throw new StatementsError(problems.join('\n'));
	if (sources.length === 0) {
		throw new StatementsError(`${input} has no scope directories`);
	}
	return sources;
};

export interface StatementsOptions {
	readonly importExtension: '.js' | '.ts';
	/** The literal delimiters of a hole; default `__` and `__`. */
	readonly holes?: HoleDelimiters;
	/** The names every scope's files may use; see `StatementScope`. */
	readonly declared?: DeclaredNames;
}

/**
 * The modules for these sources, or every problem with them, through
 * {@link renderStatementModulesSync}. Pure: the caller reads the files and
 * writes the result.
 */
export const renderStatements = (
	sources: ReadonlyArray<Source>,
	options: StatementsOptions,
): {
	readonly files: ReadonlyArray<OutputFile>;
	readonly warnings: ReadonlyArray<string>;
} => {
	const scopes = new Map<string, Array<{ name: string; sql: string }>>();
	for (const { scope, name, sql } of sources) {
		const files = scopes.get(scope) ?? [];
		files.push({ name, sql });
		scopes.set(scope, files);
	}
	const { declared, ...rest } = options;
	try {
		const { files, warnings } = renderStatementModulesSync({
			scopes: [...scopes].map(([name, files]) => ({
				name,
				files,
				...(declared !== undefined && { declared }),
			})),
			...rest,
		});
		return { files, warnings: warnings.map((warning) => warning.text) };
	} catch (error) {
		if (error instanceof StatementModulesError)
			throw new StatementsError(error.message);
		throw error;
	}
};

const WATCH_INTERVAL_MS = 200;

const pause = (ms: number, signal: AbortSignal | undefined): Promise<void> =>
	new Promise((done) => {
		const timer = setTimeout(done, ms);
		signal?.addEventListener(
			'abort',
			() => {
				clearTimeout(timer);
				done();
			},
			{ once: true },
		);
	});

/** Names, sizes and modification times of everything under `dir`. */
const fingerprint = async (dir: string, prefix = ''): Promise<string> => {
	const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
	const parts: string[] = [];
	for (const entry of entries.sort((a, b) => (a.name < b.name ? -1 : 1))) {
		const path = join(dir, entry.name);
		if (entry.isDirectory()) {
			parts.push(
				`${prefix}${entry.name}/`,
				await fingerprint(path, `${prefix}${entry.name}/`),
			);
		} else {
			const info = await stat(path).catch(() => undefined);
			parts.push(`${prefix}${entry.name}:${info?.size}:${info?.mtimeMs}`);
		}
	}
	return parts.join('\n');
};

const owned = (content: string): boolean => content.startsWith(STATEMENTS_MARK);

const inside = (parent: string, child: string): boolean => {
	const path = relative(parent, child);
	return path === '' || (!path.startsWith('..') && !path.startsWith(sep));
};

/**
 * `--verify`: sends the sources to a discarded in-memory engine through
 * {@link verifyStatements} and reports the outcome; false when generation
 * must stop.
 */
const verify = async (
	sources: ReadonlyArray<Source>,
	schemaPaths: ReadonlyArray<string>,
	io: CliIo,
	holes: HoleDelimiters | undefined,
	timeoutSeconds: number | undefined,
): Promise<boolean> => {
	const report = (lines: string) =>
		io.stderr(
			`effect-surrealkit: statements not verified:\n${lines
				.split('\n')
				.map((line) => `  ${line}`)
				.join('\n')}\n`,
		);
	const schema: VerifyFile[] = [];
	for (const path of schemaPaths) {
		try {
			schema.push({
				name: path,
				sql: await readFile(resolve(io.cwd, path), 'utf8'),
			});
		} catch (error) {
			report(
				`cannot read --verify-schema ${path}: ${(error as Error).message}`,
			);
			return false;
		}
	}
	const scopes = new Map<string, Array<{ name: string; sql: string }>>();
	for (const { scope, name, sql } of sources) {
		const files = scopes.get(scope) ?? [];
		files.push({ name, sql });
		scopes.set(scope, files);
	}
	const result = await Effect.runPromise(
		Effect.result(
			verifyStatements({
				scopes: [...scopes].map(([name, files]) => ({ name, files })),
				schema,
				...(holes !== undefined && { holes }),
				...(timeoutSeconds !== undefined && { timeoutSeconds }),
			}),
		),
	);
	if (result._tag === 'Failure') {
		const error = result.failure;
		if (error instanceof StatementVerificationError)
			report(`on SurrealDB ${error.engine}:\n${error.message}`);
		else if (error instanceof TestingError) report(error.message);
		else throw error;
		return false;
	}
	const { engine, sent, notSent } = result.success;
	io.stderr(
		`effect-surrealkit: verified ${sent.length} texts on SurrealDB ${engine} (${notSent.length} fragments and templates not sent)\n`,
	);
	return true;
};

/** Runs `statements`; resolves to the exit code. */
export const statements = async (
	argv: ReadonlyArray<string>,
	io: CliIo,
): Promise<number> => {
	const { values } = parseArgs({
		args: [...argv],
		options: {
			input: { type: 'string' },
			out: { type: 'string' },
			check: { type: 'boolean' },
			watch: { type: 'boolean' },
			'import-extension': { type: 'string' },
			'hole-open': { type: 'string' },
			'hole-close': { type: 'string' },
			schema: { type: 'string' },
			verify: { type: 'boolean' },
			'verify-schema': { type: 'string', multiple: true },
			'verify-timeout': { type: 'string' },
			help: { type: 'boolean' },
		},
		strict: true,
		allowPositionals: false,
	});
	if (values.help) {
		io.stdout(STATEMENTS_USAGE);
		return 0;
	}
	if (values.input === undefined)
		throw new UsageError('--input <dir> is required');
	if (values.out === undefined)
		throw new UsageError('--out <dir> is required');
	if (values.check && values.watch)
		throw new UsageError('--check and --watch cannot be combined');
	const verifySchema = values['verify-schema'] ?? [];
	if (verifySchema.length > 0 && !values.verify)
		throw new UsageError('--verify-schema needs --verify');
	const verifyTimeout = values['verify-timeout'];
	if (verifyTimeout !== undefined && !values.verify)
		throw new UsageError('--verify-timeout needs --verify');
	if (verifyTimeout !== undefined && !/^[1-9]\d*$/u.test(verifyTimeout))
		throw new UsageError(
			'--verify-timeout must be a whole number of seconds, at least 1',
		);
	const timeoutSeconds =
		verifyTimeout === undefined ? undefined : Number(verifyTimeout);
	const importExtension = values['import-extension'] ?? '.js';
	if (importExtension !== '.js' && importExtension !== '.ts')
		throw new UsageError('--import-extension must be .js or .ts');
	const holeOpen = values['hole-open'];
	const holeClose = values['hole-close'];
	let holes: HoleDelimiters | undefined;
	if (holeOpen !== undefined || holeClose !== undefined) {
		try {
			holes = holeDelimiters({
				open: holeOpen ?? '__',
				close: holeClose ?? '__',
			});
		} catch (error) {
			throw new UsageError(
				`--hole-open/--hole-close: ${(error as Error).message}`,
			);
		}
	}
	const input = resolve(io.cwd, values.input);
	const out = resolve(io.cwd, values.out);
	if (inside(input, out))
		throw new UsageError('--out must be outside --input');
	let declared: DeclaredNames | undefined;
	if (values.schema !== undefined) {
		const path = resolve(io.cwd, values.schema);
		let json: unknown;
		try {
			json = JSON.parse(await readFile(path, 'utf8'));
		} catch (error) {
			throw new UsageError(
				`--schema ${values.schema}: ${(error as Error).message}`,
			);
		}
		const decoded = await Effect.runPromise(
			Effect.result(decodeDocument(json)),
		);
		if (decoded._tag === 'Failure')
			throw new UsageError(
				`--schema ${values.schema}: ${decoded.failure.message}`,
			);
		declared = declaredNames(decoded.success);
	}
	const options: StatementsOptions = {
		importExtension,
		...(holes !== undefined && { holes }),
		...(declared !== undefined && { declared }),
	};

	const once = async (): Promise<number> => {
		let rendered: ReturnType<typeof renderStatements>;
		let sources: ReadonlyArray<Source>;
		try {
			sources = await readSources(input);
			rendered = renderStatements(sources, options);
		} catch (error) {
			if (error instanceof StatementsError) {
				io.stderr(
					`effect-surrealkit: statements not generated:\n${error.message
						.split('\n')
						.map((line) => `  ${line}`)
						.join('\n')}\n`,
				);
				return 1;
			}
			throw error;
		}
		const { files, warnings } = rendered;
		for (const warning of warnings)
			io.stderr(`effect-surrealkit: warning: ${warning}\n`);
		if (values.verify) {
			const verified = await verify(
				sources,
				verifySchema,
				io,
				holes,
				timeoutSeconds,
			);
			if (!verified) return 1;
		}
		const drift = await compareWith(out, files, owned, 1);
		if (values.check) {
			const problems = problemsOf(drift);
			if (problems.length > 0) {
				io.stderr(
					`effect-surrealkit: ${out} is not up to date:\n${problems.map((p) => `  ${p}`).join('\n')}\n`,
				);
				return 1;
			}
			return 0;
		}
		if (drift.foreign.length > 0) {
			io.stderr(
				`effect-surrealkit: refusing to overwrite files this command did not generate:\n${drift.foreign.map((p) => `  ${p}`).join('\n')}\n`,
			);
			return 1;
		}
		const touched = await apply(out, files, drift);
		io.stderr(
			`effect-surrealkit: ${files.length} modules in ${out} (${touched === 0 ? 'unchanged' : `${touched} updated`})\n`,
		);
		return 0;
	};

	const first = await once();
	if (!values.watch) return first;

	// Polls rather than using fs.watch: file events are unreliable across
	// platforms, network volumes and sandboxes, and a scope tree is small.
	const { signal } = io;
	let seen = await fingerprint(input);
	io.stderr(`effect-surrealkit: watching ${input}\n`);
	const stopped = () => signal?.aborted === true;
	while (!stopped()) {
		await pause(WATCH_INTERVAL_MS, signal);
		if (stopped()) break;
		const now = await fingerprint(input);
		if (now === seen) continue;
		seen = now;
		await once().catch((error: unknown) => {
			io.stderr(`effect-surrealkit: ${(error as Error).message}\n`);
		});
	}
	return 0;
};
