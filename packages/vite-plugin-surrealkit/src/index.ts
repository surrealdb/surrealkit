import { type ChildProcess, spawn } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';

import picomatch from 'picomatch';
import type { Plugin, ResolvedConfig, ViteDevServer } from 'vite';

import { killTree, stopOnExit } from './process.js';

// Inlined from Vite so that nothing is imported from `vite` at runtime: the
// plugin only depends on Vite's types, never on its export surface.
const isWindows = process.platform === 'win32';

function normalizePath(id: string): string {
	return path.posix.normalize(isWindows ? id.replace(/\\/g, '/') : id);
}

// Covers both the default module (`database/schema`) and named modules
// (`database/modules/<name>/schema`). Override with `schemaGlobs` for a custom
// `[schema.<name>] path`.
const DEFAULT_SCHEMA_GLOBS = [
	'database/schema/**/*.surql',
	'database/modules/*/schema/**/*.surql',
];

/** How long a generation child gets to exit after SIGTERM on close. */
const KILL_GRACE_MS = 3000;

type RunMode = 'serve' | 'build';
type LogLevel = 'silent' | 'error' | 'info' | 'debug';

/**
 * - `'sync'` runs `surrealkit sync`, which applies the schema to the
 *   configured database.
 * - `'generate'` runs `surrealkit compile`, which builds the schema document
 *   in a throwaway embedded engine and runs the configured `[typegen]`
 *   emitters. It never connects to or changes a database.
 */
type PluginMode = 'sync' | 'generate';

export interface SurrealkitPluginOptions {
	/** What a schema change runs. Default: `'sync'`. */
	mode?: PluginMode;
	/** SurrealKit binary (or executable path). */
	binary?: string;
	/** Working directory for the SurrealKit process. Defaults to Vite root. */
	cwd?: string;
	/** Extra env vars merged with process.env. */
	env?: Record<string, string>;
	/** Additional args appended after `surrealkit sync` (sync mode). */
	syncArgs?: string[];
	/** Additional args appended after `surrealkit compile` (generate mode). */
	generateArgs?: string[];
	/** Schema modules to sync or compile. Default: SurrealKit's selection. */
	schemas?: string[];
	/** Database targets to sync to (sync mode). Default: the primary/only target. */
	targets?: string[];
	/** Sync every declared module against every declared target (sync mode). */
	all?: boolean;
	/** Globs (relative to Vite root) that trigger a run in dev. */
	schemaGlobs?: string[];
	/**
	 * Directories (relative to Vite root) the generation writes (generate
	 * mode). Vite's per-file updates for them are suppressed while the plugin
	 * is active; one full reload follows a successful generation that changed
	 * them, so the browser never sees a half-written module set.
	 */
	generatedDirs?: string[];
	/** Where the plugin runs. Default: ['serve']. */
	include?: RunMode[];
	/** Run when dev server starts or build begins. Default: true. */
	runOnStartup?: boolean;
	/** Send full-reload after a successful schema-triggered sync in dev. Default: false. */
	reloadOnSync?: boolean;
	/** Send full-reload after a successful generation in dev (generate mode). Default: true. */
	reloadOnGenerate?: boolean;
	/** Debounce window for dev file changes in ms. Default: 150. */
	debounceMs?: number;
	/** Logging verbosity. Default: 'info'. */
	logLevel?: LogLevel;
	/** Fail Vite build if the run exits non-zero. Default: true. */
	failBuildOnError?: boolean;
}

interface ResolvedOptions {
	mode: PluginMode;
	binary: string;
	cwd?: string;
	env?: Record<string, string>;
	syncArgs: string[];
	generateArgs: string[];
	schemas: string[];
	targets: string[];
	all: boolean;
	schemaGlobs: string[];
	generatedDirs: string[];
	include: Set<RunMode>;
	runOnStartup: boolean;
	reloadOnSync: boolean;
	reloadOnGenerate: boolean;
	debounceMs: number;
	logLevel: LogLevel;
	failBuildOnError: boolean;
}

function resolveOptions(options: SurrealkitPluginOptions): ResolvedOptions {
	const mode = options.mode ?? 'sync';
	if (mode !== 'sync' && mode !== 'generate') {
		throw new Error(
			`[vite-plugin-surrealkit] unknown mode ${JSON.stringify(mode)}; expected 'sync' or 'generate'`,
		);
	}
	if (mode === 'generate') {
		// Generation never connects to a database, so database options are
		// mistakes, not no-ops.
		const misplaced = [
			options.targets !== undefined && 'targets',
			options.all !== undefined && 'all',
			options.syncArgs !== undefined && 'syncArgs',
			options.reloadOnSync !== undefined && 'reloadOnSync',
		].filter(Boolean);
		if (misplaced.length > 0) {
			throw new Error(
				`[vite-plugin-surrealkit] ${misplaced.join(', ')} only apply to mode 'sync'; mode 'generate' never connects to a database`,
			);
		}
	} else {
		const misplaced = [
			options.generateArgs !== undefined && 'generateArgs',
			options.generatedDirs !== undefined && 'generatedDirs',
			options.reloadOnGenerate !== undefined && 'reloadOnGenerate',
		].filter(Boolean);
		if (misplaced.length > 0) {
			throw new Error(
				`[vite-plugin-surrealkit] ${misplaced.join(', ')} only apply to mode 'generate'`,
			);
		}
	}

	return {
		mode,
		binary: options.binary ?? 'surrealkit',
		cwd: options.cwd,
		env: options.env,
		syncArgs: options.syncArgs ?? [],
		generateArgs: options.generateArgs ?? [],
		schemas: options.schemas ?? [],
		targets: options.targets ?? [],
		all: options.all ?? false,
		schemaGlobs: options.schemaGlobs ?? DEFAULT_SCHEMA_GLOBS,
		generatedDirs: options.generatedDirs ?? [],
		include: new Set(options.include ?? ['serve']),
		runOnStartup: options.runOnStartup ?? true,
		reloadOnSync: options.reloadOnSync ?? false,
		reloadOnGenerate: options.reloadOnGenerate ?? true,
		debounceMs: options.debounceMs ?? 150,
		logLevel: options.logLevel ?? 'info',
		failBuildOnError: options.failBuildOnError ?? true,
	};
}

/** The SurrealKit argv for one run. `generate` is never `sync`. */
function commandArgs(options: ResolvedOptions): string[] {
	const schemas = options.schemas.flatMap((name) => ['--schema', name]);
	if (options.mode === 'generate') {
		return ['compile', ...schemas, ...options.generateArgs];
	}
	return [
		'sync',
		...schemas,
		...options.targets.flatMap((name) => ['--target', name]),
		...(options.all ? ['--all'] : []),
		...options.syncArgs,
	];
}

interface CommandResult {
	code: number | null;
	signal: NodeJS.Signals | null;
	output: string;
}

interface RunningCommand {
	child: ChildProcess;
	/** Settles when the process exits, even if a grandchild keeps stdio open. */
	exited: Promise<void>;
	/** Spawned as the leader of its own process group (POSIX generate mode). */
	group: boolean;
	result: Promise<CommandResult>;
}

function truncateOutput(value: string, maxChars = 4000): string {
	if (value.length <= maxChars) {
		return value;
	}

	const sliced = value.slice(value.length - maxChars);
	return `...${sliced}`;
}

function startCommand(
	options: ResolvedOptions,
	root: string,
	onDebugLine?: (line: string) => void,
): RunningCommand {
	// A generation runs in its own process group so that closing the dev
	// server stops it together with the emitters it spawned.
	const group = options.mode === 'generate' && !isWindows;
	const child = spawn(options.binary, commandArgs(options), {
		cwd: options.cwd ?? root,
		env: {
			...process.env,
			...options.env,
		},
		shell: process.platform === 'win32',
		stdio: ['ignore', 'pipe', 'pipe'],
		detached: group,
	});
	const exited = new Promise<void>((resolve) => {
		child.once('exit', () => resolve());
		child.once('error', () => resolve());
	});

	const result = new Promise<CommandResult>((resolve, reject) => {
		let output = '';

		const consume = (chunk: string): void => {
			output += chunk;
			if (!onDebugLine) {
				return;
			}

			const lines = chunk
				.split('\n')
				.map((line) => line.trimEnd())
				.filter(Boolean);
			for (const line of lines) {
				onDebugLine(line);
			}
		};

		child.stdout?.setEncoding('utf8');
		child.stderr?.setEncoding('utf8');
		child.stdout?.on('data', consume);
		child.stderr?.on('data', consume);

		child.on('error', reject);
		child.on('close', (code, signal) => {
			resolve({
				code,
				signal,
				output: truncateOutput(output.trim()),
			});
		});
	});

	// Ctrl-C and SIGHUP do not reach a generation in its own process group.
	if (options.mode === 'generate') stopOnExit({ child, group }, exited);

	return { child, exited, group, result };
}

/** Stops a child process: SIGTERM, then SIGKILL after a grace period. */
async function terminate(running: RunningCommand): Promise<void> {
	const { child, exited, group } = running;
	if (child.exitCode !== null || child.signalCode !== null) return;

	const signal = (name: NodeJS.Signals): void =>
		killTree({ child, group }, name);

	signal('SIGTERM');
	const timer = setTimeout(() => {
		if (child.exitCode === null && child.signalCode === null) {
			signal('SIGKILL');
		}
	}, KILL_GRACE_MS);
	try {
		await exited;
	} finally {
		clearTimeout(timer);
	}
}

/**
 * Directories to hand to `ViteDevServer.watcher.add()` for a set of globs.
 *
 * Vite builds its chokidar watcher with `disableGlobbing: true`, so a glob
 * passed to `watcher.add()` is treated as a literal path. On Vite 8 registering
 * such a (non-existent) path suppresses change events for the real files
 * alongside it, which silently broke schema watching. Add the globs' existing
 * base directories instead and let `createMatcher` do the filtering.
 */
function watchRoots(root: string, globs: string[]): string[] {
	const dirs = new Set<string>();

	for (const glob of globs) {
		const { base } = picomatch.scan(glob);
		let dir = path.resolve(root, base);

		// Fall back to the nearest existing ancestor, never above the root.
		while (
			!existsSync(dir) &&
			dir !== root &&
			dir.startsWith(root + path.sep)
		) {
			dir = path.dirname(dir);
		}

		if (existsSync(dir)) {
			dirs.add(dir);
		}
	}

	return [...dirs];
}

/** Cheap content fingerprint, or `undefined` if the file is unreadable. */
function fingerprint(file: string): string | undefined {
	try {
		const stats = statSync(file, { bigint: true });
		return `${stats.mtimeNs}:${stats.size}`;
	} catch {
		return undefined;
	}
}

/**
 * Fingerprints of the schema files that already exist under `dirs`.
 *
 * Handing a directory to `watcher.add()` makes chokidar re-announce a tree it
 * may already be watching, re-emitting `add` and `change` for files nobody
 * touched. Comparing fingerprints tells those apart from real edits, which
 * always move the mtime.
 */
function snapshotExisting(
	dirs: string[],
	matches: (filePath: string) => boolean,
): Map<string, string> {
	const files = new Map<string, string>();

	for (const dir of dirs) {
		let entries: string[];
		try {
			entries = readdirSync(dir, { recursive: true }) as string[];
		} catch {
			continue;
		}

		for (const entry of entries) {
			const file = path.join(dir, entry);
			if (!matches(file)) continue;

			const print = fingerprint(file);
			if (print !== undefined) {
				files.set(file, print);
			}
		}
	}

	return files;
}

/** One string describing every file under `dirs`, for change detection. */
function snapshotKey(dirs: string[]): string {
	const files = snapshotExisting(dirs, () => true);
	return [...files.entries()]
		.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
		.map(([file, print]) => `${file}=${print}`)
		.join('\n');
}

function createMatcher(
	root: string,
	globs: string[],
): (filePath: string) => boolean {
	const matchers = globs.map((glob) => picomatch(glob));

	return (filePath: string): boolean => {
		const rel = normalizePath(path.relative(root, filePath));
		return matchers.some((matcher) => matcher(rel));
	};
}

function isInside(dir: string, file: string): boolean {
	const relative = path.relative(dir, file);
	return (
		relative === '' ||
		(!relative.startsWith('..') && !path.isAbsolute(relative))
	);
}

export function surrealkitPlugin(
	rawOptions: SurrealkitPluginOptions = {},
): Plugin {
	const options = resolveOptions(rawOptions);
	const generating = options.mode === 'generate';
	const verb = generating ? 'compile' : 'sync';

	let config: ResolvedConfig | undefined;
	let server: ViteDevServer | undefined;
	let matchesSchemaFile: ((filePath: string) => boolean) | undefined;
	let generatedDirs: string[] = [];
	// Files whose per-file update was held back while a generation ran.
	const heldBack = new Set<string>();

	let queued = false;
	let queuedReason: string | undefined;
	let running = false;
	let current: RunningCommand | undefined;
	let timer: NodeJS.Timeout | undefined;
	let disposed = false;
	let dispose: (() => Promise<void>) | undefined;

	const log = {
		error: (message: string): void => {
			if (options.logLevel === 'silent') {
				return;
			}

			(config?.logger ?? console).error(
				`[vite-plugin-surrealkit] ${message}`,
			);
		},
		info: (message: string): void => {
			if (options.logLevel === 'silent' || options.logLevel === 'error') {
				return;
			}

			(config?.logger ?? console).info(
				`[vite-plugin-surrealkit] ${message}`,
			);
		},
		debug: (message: string): void => {
			if (options.logLevel !== 'debug') {
				return;
			}

			(config?.logger ?? console).info(
				`[vite-plugin-surrealkit] ${message}`,
			);
		},
	};

	const reloadAfter = (reason: string, before: string | undefined): void => {
		if (!server || reason === 'startup') return;

		if (!generating) {
			if (options.reloadOnSync) {
				server.hot.send({ type: 'full-reload' });
				log.debug('sent full-reload to browser');
			}
			return;
		}

		// Updates held back while the generation ran reach the browser only this way.
		const held = heldBack.size;
		heldBack.clear();
		if (held > 0) {
			server.hot.send({ type: 'full-reload' });
			log.debug(
				`sent full-reload to browser (${held} held-back updates)`,
			);
			return;
		}
		if (!options.reloadOnGenerate) return;
		// Only after a successful run, and only when the published output moved.
		if (before !== undefined && snapshotKey(generatedDirs) === before) {
			log.debug('generated files unchanged; no reload');
			return;
		}
		server.hot.send({ type: 'full-reload' });
		log.debug('sent full-reload to browser');
	};

	const runOnce = async (reason: string): Promise<void> => {
		if (!config) return;

		log.info(`running \`surrealkit ${verb}\` (${reason})`);
		const before =
			generating && generatedDirs.length > 0
				? snapshotKey(generatedDirs)
				: undefined;

		let result: CommandResult;
		try {
			current = startCommand(options, config.root, (line) => {
				log.debug(line);
			});
			result = await current.result;
		} catch (err: unknown) {
			const message = err instanceof Error ? err.message : String(err);
			log.error(`failed to start SurrealKit process: ${message}`);
			result = { code: 1, signal: null, output: '' };
		} finally {
			current = undefined;
		}

		// Closed while running: the child was terminated on purpose.
		if (disposed) return;

		if (result.code === 0) {
			log.info(
				generating
					? 'Schema compile and type generation completed successfully'
					: 'Database schema sync completed successfully',
			);
			reloadAfter(reason, before);
			return;
		}

		const exit =
			result.code === null
				? `signal ${result.signal ?? 'unknown'}`
				: `exit code ${result.code}`;
		const detail = result.output ? `\n${result.output}` : '';
		const message = `${verb} failed (${exit})${detail}`;

		log.error(message);
		if (heldBack.size > 0) {
			// A failed generation never reloads: its output may be half published.
			log.info(
				`${heldBack.size} file update(s) arrived during the failed generation and were held back; reload the page to apply them`,
			);
			heldBack.clear();
		}

		if (config.command === 'build' && options.failBuildOnError) {
			throw new Error(message);
		}
	};

	const run = async (reason: string): Promise<void> => {
		if (!config || disposed) return;

		if (running) {
			queued = true;
			queuedReason = reason;
			return;
		}

		running = true;
		let currentReason = reason;
		try {
			do {
				queued = false;
				queuedReason = undefined;
				await runOnce(currentReason);

				if (queued) {
					currentReason = queuedReason ?? 'queued';
				}
			} while (queued && !disposed);
		} finally {
			running = false;
			queuedReason = undefined;
		}
	};

	const schedule = (reason: string): void => {
		if (disposed) return;

		if (timer) clearTimeout(timer);

		timer = setTimeout(() => {
			void run(reason);
		}, options.debounceMs);
	};

	return {
		name: 'vite-plugin-surrealkit',

		configResolved(resolved) {
			config = resolved;
			matchesSchemaFile = createMatcher(
				resolved.root,
				options.schemaGlobs,
			);
			generatedDirs = options.generatedDirs.map((dir) =>
				path.resolve(resolved.root, dir),
			);

			if (options.include.size === 0) {
				log.error(
					'no include modes configured; plugin is effectively disabled',
				);
			}
		},

		async buildStart() {
			if (
				!config ||
				!options.include.has('build') ||
				config.command !== 'build'
			) {
				return;
			}

			if (options.runOnStartup) {
				await run('build-start');
			}
		},

		async configureServer(devServer) {
			if (!options.include.has('serve')) {
				return;
			}

			server = devServer;

			const dirs = watchRoots(devServer.config.root, options.schemaGlobs);
			const known = snapshotExisting(dirs, (file) =>
				Boolean(matchesSchemaFile?.(file)),
			);

			for (const dir of dirs) {
				devServer.watcher.add(dir);
			}

			const onSchemaEvent =
				(event: 'add' | 'change' | 'unlink') =>
				(filePath: string): void => {
					if (!matchesSchemaFile || !matchesSchemaFile(filePath)) {
						return;
					}

					if (event === 'unlink') {
						known.delete(filePath);
					} else {
						const print = fingerprint(filePath);

						// Nothing moved: this is `watcher.add()` (or Vite's
						// own watcher) re-announcing a file, not an edit.
						if (print !== undefined) {
							if (known.get(filePath) === print) return;
							known.set(filePath, print);
						}
					}

					log.debug(
						`${event}: ${normalizePath(path.relative(devServer.config.root, filePath))}`,
					);
					schedule(`${event}`);
				};

			const onAdd = onSchemaEvent('add');
			const onChange = onSchemaEvent('change');
			const onUnlink = onSchemaEvent('unlink');

			devServer.watcher.on('add', onAdd);
			devServer.watcher.on('change', onChange);
			devServer.watcher.on('unlink', onUnlink);

			// `httpServer` is null in middleware mode, so teardown hangs off the
			// `closeBundle` hook below, which fires on close in both modes.
			dispose = async (): Promise<void> => {
				if (disposed) return;
				disposed = true;

				devServer.watcher.off('add', onAdd);
				devServer.watcher.off('change', onChange);
				devServer.watcher.off('unlink', onUnlink);

				if (timer) {
					clearTimeout(timer);
					timer = undefined;
				}

				// A generation half-way through is safe to stop: emitters write
				// atomically per file and the next run regenerates everything.
				// A sync is left to finish against its database.
				if (generating && current) {
					log.debug('terminating the running generation');
					await terminate(current);
				}
			};

			if (!options.runOnStartup) return;

			if (generating) {
				// Modules importing generated contracts must not be served
				// before the first generation has published them.
				await run('startup');
			} else {
				schedule('startup');
			}
		},

		// Per-file updates of generated output are replaced by one full reload
		// after a successful generation. While a generation runs, its module set
		// may be half published, so no per-file update goes out at all, whether
		// or not `generatedDirs` names where it lands: the run ends with a reload.
		hotUpdate({ file }) {
			if (!generating || !server) return;
			if (generatedDirs.some((dir) => isInside(dir, file))) return [];
			if (running) {
				heldBack.add(file);
				return [];
			}
		},

		// Fires when the dev server closes, in both standalone and middleware
		// mode, and may fire more than once - `dispose` is idempotent.
		async closeBundle() {
			await dispose?.();
		},
	};
}

export default surrealkitPlugin;
