import { spawn } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';

import picomatch from 'picomatch';
import type { Plugin, ResolvedConfig, ViteDevServer } from 'vite';

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

type RunMode = 'serve' | 'build';
type LogLevel = 'silent' | 'error' | 'info' | 'debug';

export interface SurrealkitPluginOptions {
	/** SurrealKit binary (or executable path). */
	binary?: string;
	/** Working directory for the SurrealKit process. Defaults to Vite root. */
	cwd?: string;
	/** Extra env vars merged with process.env. */
	env?: Record<string, string>;
	/** Additional args appended after `surrealkit sync`. */
	syncArgs?: string[];
	/** Schema modules to sync. Default: whatever `surrealkit sync` selects. */
	schemas?: string[];
	/** Database targets to sync to. Default: the primary/only target. */
	targets?: string[];
	/** Sync every declared module against every declared target. */
	all?: boolean;
	/** Globs (relative to Vite root) that trigger sync in dev. */
	schemaGlobs?: string[];
	/** Where the plugin runs. Default: ['serve']. */
	include?: RunMode[];
	/** Run an initial sync when dev server starts or build begins. Default: true. */
	runOnStartup?: boolean;
	/** Send full-reload after a successful schema-triggered sync in dev. Default: false. */
	reloadOnSync?: boolean;
	/** Debounce window for dev file changes in ms. Default: 150. */
	debounceMs?: number;
	/** Logging verbosity. Default: 'info'. */
	logLevel?: LogLevel;
	/** Fail Vite build if sync exits non-zero. Default: true. */
	failBuildOnError?: boolean;
}

interface ResolvedOptions {
	binary: string;
	cwd?: string;
	env?: Record<string, string>;
	syncArgs: string[];
	schemas: string[];
	targets: string[];
	all: boolean;
	schemaGlobs: string[];
	include: Set<RunMode>;
	runOnStartup: boolean;
	reloadOnSync: boolean;
	debounceMs: number;
	logLevel: LogLevel;
	failBuildOnError: boolean;
}

function resolveOptions(options: SurrealkitPluginOptions): ResolvedOptions {
	return {
		binary: options.binary ?? 'surrealkit',
		cwd: options.cwd,
		env: options.env,
		syncArgs: options.syncArgs ?? [],
		schemas: options.schemas ?? [],
		targets: options.targets ?? [],
		all: options.all ?? false,
		schemaGlobs: options.schemaGlobs ?? DEFAULT_SCHEMA_GLOBS,
		include: new Set(options.include ?? ['serve']),
		runOnStartup: options.runOnStartup ?? true,
		reloadOnSync: options.reloadOnSync ?? false,
		debounceMs: options.debounceMs ?? 150,
		logLevel: options.logLevel ?? 'info',
		failBuildOnError: options.failBuildOnError ?? true,
	};
}

interface CommandResult {
	code: number | null;
	signal: NodeJS.Signals | null;
	output: string;
}

function truncateOutput(value: string, maxChars = 4000): string {
	if (value.length <= maxChars) {
		return value;
	}

	const sliced = value.slice(value.length - maxChars);
	return `...${sliced}`;
}

function runSyncCommand(
	options: ResolvedOptions,
	root: string,
	onDebugLine?: (line: string) => void,
): Promise<CommandResult> {
	return new Promise((resolve, reject) => {
		const commandArgs = [
			'sync',
			...options.schemas.flatMap((name) => ['--schema', name]),
			...options.targets.flatMap((name) => ['--target', name]),
			...(options.all ? ['--all'] : []),
			...options.syncArgs,
		];
		const child = spawn(options.binary, commandArgs, {
			cwd: options.cwd ?? root,
			env: {
				...process.env,
				...options.env,
			},
			shell: process.platform === 'win32',
			stdio: ['ignore', 'pipe', 'pipe'],
		});

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

export function surrealkitPlugin(
	rawOptions: SurrealkitPluginOptions = {},
): Plugin {
	const options = resolveOptions(rawOptions);

	let config: ResolvedConfig | undefined;
	let server: ViteDevServer | undefined;
	let matchesSchemaFile: ((filePath: string) => boolean) | undefined;

	let queued = false;
	let queuedReason: string | undefined;
	let running = false;
	let timer: NodeJS.Timeout | undefined;
	let disposed = false;
	let dispose: (() => void) | undefined;

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

	const runSync = async (reason: string): Promise<void> => {
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
				log.info(`running \`surrealkit sync\` (${currentReason})`);

				const result = await runSyncCommand(
					options,
					config.root,
					(line) => {
						log.debug(line);
					},
				).catch((err: unknown) => {
					const message =
						err instanceof Error ? err.message : String(err);
					log.error(`failed to start SurrealKit process: ${message}`);
					return {
						code: 1,
						signal: null,
						output: '',
					} satisfies CommandResult;
				});

				if (result.code === 0) {
					log.info('Database schema sync completed successfully');

					if (
						options.reloadOnSync &&
						currentReason !== 'startup' &&
						server
					) {
						server.hot.send({ type: 'full-reload' });
						log.debug('sent full-reload to browser');
					}
				} else {
					const exit =
						result.code === null
							? `signal ${result.signal ?? 'unknown'}`
							: `exit code ${result.code}`;
					const detail = result.output ? `\n${result.output}` : '';
					const message = `sync failed (${exit})${detail}`;

					log.error(message);

					if (
						config.command === 'build' &&
						options.failBuildOnError
					) {
						throw new Error(message);
					}
				}

				if (queued) {
					currentReason = queuedReason ?? 'queued';
				}
			} while (queued);
		} finally {
			running = false;
			queuedReason = undefined;
		}
	};

	const scheduleSync = (reason: string): void => {
		if (disposed) return;

		if (timer) clearTimeout(timer);

		timer = setTimeout(() => {
			void runSync(reason);
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
				await runSync('build-start');
			}
		},

		configureServer(devServer) {
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
					scheduleSync(`${event}`);
				};

			const onAdd = onSchemaEvent('add');
			const onChange = onSchemaEvent('change');
			const onUnlink = onSchemaEvent('unlink');

			devServer.watcher.on('add', onAdd);
			devServer.watcher.on('change', onChange);
			devServer.watcher.on('unlink', onUnlink);

			// `httpServer` is null in middleware mode, so teardown hangs off the
			// `closeBundle` hook below, which fires on close in both modes.
			dispose = (): void => {
				if (disposed) return;
				disposed = true;

				devServer.watcher.off('add', onAdd);
				devServer.watcher.off('change', onChange);
				devServer.watcher.off('unlink', onUnlink);

				if (timer) {
					clearTimeout(timer);
					timer = undefined;
				}
			};

			if (options.runOnStartup) {
				scheduleSync('startup');
			}
		},

		// Fires when the dev server closes, in both standalone and middleware
		// mode, and may fire more than once - `dispose` is idempotent.
		closeBundle() {
			dispose?.();
		},
	};
}

export default surrealkitPlugin;
