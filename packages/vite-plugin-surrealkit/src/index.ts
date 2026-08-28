import { spawn } from 'node:child_process';
import path from 'node:path';

import picomatch from 'picomatch';
import {
	normalizePath,
	type Plugin,
	type ResolvedConfig,
	type ViteDevServer,
} from 'vite';

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
		if (!config) return;

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
						server.ws.send({ type: 'full-reload' });
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

			if (options.schemaGlobs.length > 0) {
				devServer.watcher.add(
					options.schemaGlobs.map((glob) =>
						path.resolve(devServer.config.root, glob),
					),
				);
			}

			const onSchemaEvent =
				(event: 'add' | 'change' | 'unlink') =>
				(filePath: string): void => {
					if (!matchesSchemaFile || !matchesSchemaFile(filePath)) {
						return;
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

			devServer.httpServer?.once('close', () => {
				devServer.watcher.off('add', onAdd);
				devServer.watcher.off('change', onChange);
				devServer.watcher.off('unlink', onUnlink);

				if (timer) {
					clearTimeout(timer);
					timer = undefined;
				}
			});

			if (options.runOnStartup) {
				scheduleSync('startup');
			}
		},
	};
}

export default surrealkitPlugin;
