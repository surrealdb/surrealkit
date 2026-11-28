/** What the CLI reads from and writes to, so tests can run it in process. */
export interface CliIo {
	readonly cwd: string;
	readonly stdin: () => Promise<string>;
	readonly stdout: (text: string) => void;
	readonly stderr: (text: string) => void;
	/** The process environment; `SURREALKIT_EMIT_PROTOCOL` selects protocol mode. */
	readonly env?: Readonly<Record<string, string | undefined>>;
	/** Ends `statements --watch`; without it, watching runs until the process ends. */
	readonly signal?: AbortSignal;
}

/** Bad arguments: exit 2, with the usage. */
export class UsageError extends Error {}
