import { type ChildProcess, spawnSync } from 'node:child_process';

/** A started SurrealKit process, as far as stopping it is concerned. */
export interface Stoppable {
	child: ChildProcess;
	/** Spawned as the leader of its own process group (POSIX generate mode). */
	group: boolean;
}

type RunSync = (
	command: string,
	args: string[],
	options: { stdio: 'ignore'; windowsHide: boolean },
) => unknown;

/**
 * Sends `signal` to a command and everything it started. On POSIX that is its
 * process group when it leads one. On Windows the command runs under
 * `cmd.exe` (`shell: true`), so `child.kill()` would stop only the shell:
 * `taskkill /T /F` ends the whole tree (it has no gentler mode for console
 * processes).
 */
export function killTree(
	{ child, group }: Stoppable,
	signal: NodeJS.Signals,
	platform: NodeJS.Platform = process.platform,
	runSync: RunSync = spawnSync,
): void {
	const pid = child.pid;
	if (pid === undefined) return;
	if (platform === 'win32') {
		runSync('taskkill', ['/pid', String(pid), '/T', '/F'], {
			stdio: 'ignore',
			windowsHide: true,
		});
		return;
	}
	if (group) {
		try {
			process.kill(-pid, signal);
			return;
		} catch {
			// The group is already gone; fall through to the child itself.
		}
	}
	try {
		child.kill(signal);
	} catch {
		// Already exited.
	}
}

const active = new Set<Stoppable>();
const SIGNALS = ['SIGINT', 'SIGHUP'] as const;
let installed = false;

const stopAll = (): void => {
	for (const running of active) killTree(running, 'SIGTERM');
	active.clear();
};

const onSignal = (signal: NodeJS.Signals): void => {
	stopAll();
	uninstall();
	// A listener replaces Node's default, which ends the process, and other
	// listeners (`signal-exit`, used by Vite's toolchain) stand down while
	// another one is registered. With ours removed, raise the signal again so
	// that the process ends as it would have without this guard.
	process.kill(process.pid, signal);
};

function install(): void {
	if (installed) return;
	installed = true;
	process.on('exit', stopAll);
	for (const signal of SIGNALS) process.on(signal, onSignal);
}

function uninstall(): void {
	if (!installed) return;
	installed = false;
	process.off('exit', stopAll);
	for (const signal of SIGNALS) process.off(signal, onSignal);
}

/**
 * Stops `running` if this process exits or receives SIGINT or SIGHUP while it
 * runs. A generation runs in its own process group, so the terminal's Ctrl-C
 * does not reach it, and Vite itself handles only SIGTERM. The handlers are
 * installed only while some command runs.
 */
export function stopOnExit(running: Stoppable, exited: Promise<void>): void {
	active.add(running);
	install();
	void exited.then(() => {
		active.delete(running);
		if (active.size === 0) uninstall();
	});
}

/** Whether the exit and signal handlers are installed (for tests). */
export function exitGuardInstalled(): boolean {
	return installed;
}
