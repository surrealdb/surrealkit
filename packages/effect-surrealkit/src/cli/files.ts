/**
 * The output guarantees both commands of the bin share: atomic writes per
 * file, unchanged files untouched, stale generated files removed, and files
 * without the generated header never touched.
 */
import { randomBytes } from 'node:crypto';
import {
	mkdir,
	readdir,
	readFile,
	rename,
	rm,
	rmdir,
	writeFile,
} from 'node:fs/promises';
import { dirname, join } from 'node:path';

/** A file to write, at a `/`-separated path relative to the output. */
export interface OutputFile {
	readonly path: string;
	readonly content: string;
}

export interface Drift {
	readonly missing: ReadonlyArray<string>;
	readonly changed: ReadonlyArray<string>;
	readonly stale: ReadonlyArray<string>;
	/** Generated paths held by a file this tool did not write. */
	readonly foreign: ReadonlyArray<string>;
}

const listDir = async (dir: string) => {
	try {
		return await readdir(dir, { withFileTypes: true });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
		throw error;
	}
};

/**
 * The `.ts` files under `dir` (to `depth` levels of directories) that
 * `owned` claims, by `/`-separated relative path.
 */
export const generatedFilesIn = async (
	dir: string,
	owned: (content: string) => boolean,
	depth = 0,
	prefix = '',
): Promise<Map<string, string>> => {
	const found = new Map<string, string>();
	const entries = (await listDir(dir)).sort((a, b) =>
		a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
	);
	for (const entry of entries) {
		const path = `${prefix}${entry.name}`;
		if (entry.isDirectory()) {
			if (depth > 0) {
				const nested = await generatedFilesIn(
					join(dir, entry.name),
					owned,
					depth - 1,
					`${path}/`,
				);
				for (const [key, value] of nested) found.set(key, value);
			}
			continue;
		}
		if (!entry.isFile() || !entry.name.endsWith('.ts')) continue;
		const content = await readFile(join(dir, entry.name), 'utf8');
		if (owned(content)) found.set(path, content);
	}
	return found;
};

export const compareWith = async (
	dir: string,
	files: ReadonlyArray<OutputFile>,
	owned: (content: string) => boolean,
	depth = 0,
): Promise<Drift> => {
	const existing = await generatedFilesIn(dir, owned, depth);
	const missing: string[] = [];
	const changed: string[] = [];
	const foreign: string[] = [];
	for (const file of files) {
		const mine = existing.get(file.path);
		const current =
			mine ??
			(await readFile(join(dir, file.path), 'utf8').catch(
				() => undefined,
			));
		if (current === undefined) missing.push(file.path);
		else if (current !== file.content) {
			changed.push(file.path);
			if (mine === undefined) foreign.push(file.path);
		}
	}
	const produced = new Set(files.map((file) => file.path));
	const stale = [...existing.keys()].filter((path) => !produced.has(path));
	return { missing, changed, stale, foreign };
};

export const writeAtomic = async (
	path: string,
	content: string,
): Promise<void> => {
	const temp = `${path}.${randomBytes(6).toString('hex')}.tmp`;
	await writeFile(temp, content, 'utf8');
	try {
		await rename(temp, path);
	} catch (error) {
		await rm(temp, { force: true });
		throw error;
	}
};

/** Writes what is missing or changed and removes what is stale. */
export const apply = async (
	dir: string,
	files: ReadonlyArray<OutputFile>,
	drift: Drift,
): Promise<number> => {
	await mkdir(dir, { recursive: true });
	for (const file of files) {
		if (
			drift.missing.includes(file.path) ||
			drift.changed.includes(file.path)
		) {
			const path = join(dir, file.path);
			await mkdir(dirname(path), { recursive: true });
			await writeAtomic(path, file.content);
		}
	}
	for (const path of drift.stale) {
		await rm(join(dir, path));
		// A directory the tool emptied goes too; one holding anything else stays.
		let parent = dirname(path);
		while (parent !== '.' && parent !== '') {
			const held = await listDir(join(dir, parent));
			if (held.length > 0) break;
			await rmdir(join(dir, parent));
			parent = dirname(parent);
		}
	}
	return drift.missing.length + drift.changed.length + drift.stale.length;
};

/** The lines `--check` prints for a drift. */
export const problemsOf = (drift: Drift): ReadonlyArray<string> => [
	...drift.missing.map((path) => `missing: ${path}`),
	...drift.changed.map((path) => `out of date or edited: ${path}`),
	...drift.stale.map((path) => `stale: ${path}`),
];
