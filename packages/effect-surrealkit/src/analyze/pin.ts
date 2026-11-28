/**
 * Pinned statement texts: one checked-in `.surql` file per statement, so a
 * statement built in TypeScript is reviewed as SurrealQL and a change to
 * its emitted text shows up in a diff. Pure; the caller reads and writes
 * the files.
 */
import { Data } from 'effect';

import type { StatementModule } from './render.js';

/** What a pin needs from a statement. */
export interface PinnableStatement {
	readonly name: string;
	readonly sql: string;
}

/** Why the pins could not be rendered; `text` is the printed line. */
export interface PinnedStatementProblem {
	readonly kind: 'unnamed' | 'duplicate' | 'path' | 'segment';
	readonly name: string;
	readonly path?: string;
	readonly message: string;
	readonly text: string;
}

/** Every problem with the statements; `message` holds one `text` per line. */
export class PinnedStatementsError extends Data.TaggedError(
	'PinnedStatementsError',
)<{
	readonly problems: ReadonlyArray<PinnedStatementProblem>;
	readonly message: string;
}> {}

/** A pin that is not current, at a path relative to the pin directory. */
export interface PinnedStatementDrift {
	readonly path: string;
	readonly name?: string;
	readonly kind: 'missing' | 'changed' | 'stale';
}

const SEGMENT = /^[A-Za-z0-9_-]+$/u;

const EXTENSION = '.surql';

const byPath = <A extends { readonly path: string }>(a: A, b: A): number =>
	a.path < b.path ? -1 : a.path > b.path ? 1 : 0;

const pins = (
	statements: Iterable<PinnableStatement>,
): ReadonlyArray<StatementModule & { readonly name: string }> => {
	const problems: PinnedStatementProblem[] = [];
	const names = new Set<string>();
	const paths = new Map<string, string>();
	const files: Array<StatementModule & { readonly name: string }> = [];
	for (const { name, sql } of statements) {
		const quoted = JSON.stringify(name);
		if (name === '') {
			const message = 'a statement has no name';
			problems.push({ kind: 'unnamed', name, message, text: message });
			continue;
		}
		if (names.has(name)) {
			const message = `statement ${quoted} is pinned twice`;
			problems.push({ kind: 'duplicate', name, message, text: message });
			continue;
		}
		names.add(name);
		const bad = name.split('.').filter((segment) => !SEGMENT.test(segment));
		if (bad.length > 0) {
			const message = `statement ${quoted} has a name segment that is empty or not letters, digits, _ and -`;
			problems.push({ kind: 'segment', name, message, text: message });
			continue;
		}
		const path = `${name.split('.').join('/')}${EXTENSION}`;
		// Case-folded: `a.B` and `a.b` are one file on a case-insensitive disk.
		const other = paths.get(path.toLowerCase());
		if (other !== undefined) {
			const message = `statements ${JSON.stringify(other)} and ${quoted} both pin to ${path}, ignoring case`;
			problems.push({ kind: 'path', name, path, message, text: message });
			continue;
		}
		paths.set(path.toLowerCase(), name);
		files.push({ path, name, content: `${sql}\n` });
	}
	if (problems.length > 0)
		throw new PinnedStatementsError({
			problems,
			message: problems.map((problem) => problem.text).join('\n'),
		});
	return files.sort(byPath);
};

/**
 * One `.surql` file per statement, sorted by path: the name with each `.`
 * a `/`, plus `.surql`, holding the statement's `sql` and one newline.
 * Throws a {@link PinnedStatementsError} with every unnamed, duplicate,
 * colliding or invalid name. Reads and writes nothing.
 */
export const renderPinnedStatements = (
	statements: Iterable<PinnableStatement>,
): ReadonlyArray<StatementModule> =>
	pins(statements).map(({ path, content }) => ({ path, content }));

/**
 * How the `.surql` files on disk (paths relative to the pin directory,
 * forward slashes) differ from {@link renderPinnedStatements}: `missing`,
 * `changed` (content compared exactly) or `stale` (no statement). Other
 * files are ignored. Empty when the pins are current.
 */
export const pinnedStatementProblems = (
	statements: Iterable<PinnableStatement>,
	files: ReadonlyArray<StatementModule>,
): ReadonlyArray<PinnedStatementDrift> => {
	const expected = pins(statements);
	const onDisk = new Map(
		files
			.filter((file) => file.path.endsWith(EXTENSION))
			.map((file) => [file.path, file.content]),
	);
	const drift: PinnedStatementDrift[] = [];
	for (const pin of expected) {
		const content = onDisk.get(pin.path);
		if (content === undefined)
			drift.push({ path: pin.path, name: pin.name, kind: 'missing' });
		else if (content !== pin.content)
			drift.push({ path: pin.path, name: pin.name, kind: 'changed' });
		onDisk.delete(pin.path);
	}
	for (const path of onDisk.keys()) drift.push({ path, kind: 'stale' });
	return drift.sort(byPath);
};
