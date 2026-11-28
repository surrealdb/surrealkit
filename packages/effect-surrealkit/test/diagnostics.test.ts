/**
 * The compiler's messages are this package's interface for a mistake, so
 * each negative type fixture's diagnostic is pinned: the fixtures are
 * compiled with their `@ts-expect-error` directives switched off, and every
 * case's error codes and message text (absolute paths removed) must match
 * the snapshot. A case is named by the `[name]` after its directive.
 */
import { join, relative } from 'node:path';

import ts from 'typescript';
import { beforeAll, describe, expect, it } from 'vitest';

import { compile, packageRoot, textOf, typesRoot } from './compiler.js';

const fixtures = [
	'negative.ts',
	'statement-negative.ts',
	'block-negative.ts',
	'search-negative.ts',
	'graph-negative.ts',
	'binds-negative.ts',
	'pipeline-negative.ts',
	'interactive-negative.ts',
	'bulk-negative.ts',
	'stream-negative.ts',
	'live-negative.ts',
].map((file) => join(typesRoot, file));

const directive = /^\s*\/\/ @ts-expect-error(?: \[([^\]\s]+)\])?/u;
/** Same length, so every position stays where it was. */
const switchedOff = '@ts-expect-(off)';

interface Case {
	readonly name: string | undefined;
	readonly file: string;
	/** The 0-based line of the directive. */
	readonly line: number;
}

const cases: ReadonlyArray<Case> = fixtures.flatMap((file) =>
	textOf(file)
		.split('\n')
		.flatMap((text, line) => {
			const match = directive.exec(text);
			return match === null ? [] : [{ name: match[1], file, line }];
		}),
);

const normalised = (text: string): string =>
	// TypeScript truncates qualified generic arguments before paths are normalised.
	// Keep the outer symbol and the subsequent diagnostic chain for those excerpts.
	text
		.split(packageRoot)
		.join('<package>')
		.replace(/('import\("[^"]+"\)\.\w+)<[^'\n]*?\.\.\.'/gu, "$1<...>'");

/**
 * The directive a diagnostic belongs to, found as the compiler finds it:
 * the nearest line above that is not blank or a line comment.
 */
const directiveLine = (diagnostic: ts.Diagnostic): number | undefined => {
	const file = diagnostic.file;
	if (file === undefined || diagnostic.start === undefined) return undefined;
	const lines = file.text.split('\n');
	let line = file.getLineAndCharacterOfPosition(diagnostic.start).line - 1;
	while (line >= 0) {
		const text = lines[line]?.trim() ?? '';
		if (text.includes(switchedOff)) return line;
		if (text !== '' && !text.startsWith('//')) return undefined;
		line -= 1;
	}
	return undefined;
};

const key = (file: string, line: number): string => `${file}:${line}`;

describe('type diagnostics', () => {
	const found = new Map<string, Array<string>>();
	const stray: Array<string> = [];

	beforeAll(() => {
		const files = new Map(
			fixtures.map((file) => [
				file,
				textOf(file).replaceAll('@ts-expect-error', switchedOff),
			]),
		);
		const { diagnostics } = compile(fixtures, files);
		for (const diagnostic of diagnostics) {
			const message = `TS${diagnostic.code}: ${normalised(
				ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'),
			)}`;
			const line = directiveLine(diagnostic);
			const file = diagnostic.file?.fileName;
			if (file === undefined || line === undefined) {
				stray.push(message);
				continue;
			}
			const at = key(file, line);
			found.set(at, [...(found.get(at) ?? []), message]);
		}
	}, 120_000);

	it('names every negative case once', () => {
		const unnamed = cases
			.filter((item) => item.name === undefined)
			.map(
				(item) =>
					`${relative(packageRoot, item.file)}:${item.line + 1}`,
			);
		expect(unnamed).toEqual([]);
		const names = cases.map((item) => item.name);
		expect(names.filter((name, at) => names.indexOf(name) !== at)).toEqual(
			[],
		);
	});

	it('reports nothing outside the negative cases', () => {
		expect(stray).toEqual([]);
	});

	it.each(
		cases.map((item) => [item.name ?? '(unnamed)', item] as const),
	)('%s', (_, item) => {
		const messages = found.get(key(item.file, item.line)) ?? [];
		expect(messages.length).toBeGreaterThan(0);
		expect(messages).toMatchSnapshot();
	});
});
