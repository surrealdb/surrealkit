/**
 * A generated part's doc comment, what an editor's hover shows: the
 * comment that opens the authored file as prose, the SurrealQL in a fenced
 * block, then the file name. Whatever the text holds, the module compiles.
 */
import { join } from 'node:path';

import { Arbitrary, Effect, Schema } from 'effect';
import ts from 'typescript';
import { describe, expect, it } from 'vitest';

import {
	analyzeSync,
	renderStatementModulesSync,
} from '../src/analyze/index.js';
import { goldenDir } from './documents.js';

const comment = (sql: string): string => analyzeSync(sql).comment;

describe('the comment that opens a text', () => {
	it('is empty when the text opens with a statement', () => {
		expect(comment('RETURN 1;')).toBe('');
		expect(comment('RETURN 1; -- after the statement')).toBe('');
		expect(comment('owner = $owner -- a fragment')).toBe('');
	});

	it('reads each line comment without its marker', () => {
		expect(comment('-- one\n-- two\nRETURN 1;')).toBe('one\ntwo');
		expect(comment('# one\n// two\n--three\nRETURN 1;')).toBe(
			'one\ntwo\nthree',
		);
	});

	it('reads a block comment without its delimiters or margin', () => {
		expect(comment('/* one */ RETURN 1;')).toBe('one');
		expect(comment('/**\n * one\n * two\n */\nRETURN 1;')).toBe('one\ntwo');
		expect(comment('/*\n\tone\n\t\ttwo\n*/\nRETURN 1;')).toBe('one\n\ttwo');
	});

	it('keeps a blank line between paragraphs, and nothing around them', () => {
		expect(comment('\n\n-- one\n\n-- two\n--\n-- three\n\nRETURN 1;')).toBe(
			'one\n\ntwo\n\nthree',
		);
		expect(comment('--\n-- one\n--\nRETURN 1;')).toBe('one');
	});

	it('stops at the first token, outside strings', () => {
		expect(comment("-- one\nRETURN '-- two';\n-- three\n")).toBe('one');
	});
});

const documentation = (file: string, name: string): string => {
	const service = ts.createLanguageService({
		getCompilationSettings: () => ({}),
		getScriptFileNames: () => [file],
		getScriptVersion: () => '1',
		getScriptSnapshot: (path) => {
			const text = ts.sys.readFile(path);
			return text === undefined
				? undefined
				: ts.ScriptSnapshot.fromString(text);
		},
		getCurrentDirectory: () => goldenDir('statements'),
		getDefaultLibFileName: (options) => ts.getDefaultLibFilePath(options),
		fileExists: ts.sys.fileExists,
		readFile: ts.sys.readFile,
	});
	const text = ts.sys.readFile(file) ?? '';
	const at = text.indexOf(`export const ${name} `) + 'export const '.length;
	return ts.displayPartsToString(
		service.getQuickInfoAtPosition(file, at)?.documentation,
	);
};

describe('the hover on a generated part', () => {
	it("shows the file's comment, its SurrealQL and its name", () => {
		expect(
			documentation(
				join(goldenDir('statements'), 'accounts', 'statements.ts'),
				'bindOwner',
			),
		).toBe(
			[
				'Binds the owner a caller names, and the caller.',
				'',
				'```surql',
				'LET $owner = $boundOwner;',
				'LET $principal = $auth.id;',
				'```',
				'',
				'`accounts/bindOwner.surql`: a part.',
			].join('\n'),
		);
	});

	it('shows the SurrealQL and the name of a file with no comment', () => {
		expect(
			documentation(
				join(goldenDir('statements'), 'counter', 'statements.ts'),
				'step',
			),
		).toMatch(
			/^```surql\nLET \$start = \$boundStart;\n[\s\S]*\nRETURN \$next \* 2;\n```\n\n`counter\/step\.surql`: a part\.$/u,
		);
	});

	it('escapes an `@` that TypeScript would read as a tag', () => {
		expect(
			documentation(
				join(goldenDir('statements'), 'catalog', 'statements.ts'),
				'byWords',
			),
		).toBe(
			[
				'```surql',
				'SELECT id, title FROM listing WHERE __WHERE__ AND title \\@1@ $terms ORDER BY id LIMIT 10',
				'```',
				'',
				'`catalog/byWords.surql`: a template.',
			].join('\n'),
		);
	});

	it('fences the SurrealQL longer than any run of backticks in it', () => {
		const { files } = renderStatementModulesSync({
			scopes: [
				{
					name: 'p',
					files: [{ name: 't', sql: "RETURN '```' + '````';" }],
				},
			],
		});
		expect(files[0]?.content).toContain(
			"/**\n * `````surql\n * RETURN '```' + '````';\n * `````\n *\n * `p/t.surql`: a part.\n */",
		);
	});
});

/** Pieces that could end a doc comment, a fence or a template literal. */
const PIECES = [
	'*/',
	'/*',
	'*',
	'/',
	'`',
	'```',
	'````',
	'${',
	'}',
	'$',
	'\\',
	'@see',
	'\r',
	'\u2028',
	' ',
	'\t',
	'a',
	'1',
] as const;

const pieces = Schema.Array(Schema.Literals(PIECES));

const texts = Arbitrary.schema(
	Schema.Struct({
		line: pieces,
		block: pieces,
		string: pieces,
		ident: pieces,
	}),
);

/** A part that holds each run of pieces where SurrealQL allows it. */
const textOf = (value: {
	readonly line: ReadonlyArray<string>;
	readonly block: ReadonlyArray<string>;
	readonly string: ReadonlyArray<string>;
	readonly ident: ReadonlyArray<string>;
}): string => {
	const line = value.line.join('');
	const block = value.block.join('').replaceAll('*/', '* /');
	const string = value.string
		.join('')
		.replaceAll('\\', '\\\\')
		.replaceAll("'", "\\'");
	const ident = value.ident
		.join('')
		.replaceAll('\\', '\\\\')
		.replaceAll('`', '\\`');
	return `-- ${line}\n/* ${block} */\nSELECT \`x${ident}\` FROM t WHERE a = '${string}'; -- ${line}\n`;
};

describe('a generated module', () => {
	it('compiles, with its doc comment whole, whatever its text holds', async () => {
		const result = await Effect.runPromise(
			Arbitrary.checkEffect(
				texts,
				(value) => {
					const sql = textOf(value);
					const { files } = renderStatementModulesSync({
						scopes: [{ name: 'p', files: [{ name: 't', sql }] }],
					});
					const content = files[0]?.content ?? '';
					const output = ts.transpileModule(content, {
						reportDiagnostics: true,
						compilerOptions: { module: ts.ModuleKind.ESNext },
					});
					const source = ts.createSourceFile(
						'statements.ts',
						content,
						ts.ScriptTarget.ESNext,
						true,
					);
					const statement = source.statements.find(
						ts.isVariableStatement,
					);
					const doc =
						statement === undefined
							? []
							: ts.getJSDocCommentsAndTags(statement);
					const [jsdoc] = doc;
					// One doc comment, on the constant, with no tag read out of
					// the text, ending with the file name.
					return (
						(output.diagnostics ?? []).length === 0 &&
						doc.length === 1 &&
						jsdoc !== undefined &&
						ts.isJSDoc(jsdoc) &&
						jsdoc.tags === undefined &&
						(
							ts.getTextOfJSDocComment(jsdoc.comment) ?? ''
						).endsWith('```\n\n`p/t.surql`: a part.')
					);
				},
				{ runs: 300, seed: 20261123 },
			),
		);
		expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe('Passed');
	});
});
