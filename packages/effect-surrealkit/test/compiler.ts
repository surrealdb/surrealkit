/**
 * The TypeScript compiler run in process over the type fixtures: what the
 * diagnostics and type-check budget tests share. Files can be replaced or
 * added in memory, so nothing is written next to the fixtures.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import ts from 'typescript';

export const packageRoot = join(import.meta.dirname, '..');
export const typesRoot = join(packageRoot, 'test', 'types');

const typesOptions = (): ts.CompilerOptions => {
	const configPath = join(typesRoot, 'tsconfig.json');
	const config = ts.readConfigFile(configPath, ts.sys.readFile);
	if (config.error !== undefined) {
		throw new Error(
			ts.flattenDiagnosticMessageText(config.error.messageText, '\n'),
		);
	}
	return ts.parseJsonConfigFileContent(config.config, ts.sys, typesRoot)
		.options;
};

export interface Compiled {
	readonly program: ts.Program;
	/** Every diagnostic of the program, in file order. */
	readonly diagnostics: ReadonlyArray<ts.Diagnostic>;
	/** The checker's deterministic counters, as `--extendedDiagnostics`. */
	readonly types: number;
	readonly instantiations: number;
}

/**
 * Type-checks `rootNames` with the options of `test/types/tsconfig.json`.
 * `files` maps an absolute path to text that replaces (or adds) that file.
 */
export const compile = (
	rootNames: ReadonlyArray<string>,
	files: ReadonlyMap<string, string> = new Map(),
): Compiled => {
	const options = typesOptions();
	const host = ts.createCompilerHost(options, true);
	const { fileExists, getSourceFile, readFile } = host;
	host.fileExists = (name) => files.has(name) || fileExists.call(host, name);
	host.readFile = (name) => files.get(name) ?? readFile.call(host, name);
	host.getSourceFile = (name, language, onError, create) => {
		const text = files.get(name);
		return text === undefined
			? getSourceFile.call(host, name, language, onError, create)
			: ts.createSourceFile(name, text, language, true);
	};
	const program = ts.createProgram({ rootNames, options, host });
	const diagnostics = ts.getPreEmitDiagnostics(program);
	return {
		program,
		diagnostics,
		types: program.getTypeCount(),
		instantiations: program.getInstantiationCount(),
	};
};

/** A file's text as it is on disk. */
export const textOf = (path: string): string => readFileSync(path, 'utf8');
