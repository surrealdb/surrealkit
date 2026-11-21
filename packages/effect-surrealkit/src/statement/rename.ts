import { AnalyzeError, lex, type Token } from '../internal/surql-lexer.js';

export type ParameterToken = Pick<Token, 'value' | 'start' | 'end'>;

interface ParameterScan {
	readonly tokens: ReadonlyArray<ParameterToken>;
	readonly recordNames: ReadonlySet<string>;
}

const possibleParameter = /[$\\]/u;

/** Reads rewritable variable spans and read-only names in record literals. */
export const scanParameters = (sql: string): ParameterScan | undefined => {
	try {
		const { tokens } = lex(sql);
		const writable: ParameterToken[] = [];
		const recordNames = new Set<string>();
		const records: Array<ReadonlyArray<Token>> = [];
		for (
			let source: ReadonlyArray<Token> | undefined = tokens;
			source !== undefined;
			source = records.pop()
		) {
			const insideRecord = source !== tokens;
			for (const [index, token] of source.entries()) {
				// Inline JavaScript functions have their own grammar inside the braces.
				if (
					token.kind === 'word' &&
					token.value.toUpperCase() === 'FUNCTION' &&
					source[index + 1]?.value === '('
				)
					return undefined;
				if (token.kind === 'param') {
					if (insideRecord) recordNames.add(token.value);
					else writable.push(token);
				}
				// Slash classification is heuristic. In a record key, a division
				// expression mistaken for a regex could hide a variable reference.
				if (
					insideRecord &&
					token.kind === 'regex' &&
					possibleParameter.test(token.value)
				) {
					return undefined;
				}
				if (
					token.kind === 'string' &&
					token.prefix === 'r' &&
					possibleParameter.test(token.value)
				) {
					// The engines differ on non-LF line endings in comments.
					if (
						/--|\/\/|#/u.test(token.value) &&
						/\r(?!\n)|[\u0085\u2028\u2029]/u.test(token.value)
					) {
						return undefined;
					}
					// The engine parses the decoded body as SurrealQL. Its spans
					// refer to the body, so none enter the rewrite list.
					records.push(lex(token.value).tokens);
				}
			}
		}
		return { tokens: writable, recordNames };
	} catch (error) {
		if (error instanceof AnalyzeError) return undefined;
		throw error;
	}
};

/** Rewrites previously located variable tokens, copying all other bytes. */
export const renameParameterTokens = (
	sql: string,
	tokens: ReadonlyArray<ParameterToken>,
	names: ReadonlyMap<string, string>,
): string => {
	let result = '';
	let copied = 0;
	for (const { value, start, end } of tokens) {
		const name = names.get(value);
		if (name === undefined || name === value) continue;
		result += `${sql.slice(copied, start)}$${name}`;
		copied = end;
	}
	return result + sql.slice(copied);
};

/** Renames variables outside strings, comments and quoted identifiers when safe. */
export const renameNames = (
	sql: string,
	names: Readonly<Record<string, string>>,
): string | undefined => {
	const renames = new Map(
		Object.entries(names).filter(([name, alias]) => name !== alias),
	);
	if (renames.size === 0) return sql;
	const scan = scanParameters(sql);
	if (scan === undefined) return undefined;
	for (const [name, alias] of renames) {
		if (scan.recordNames.has(name) || scan.recordNames.has(alias))
			return undefined;
	}
	return renameParameterTokens(sql, scan.tokens, renames);
};
