import { AnalyzeError, lex, type Token } from '../internal/surql-lexer.js';

export type ParameterToken = Pick<Token, 'value' | 'start' | 'end'>;

/** Reads the variable names and their source spans without changing the text. */
export const parameterTokens = (
	sql: string,
): ReadonlyArray<ParameterToken> | undefined => {
	try {
		const { tokens } = lex(sql);
		// Inline JavaScript functions have their own grammar inside the braces.
		if (
			tokens.some(
				(token, index) =>
					token.kind === 'word' &&
					token.value.toUpperCase() === 'FUNCTION' &&
					tokens[index + 1]?.value === '(',
			)
		)
			return undefined;
		return tokens.filter((token) => token.kind === 'param');
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

/** Renames variables outside strings, comments and quoted identifiers. */
export const renameNames = (
	sql: string,
	names: Readonly<Record<string, string>>,
): string | undefined => {
	const renames = new Map(Object.entries(names));
	if (renames.size === 0) return sql;
	const tokens = parameterTokens(sql);
	return tokens === undefined
		? undefined
		: renameParameterTokens(sql, tokens, renames);
};
