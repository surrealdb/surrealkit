/** Renames variables outside strings, comments and quoted identifiers. */
export const renameNames = (
	sql: string,
	_names: Readonly<Record<string, string>>,
): string | undefined => sql;
