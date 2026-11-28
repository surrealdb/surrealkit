import { type SchemaAST, SchemaIssue } from 'effect';

const diagnostics = new WeakMap<SchemaIssue.Issue, string>();

/** Registers a diagnostic authored by this package, without attaching data. */
export const staticCodecIssue = (message: string): SchemaIssue.Issue => {
	const issue = new SchemaIssue.InvalidValue({ message });
	diagnostics.set(issue, message);
	return issue;
};

const matches = (
	ast: SchemaAST.AST,
	expected: SchemaAST.AST,
	seen = new Set<SchemaAST.AST>(),
): boolean => {
	if (ast === expected) return true;
	if (seen.has(ast)) return false;
	seen.add(ast);
	return (
		(ast.encoding?.some(({ to }) => matches(to, expected, seen)) ??
			false) ||
		(ast._tag === 'Union' &&
			ast.types.some((member) => matches(member, expected, seen))) ||
		(ast._tag === 'Suspend' && matches(ast.thunk(), expected, seen))
	);
};

const property = (
	ast: SchemaAST.AST,
	child: SchemaIssue.Pointer,
): { readonly path: string; readonly ast: SchemaAST.AST | undefined } => {
	const [key] = child.path;
	if (child.path.length === 1 && ast._tag === 'Objects') {
		const field = ast.propertySignatures.find(({ name }) => name === key);
		if (field !== undefined && typeof field.name === 'string') {
			return { path: `[${JSON.stringify(field.name)}]`, ast: field.type };
		}
	}
	if (
		child.path.length === 1 &&
		ast._tag === 'Arrays' &&
		typeof key === 'number' &&
		Number.isSafeInteger(key) &&
		key >= 0
	) {
		const element = ast.elements[key];
		return element !== undefined
			? { path: `[${key}]`, ast: element }
			: { path: '[]', ast: ast.rest[0] };
	}
	return { path: '', ast: undefined };
};

/** Formats typed codec failures without accessing inputs or custom annotations. */
export const codecIssueText = (
	issue: SchemaIssue.Issue,
	ast: SchemaAST.AST | undefined,
	path = '',
): string => {
	const at = (message: string) =>
		path.length === 0 ? message : `${message}\n  at ${path}`;
	const trusted = diagnostics.get(issue);
	if (trusted !== undefined) return at(trusted);
	switch (issue._tag) {
		case 'Composite':
			if (ast === undefined || !matches(ast, issue.ast))
				return at('Value failed codec validation');
			return issue.issues
				.map((child) => {
					if (child._tag !== 'Pointer')
						return codecIssueText(child, issue.ast, path);
					const next = property(issue.ast, child);
					return codecIssueText(
						child.issue,
						next.ast,
						path + next.path,
					);
				})
				.join('\n');
		case 'AnyOf':
			return ast !== undefined &&
				matches(ast, issue.ast) &&
				issue.issues.length > 0
				? issue.issues
						.map((child) => codecIssueText(child, issue.ast, path))
						.join('\n')
				: at('No codec alternative accepted the value');
		case 'MissingKey':
			return at('Missing required field');
		case 'UnexpectedKey':
			return at('Unexpected field');
		case 'InvalidType':
			return at('Value has an invalid type');
		case 'Encoding':
			return at('Value failed a codec transformation');
		default:
			return at('Value failed codec validation');
	}
};
