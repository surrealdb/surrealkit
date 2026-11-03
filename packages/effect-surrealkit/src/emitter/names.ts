/** Identifiers and file names for generated modules. */

const words = (name: string): string[] =>
	name
		.replace(/([a-z0-9])([A-Z])/g, '$1 $2')
		.split(/[^A-Za-z0-9]+/)
		.filter((word) => word.length > 0);

/** `member_of` → `MemberOf`; a leading digit gets a `T` prefix. */
export const pascalCase = (name: string): string => {
	const joined = words(name)
		.map((word) => word[0]?.toUpperCase() + word.slice(1))
		.join('');
	if (joined.length === 0) return 'T';
	return /^[0-9]/.test(joined) ? `T${joined}` : joined;
};

/** `member_of` → `member-of`. */
export const kebabCase = (name: string): string => {
	const joined = words(name)
		.map((word) => word.toLowerCase())
		.join('-');
	return joined.length === 0 ? 'table' : joined;
};

/** `fn::doc::related` → `fn_doc_related`. */
export const functionIdentifier = (name: string): string =>
	`fn_${name
		.replace(/^fn::/, '')
		.split('::')
		.map((part) => part.replace(/[^A-Za-z0-9_]/g, '_'))
		.join('_')}`;

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;

/**
 * A property key as written in an object literal: bare when possible.
 * `__proto__` is computed, because a plain `__proto__:` (quoted or not)
 * sets the prototype instead of defining a property.
 */
export const propertyKey = (key: string): string => {
	if (key === '__proto__') return '["__proto__"]';
	return IDENTIFIER.test(key) ? key : JSON.stringify(key);
};

/** Property access for a key: `.name` or `["odd.name"]`. */
export const propertyAccess = (key: string): string =>
	IDENTIFIER.test(key) ? `.${key}` : `[${JSON.stringify(key)}]`;
