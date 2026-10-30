/**
 * semanticDigest (schema document v2, section 13): SHA-256 over the RFC 8785
 * canonical JSON of the document's semantic projection.
 *
 * The digest itself needs a SHA-256 implementation, which the caller passes
 * in (`node:crypto` in the CLI), so this module stays host-neutral.
 */

/**
 * RFC 8785 canonical JSON for the value shapes a schema document contains:
 * objects (keys sorted by UTF-16 code units, which equals byte order for the
 * document's ASCII keys), arrays, strings, booleans, null and integers.
 *
 * @throws TypeError for a non-integer number, `-0`, or any other value.
 */
export const canonicalJson = (value: unknown): string => {
	if (value === null) return 'null';
	switch (typeof value) {
		case 'boolean':
			return value ? 'true' : 'false';
		case 'string':
			return JSON.stringify(value);
		case 'number':
			if (!Number.isInteger(value) || Object.is(value, -0)) {
				throw new TypeError(
					`schema documents contain only integers; found ${value}`,
				);
			}
			return String(value);
		case 'object': {
			if (Array.isArray(value)) {
				return `[${value.map(canonicalJson).join(',')}]`;
			}
			const record = value as Record<string, unknown>;
			const keys = Object.keys(record).sort();
			return `{${keys
				.filter((key) => record[key] !== undefined)
				.map(
					(key) =>
						`${JSON.stringify(key)}:${canonicalJson(record[key])}`,
				)
				.join(',')}}`;
		}
		default:
			throw new TypeError(`cannot canonicalise a ${typeof value}`);
	}
};

const omit = (
	value: Record<string, unknown>,
	keys: ReadonlyArray<string>,
): Record<string, unknown> =>
	Object.fromEntries(
		Object.entries(value).filter(([key]) => !keys.includes(key)),
	);

/**
 * The semantic projection: removes `semanticDigest`, `provenance`,
 * `diagnostics`, every table and field `define`, and the derived
 * `value.readsInput` and `assert.recognized`.
 */
export const semanticProjection = (
	document: Record<string, unknown>,
): Record<string, unknown> => {
	const projected = omit(document, [
		'semanticDigest',
		'provenance',
		'diagnostics',
	]);
	const tables = projected.tables;
	if (!Array.isArray(tables)) return projected;
	return {
		...projected,
		tables: tables.map((table: Record<string, unknown>) => {
			const fields = Array.isArray(table.fields) ? table.fields : [];
			return {
				...omit(table, ['define']),
				fields: fields.map((field: Record<string, unknown>) => {
					const next = omit(field, ['define']);
					if (
						typeof field.value === 'object' &&
						field.value !== null
					) {
						next.value = omit(
							field.value as Record<string, unknown>,
							['readsInput'],
						);
					}
					if (
						typeof field.assert === 'object' &&
						field.assert !== null
					) {
						next.assert = omit(
							field.assert as Record<string, unknown>,
							['recognized'],
						);
					}
					return next;
				}),
			};
		}),
	};
};

/** `sha256:<hex>` of the canonical semantic projection. */
export const semanticDigest = (
	document: Record<string, unknown>,
	sha256Hex: (text: string) => string,
): string => `sha256:${sha256Hex(canonicalJson(semanticProjection(document)))}`;
