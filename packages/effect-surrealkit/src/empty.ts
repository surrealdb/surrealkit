/**
 * The exactly empty object.
 *
 * In Effect 4 `Schema.Struct({})` declares no keys and so checks only that
 * the value is not `null` or `undefined`: it accepts any object, string,
 * number or array and passes it through unchanged, even under
 * `onExcessProperty: "error"`. SurrealDB is stricter: a `{}` literal kind,
 * a closed object with no declared fields and the content of a schemafull
 * table with no writable fields all accept exactly `{}`. {@link EmptyObject}
 * is that shape.
 */
import { Schema, type SchemaAST } from 'effect';

import { RUNTIME_IMPORT, SDK_TYPE_IMPORT } from './internal/code.js';
import { isPlainObject } from './internal/declare.js';

/**
 * A check that the value is a plain object without own enumerable keys:
 * `{}` or `Object.create(null)`, not an array or a class instance. Its JSON
 * Schema is `{ "type": "object", "additionalProperties": false }`.
 */
export const isEmptyObject = (
	annotations?: Schema.Annotations.Filter,
): SchemaAST.Filter<object> =>
	Schema.makeFilter(
		(value: object) =>
			isPlainObject(value) && Object.keys(value).length === 0,
		{
			expected: 'an empty object',
			representation: {
				id: 'effect-surrealkit/isEmptyObject',
				payload: null,
			},
			toJsonSchema: () => ({
				type: 'object',
				additionalProperties: false,
			}),
			toCode: () => ({
				runtime: 'SurrealSchema.isEmptyObject()',
				importDeclarations: [RUNTIME_IMPORT, SDK_TYPE_IMPORT],
			}),
			...annotations,
		},
	);

/**
 * Exactly `{}`: a `Schema.Struct({})` that also refuses every key, every
 * array and every value that is not a plain object, in both directions.
 *
 * Generated modules print it wherever an object has no fields: the create,
 * merge and content inputs of a schemafull table with no writable fields, a
 * closed nested object with no declared fields, and the `{}` literal kind.
 * It stays a `Schema.Struct`, so a table descriptor's operation schemas keep
 * their `fields` (none) and its TypeScript type stays `{}`: a caller that
 * passes `{}` is unaffected, and a stray key fails at run time instead of
 * reaching SurrealDB. The base of an open row (`Schema.StructWithRest` over a
 * record) stays a plain `Schema.Struct({})`, because there the rest holds the
 * keys.
 */
export const EmptyObject = Schema.Struct({}).check(isEmptyObject());
