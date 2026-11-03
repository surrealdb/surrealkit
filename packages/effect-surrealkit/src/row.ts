/**
 * NONE versus NULL versus absent.
 *
 * The SDK decodes SurrealQL NONE as `undefined`, so an object coming back
 * from a query can carry a key whose value is `undefined` where the stored
 * record simply has no value. Exact `optionalKey` schemas reject such keys.
 * `NoneAsAbsent` removes them first; `null` is kept, because NULL is a value.
 */
import type { Schema } from 'effect';

import { runtimeCode } from './internal/code.js';
import { isPlainObject, wrap } from './internal/declare.js';

/**
 * Removes properties whose value is `undefined` from plain objects, at every
 * depth, including inside arrays and sets. Arrays keep their `undefined`
 * elements (an `array<option<T>>` element is positional). Class instances
 * (SDK values) are left untouched.
 */
export const stripNone = (value: unknown): unknown => {
	if (value instanceof Set) {
		let changed = false;
		const next = new Set<unknown>();
		for (const element of value) {
			const stripped = stripNone(element);
			if (stripped !== element) changed = true;
			next.add(stripped);
		}
		return changed ? next : value;
	}
	if (Array.isArray(value)) {
		let changed = false;
		const next = value.map((element) => {
			const stripped = stripNone(element);
			if (stripped !== element) changed = true;
			return stripped;
		});
		return changed ? next : value;
	}
	if (!isPlainObject(value)) return value;
	let changed = false;
	const next: Record<string, unknown> = {};
	for (const [key, element] of Object.entries(value)) {
		if (element === undefined) {
			changed = true;
			continue;
		}
		const stripped = stripNone(element);
		if (stripped !== element) changed = true;
		next[key] = stripped;
	}
	return changed ? next : value;
};

/**
 * Decodes through `schema` after {@link stripNone}, so a NONE that the SDK
 * surfaced as an `undefined` property reads as an absent key. Encoding is
 * exactly the wrapped schema's encoding: an `undefined` property is kept, so
 * a `Merge` value still sends NONE to remove a field.
 */
export const NoneAsAbsent = <S extends Schema.Top>(schema: S) =>
	wrap(schema, {
		id: 'effect-surrealkit/NoneAsAbsent',
		expected: 'a SurrealDB row',
		preprocess: stripNone,
		code: ([inner]) =>
			runtimeCode(
				`NoneAsAbsent(${inner?.runtime ?? ''})`,
				inner?.Type ?? 'unknown',
			),
	});
