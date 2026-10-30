/** `uuid`, `bytes`, and kinds with no SDK value representation. */
import { Schema, SchemaGetter } from 'effect';
import { Uuid as SdkUuid } from 'surrealdb';

import { quote, runtimeCode } from './internal/code.js';
import {
	constrained,
	invalid,
	leaf,
	ok,
	wrongType,
} from './internal/declare.js';

const byte = Schema.Number.check(
	Schema.isInt(),
	Schema.isBetween({ minimum: 0, maximum: 255 }),
);

const LONE_SURROGATE =
	/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/** Replaces lone surrogates with U+FFFD, as UTF-8 transport would. */
export const toWellFormed = (text: string): string =>
	text.replace(new RegExp(LONE_SURROGATE.source, 'g'), '�');

/**
 * `string`: a JS string SurrealDB can store. SurrealDB strings are UTF-8, so
 * a JS string containing a lone surrogate cannot be represented: the SDK's
 * CBOR encoder would silently replace it with U+FFFD. Such strings fail here
 * instead.
 */
export const SurrealString = leaf<string>({
	id: 'effect-surrealkit/String',
	expected: 'a string',
	code: runtimeCode('String', 'string'),
	parse: (input) => {
		if (typeof input !== 'string') return wrongType;
		return LONE_SURROGATE.test(input)
			? invalid(
					'string contains a lone UTF-16 surrogate, which SurrealDB (UTF-8) cannot store; it would be replaced with U+FFFD',
				)
			: ok(input);
	},
	arbitrary: (constraint) =>
		Schema.link<string>()(constrained(Schema.String, constraint), {
			decode: SchemaGetter.transform(toWellFormed),
			encode: SchemaGetter.transform((text) => text),
		}),
});

/** `uuid`: the SDK `Uuid` class (any version). */
export const Uuid = leaf<SdkUuid>({
	id: 'effect-surrealkit/Uuid',
	expected: 'a SurrealDB Uuid',
	code: runtimeCode('Uuid', 'Surreal.Uuid'),
	parse: (input) => (input instanceof SdkUuid ? ok(input) : wrongType),
	equivalence: (self, that) => self.equals(that),
	arbitrary: () =>
		Schema.link<SdkUuid>()(
			Schema.Array(byte).check(Schema.isBetweenLength(16, 16)),
			{
				decode: SchemaGetter.transform(
					(bytes) => new SdkUuid(Uint8Array.from(bytes)),
				),
				encode: SchemaGetter.transform((uuid) => [
					...uuid.toUint8Array(),
				]),
			},
		),
});

const sameBytes = (self: Uint8Array, that: Uint8Array): boolean =>
	self.length === that.length && self.every((b, i) => b === that[i]);

/**
 * `bytes`: a `Uint8Array`. The SDK decodes bytes as an `ArrayBuffer`, which
 * is accepted and copied into a `Uint8Array`; encoding sends the
 * `Uint8Array`, which the SDK writes as CBOR bytes.
 */
export const Bytes = leaf<Uint8Array, ArrayBuffer | Uint8Array>({
	id: 'effect-surrealkit/Bytes',
	expected: 'bytes (Uint8Array or ArrayBuffer)',
	code: runtimeCode('Bytes', 'Uint8Array'),
	parse: (input) => {
		if (input instanceof Uint8Array) return ok(input);
		if (input instanceof ArrayBuffer) return ok(new Uint8Array(input));
		return wrongType;
	},
	equivalence: sameBytes,
	arbitrary: () =>
		Schema.link<Uint8Array>()(
			Schema.Array(byte).check(Schema.isMaxLength(64)),
			{
				decode: SchemaGetter.transform((bytes) =>
					Uint8Array.from(bytes),
				),
				encode: SchemaGetter.transform((bytes) => [...bytes]),
			},
		),
});

const unsupportedReason: Readonly<Record<string, string>> = {
	regex: 'regex values have no CBOR transport in SurrealDB 3.x; project the field to a string (`<string> field`) and use a string schema',
	function:
		'closures are not storable or transportable values; call functions through their signatures instead',
};

/**
 * A kind no SDK value can represent (`regex`, `function`). Always fails, with
 * the reason, in both directions.
 */
export const Unsupported = <const K extends string>(kind: K) =>
	leaf<never>({
		id: 'effect-surrealkit/Unsupported',
		payload: kind,
		expected: `a ${kind} value (unsupported)`,
		code: runtimeCode(`Unsupported(${quote(kind)})`, 'never'),
		parse: () =>
			invalid(
				unsupportedReason[kind] ??
					`the SurrealDB kind \`${kind}\` has no SDK value representation`,
			),
	});
