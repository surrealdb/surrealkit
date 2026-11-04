/**
 * Query identity and value transport.
 *
 * A query's identity is its text plus its parameters, rendered with the SDK's
 * own SurrealQL printer: a `RecordId` prints `r"user:ada"` and the string
 * prints `"user:ada"`, so the two (which match different records) never share
 * a cache entry. Parameter order does not matter, and an `undefined` parameter
 * is the same as an absent one.
 *
 * Results cross the server/client boundary as base64 CBOR, the SDK's own
 * wire encoding, so every SDK value class arrives as itself.
 */

import { CborCodec, toSurqlString } from 'surrealdb';

/** A query's bindings, with `undefined` values removed. */
export function cleanParams(
	params: object | undefined,
): Record<string, unknown> | undefined {
	if (!params) return undefined;
	const out: Record<string, unknown> = {};
	let any = false;
	for (const [name, value] of Object.entries(params)) {
		if (value === undefined) continue;
		out[name] = value;
		any = true;
	}
	return any ? out : undefined;
}

/** The stable identity of `text` bound to `params`. */
export function queryKey(text: string, params: object | undefined): string {
	const clean = cleanParams(params);
	if (!clean) return text;
	const bound = Object.keys(clean)
		.sort()
		.map((name) => `${name}=${render(clean[name])}`)
		.join('&');
	return `${text}\u0000${bound}`;
}

function render(value: unknown): string {
	try {
		return toSurqlString(value);
	} catch {
		return JSON.stringify(value) ?? String(value);
	}
}

/** Encodes a value as base64 CBOR. */
export function encodeValue(value: unknown): string {
	const bytes = CborCodec.DEFAULT.encode(value);
	let binary = '';
	for (let i = 0; i < bytes.length; i += 0x8000) {
		binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	}
	return btoa(binary);
}

/** Decodes base64 CBOR written by {@link encodeValue}. */
export function decodeValue<T>(text: string): T {
	const binary = atob(text);
	const bytes = new Uint8Array(binary.length);
	for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
	return CborCodec.DEFAULT.decode<T>(bytes);
}

/**
 * Whether two decoded values are equal. SDK value classes compare by their
 * SurrealQL text; plain objects and arrays compare member by member.
 */
export function sameValue(a: unknown, b: unknown): boolean {
	if (Object.is(a, b)) return true;
	if (typeof a !== 'object' || typeof b !== 'object' || !a || !b)
		return false;
	if (Object.getPrototypeOf(a) !== Object.getPrototypeOf(b)) return false;
	if (Array.isArray(a)) {
		const other = b as unknown[];
		return (
			a.length === other.length &&
			a.every((item, i) => sameValue(item, other[i]))
		);
	}
	if (a instanceof Date) return a.getTime() === (b as Date).getTime();
	const proto = Object.getPrototypeOf(a);
	if (proto !== Object.prototype && proto !== null) {
		return render(a) === render(b);
	}
	const left = Object.keys(a);
	const right = Object.keys(b);
	return (
		left.length === right.length &&
		left.every((key) =>
			sameValue(
				(a as Record<string, unknown>)[key],
				(b as Record<string, unknown>)[key],
			),
		)
	);
}
