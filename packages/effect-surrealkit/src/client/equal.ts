/**
 * Whether two encoded binding values are certainly one value, for the
 * statements of a transaction that bind the same name.
 *
 * The comparison is structural and knows the kinds a binding can carry:
 * primitives (each only with its own kind, so `1` is not `1n` and `0` is not
 * `-0`), arrays, sets (in any order), byte arrays (`Uint8Array` and
 * `ArrayBuffer`, each only with its own kind), plain objects (key order
 * irrelevant, a missing key not the same as a NONE one) and the SDK's value
 * classes, by class and value. Anything else, and anything it cannot finish
 * comparing (a cycle, a throwing getter, a structure too deep), is not the
 * same: a caller is told to rename the binding instead.
 *
 * @internal
 */
import {
	BoundExcluded,
	BoundIncluded,
	DateTime,
	Decimal,
	Duration,
	FileRef,
	Future,
	Geometry,
	Range,
	RecordId,
	RecordIdRange,
	StringRecordId,
	Table,
	Uuid,
} from 'surrealdb';

type Path = Set<object>;

const sameBytes = (a: Uint8Array, b: Uint8Array): boolean =>
	a.byteLength === b.byteLength &&
	a.every((byte, index) => byte === b[index]);

const isPlain = (value: object): boolean => {
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
};

const sameBound = (a: unknown, b: unknown, path: Path): boolean => {
	if (a === undefined || b === undefined) return a === b;
	if (a instanceof BoundIncluded) {
		return b instanceof BoundIncluded && same(a.value, b.value, path);
	}
	if (a instanceof BoundExcluded) {
		return b instanceof BoundExcluded && same(a.value, b.value, path);
	}
	return false;
};

/** The SDK's value classes: `undefined` when `a` is not one. */
const sameSdkValue = (
	a: object,
	b: object,
	path: Path,
): boolean | undefined => {
	if (a instanceof RecordId) {
		return (
			b instanceof RecordId &&
			a.table.name === b.table.name &&
			same(a.id, b.id, path)
		);
	}
	if (a instanceof RecordIdRange) {
		return (
			b instanceof RecordIdRange &&
			a.table.name === b.table.name &&
			sameBound(a.begin, b.begin, path) &&
			sameBound(a.end, b.end, path)
		);
	}
	if (a instanceof Range) {
		return (
			b instanceof Range &&
			sameBound(a.begin, b.begin, path) &&
			sameBound(a.end, b.end, path)
		);
	}
	// `Decimal.equals` holds for `1.0` and `1.00`; the text tells them apart.
	if (a instanceof Decimal) {
		return b instanceof Decimal && a.toString() === b.toString();
	}
	const leaves = [
		DateTime,
		Duration,
		Uuid,
		Table,
		StringRecordId,
		FileRef,
		Future,
		Geometry,
	] as const;
	for (const leaf of leaves) {
		if (a instanceof leaf) {
			return (
				b instanceof leaf &&
				(a as { equals(other: unknown): boolean }).equals(b)
			);
		}
	}
	return undefined;
};

const sameObject = (a: object, b: object, path: Path): boolean => {
	if (Array.isArray(a)) {
		return (
			Array.isArray(b) &&
			Object.getPrototypeOf(a) === Array.prototype &&
			Object.getPrototypeOf(b) === Array.prototype &&
			a.length === b.length &&
			a.every((item, index) => same(item, b[index], path))
		);
	}
	if (a instanceof Uint8Array) {
		return (
			b instanceof Uint8Array &&
			Object.getPrototypeOf(a) === Uint8Array.prototype &&
			Object.getPrototypeOf(b) === Uint8Array.prototype &&
			sameBytes(a, b)
		);
	}
	if (a instanceof ArrayBuffer) {
		return (
			b instanceof ArrayBuffer &&
			sameBytes(new Uint8Array(a), new Uint8Array(b))
		);
	}
	if (a instanceof Set) {
		if (!(b instanceof Set) || a.size !== b.size) return false;
		if (Object.getPrototypeOf(a) !== Set.prototype) return false;
		if (Object.getPrototypeOf(b) !== Set.prototype) return false;
		const unmatched = [...b];
		for (const item of a) {
			const at = unmatched.findIndex((other) => same(item, other, path));
			if (at < 0) return false;
			unmatched.splice(at, 1);
		}
		return true;
	}
	const sdk = sameSdkValue(a, b, path);
	if (sdk !== undefined) return sdk;
	if (isPlain(a) && isPlain(b)) {
		if (
			Object.getOwnPropertySymbols(a).length > 0 ||
			Object.getOwnPropertySymbols(b).length > 0
		) {
			return false;
		}
		const keys = Object.keys(a);
		const other = Object.keys(b);
		if (keys.length !== other.length) return false;
		const record = a as Record<string, unknown>;
		const that = b as Record<string, unknown>;
		return keys.every(
			(key) =>
				Object.hasOwn(that, key) && same(record[key], that[key], path),
		);
	}
	return false;
};

const same = (a: unknown, b: unknown, path: Path): boolean => {
	if (typeof a !== typeof b) return false;
	if (typeof a === 'function' || typeof a === 'symbol') return false;
	if (typeof a !== 'object' || a === null || b === null) {
		return Object.is(a, b);
	}
	if (a === b) return true;
	const other = b as object;
	// A cycle cannot be compared to the end.
	if (path.has(a) || path.has(other)) return false;
	path.add(a);
	path.add(other);
	try {
		return sameObject(a, other, path);
	} finally {
		path.delete(a);
		path.delete(other);
	}
};

/** True only when `a` and `b` are certainly the same value. Never throws. */
export const sameValue = (a: unknown, b: unknown): boolean => {
	try {
		return same(a, b, new Set());
	} catch {
		// A throwing getter or proxy, or a structure too deep to walk.
		return false;
	}
};
