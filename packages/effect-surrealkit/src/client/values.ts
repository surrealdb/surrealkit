/**
 * Whether an encoded binding value is one the SDK sends without loss: the
 * package's `SurrealValue` shape (plus `undefined` for NONE, `ArrayBuffer`
 * for bytes and the SDK's record id and range values), with well-formed
 * strings and integers the SDK can encode.
 *
 * The SDK's CBOR encoder accepts more than it can represent: a class
 * instance becomes its own enumerable fields, a `Map` a document whatever
 * its keys, a symbol or function an empty document, a lone surrogate
 * U+FFFD, a symbol-keyed field nothing. This check refuses those before
 * anything is sent.
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
	Geometry,
	Range,
	RecordId,
	RecordIdRange,
	StringRecordId,
	Table,
	Uuid,
} from 'surrealdb';

/** What is wrong with a value, and where inside it. */
export interface ValueProblem {
	readonly path: ReadonlyArray<string | number>;
	readonly problem: string;
}

/** The largest integer magnitude the SDK encodes as a `number`. */
const MAX_NUMBER = 2 ** 53;
const MAX_BIGINT = 2n ** 64n;

const wellFormed = (text: string): boolean => {
	for (let i = 0; i < text.length; i++) {
		const unit = text.charCodeAt(i);
		if (unit >= 0xd800 && unit <= 0xdbff) {
			const next = text.charCodeAt(i + 1);
			if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
			i++;
		} else if (unit >= 0xdc00 && unit <= 0xdfff) {
			return false;
		}
	}
	return true;
};

/** SDK values that encode as themselves and hold nothing to check. */
const leaves = [
	DateTime,
	Duration,
	Decimal,
	Uuid,
	Table,
	FileRef,
	StringRecordId,
] as const;

type Path = Array<string | number>;

const fail = (path: Path, problem: string): ValueProblem => ({
	path: [...path],
	problem,
});

const checkBound = (
	bound: unknown,
	path: Path,
	seen: Set<object>,
): ValueProblem | undefined => {
	if (bound === undefined) return undefined;
	if (bound instanceof BoundIncluded || bound instanceof BoundExcluded) {
		return check(bound.value, path, seen);
	}
	return fail(
		path,
		'a range bound that is not BoundIncluded or BoundExcluded',
	);
};

const checkObject = (
	value: object,
	path: Path,
	seen: Set<object>,
): ValueProblem | undefined => {
	if (Array.isArray(value)) {
		for (let index = 0; index < value.length; index++) {
			path.push(index);
			const problem = check(value[index], path, seen);
			path.pop();
			if (problem !== undefined) return problem;
		}
		return undefined;
	}
	if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
		return undefined;
	}
	if (ArrayBuffer.isView(value)) {
		return fail(
			path,
			'an array buffer view, which the SDK sends as its raw bytes; send a Uint8Array',
		);
	}
	if (value instanceof Set) {
		let index = 0;
		for (const item of value) {
			path.push(index);
			const problem = check(item, path, seen);
			path.pop();
			if (problem !== undefined) return problem;
			index++;
		}
		return undefined;
	}
	if (value instanceof Map) {
		return fail(
			path,
			'a Map, which the SDK sends as a document whatever its keys; send a plain object',
		);
	}
	if (value instanceof Date) {
		return Number.isNaN(value.getTime())
			? fail(path, 'an invalid Date')
			: undefined;
	}
	if (value instanceof Geometry) {
		// The SDK encodes each coordinate as a number, including coordinates
		// inside a GeometryCollection.
		return check(value.toJSON(), path, seen);
	}
	if (value instanceof RecordId) {
		path.push('id');
		const problem = check(value.id, path, seen);
		path.pop();
		return problem;
	}
	if (value instanceof RecordIdRange || value instanceof Range) {
		path.push('begin');
		const begin = checkBound(value.begin, path, seen);
		path.pop();
		if (begin !== undefined) return begin;
		path.push('end');
		const end = checkBound(value.end, path, seen);
		path.pop();
		return end;
	}
	for (const leaf of leaves) {
		if (value instanceof leaf) return undefined;
	}
	const proto = Object.getPrototypeOf(value);
	if (proto !== Object.prototype && proto !== null) {
		return fail(
			path,
			'a non-plain object, which the SDK would send as its own enumerable fields; encode it to a SurrealDB value',
		);
	}
	if (Object.getOwnPropertySymbols(value).length > 0) {
		return fail(path, 'an object with symbol keys, which the SDK drops');
	}
	const record = value as Record<string, unknown>;
	for (const key of Object.keys(record)) {
		path.push(key);
		const problem = wellFormed(key)
			? check(record[key], path, seen)
			: fail(
					path,
					'a key with a lone UTF-16 surrogate, which UTF-8 cannot carry',
				);
		path.pop();
		if (problem !== undefined) return problem;
	}
	return undefined;
};

const check = (
	value: unknown,
	path: Path,
	seen: Set<object>,
): ValueProblem | undefined => {
	switch (typeof value) {
		case 'undefined':
		case 'boolean':
			return undefined;
		case 'string':
			return wellFormed(value)
				? undefined
				: fail(
						path,
						'a string with a lone UTF-16 surrogate, which UTF-8 cannot carry; the SDK would replace it with U+FFFD',
					);
		case 'number':
			if (Object.is(value, -0)) {
				return fail(
					path,
					'negative zero, which the SDK encodes as positive zero',
				);
			}
			return Number.isInteger(value) && Math.abs(value) > MAX_NUMBER
				? fail(
						path,
						'an integer beyond ±2^53, which the SDK cannot encode as a number; send a bigint',
					)
				: undefined;
		case 'bigint':
			return value >= MAX_BIGINT || value < -MAX_BIGINT
				? fail(
						path,
						'a bigint beyond 64 bits, which the SDK cannot encode',
					)
				: undefined;
		case 'symbol':
		case 'function':
			return fail(
				path,
				`a ${typeof value}, which is not a SurrealDB value`,
			);
		default: {
			if (value === null) return undefined;
			const object = value as object;
			if (seen.has(object)) {
				return fail(path, 'a cycle, which cannot be sent');
			}
			seen.add(object);
			try {
				return checkObject(object, path, seen);
			} finally {
				seen.delete(object);
			}
		}
	}
};

/**
 * The first problem with `value`, or `undefined` when the SDK sends it
 * without loss. Never throws: a value it cannot finish reading (a throwing
 * getter, a proxy, a structure too deep) is a problem.
 */
export const valueProblem = (value: unknown): ValueProblem | undefined => {
	const path: Path = [];
	try {
		return check(value, path, new Set());
	} catch {
		return fail(path, 'a value that cannot be read to the end');
	}
};

/** `$name[*][0]`: a display path that does not expose keys from the data. */
export const pathText = (
	binding: string,
	path: ReadonlyArray<string | number>,
): string =>
	`$${binding}${path
		.map((step) => (typeof step === 'number' ? `[${step}]` : '[*]'))
		.join('')}`;
