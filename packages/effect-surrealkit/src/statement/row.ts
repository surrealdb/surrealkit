/**
 * Rows as expressions, and the codecs rows come back through.
 *
 * A row object has one expression per field of a generated row schema, built
 * once from the schema's own fields. A nested object field carries its
 * children as properties (`row.address.city`); an optional field, and every
 * field below one, yields a value that may be NONE.
 */
import { Schema } from 'effect';
import { escapeIdent } from 'surrealdb';

import { runtimeCode } from '../internal/code.js';
import { isPlainObject, wrap } from '../internal/declare.js';
import { NoneAsAbsent, stripNone } from '../row.js';
import {
	type CodecOf,
	type Expr,
	makePath,
	type OrNone,
	orNone,
	type Path,
} from './expr.js';
import type { Codec } from './statement.js';

/** A row schema's own fields: codecs, possibly optional keys. */
export type Fields = { readonly [field: string]: Codec };

/** A closed row schema. */
export type StructCodec = Codec & { readonly fields: Fields };

/** An open row schema (a schemaless table, a `FLEXIBLE` object). */
export type OpenStructCodec = Codec & {
	readonly schema: StructCodec;
	readonly records: unknown;
};

/** A row schema as generated modules declare it. */
export type RowCodecIn = StructCodec | OpenStructCodec;

/** The declared fields of a row schema. */
export type FieldsOf<R> = R extends {
	readonly schema: { readonly fields: infer F };
}
	? F
	: R extends { readonly fields: infer F }
		? F
		: never;

/** @internal */
export type Unwrapped<S> = S extends { readonly schema: infer I extends Codec }
	? S extends { readonly '~type.optionality': 'optional' }
		? I
		: S
	: S;

/** @internal */
export type IsOptional<S> = S extends {
	readonly '~type.optionality': 'optional';
}
	? true
	: false;

/** @internal */
export type NestedFields<S> = S extends RowCodecIn ? FieldsOf<S> : never;

/**
 * The expression of one field, a field path, with its children when it is
 * an object.
 */
export type FieldExpr<S, Optional extends boolean> = Path<
	Optional extends true ? OrNone<Unwrapped<S> & Codec> : Unwrapped<S> & Codec
> &
	([NestedFields<Unwrapped<S>>] extends [never]
		? unknown
		: RowOf<NestedFields<Unwrapped<S>>, Optional>);

/** One expression per field of a row schema. */
export type RowOf<F, Optional extends boolean = false> = {
	readonly [K in keyof F & string]: FieldExpr<
		F[K],
		Optional extends true ? true : IsOptional<F[K]>
	>;
};

/** @internal */
export const isOptional = (codec: Codec): boolean =>
	codec.ast.context?.isOptional === true;

/** @internal */
export const unwrap = (codec: Codec): Codec =>
	isOptional(codec) && 'schema' in codec && Schema.isSchema(codec.schema)
		? (codec.schema as Codec)
		: codec;

/** The declared fields of a row schema, or `undefined` for any other codec. */
export const fieldsOf = (codec: Codec): Fields | undefined => {
	if (
		'schema' in codec &&
		'records' in codec &&
		Schema.isSchema(codec.schema) &&
		'fields' in codec.schema
	) {
		return codec.schema.fields as Fields;
	}
	if ('fields' in codec && isPlainObject(codec.fields)) {
		return codec.fields as Fields;
	}
	return undefined;
};

const build = (
	fields: Fields,
	prefix: string,
	optional: boolean,
	root: { readonly root: string; readonly rootSql: string } | undefined,
): Record<string, Expr> => {
	const row: Record<string, Expr> = {};
	for (const [key, declared] of Object.entries(fields)) {
		const mayBeNone = optional || isOptional(declared);
		const codec = unwrap(declared);
		const path = `${prefix}${escapeIdent(key)}`;
		const start = root ?? { root: key, rootSql: path };
		const nested = fieldsOf(codec);
		row[key] = Object.assign(
			nested === undefined
				? {}
				: build(nested, `${path}.`, mayBeNone, start),
			makePath(mayBeNone ? orNone(codec) : codec, path, start),
		);
	}
	return row;
};

/**
 * The row object of a generated row schema: one expression per field, the
 * path quoted where SurrealQL needs it.
 */
export const rowOf = <R extends RowCodecIn>(schema: R): RowOf<FieldsOf<R>> => {
	const fields = fieldsOf(schema);
	if (fields === undefined) {
		throw new TypeError('rowOf takes a generated row schema (a struct)');
	}
	return build(fields, '', false, undefined) as RowOf<FieldsOf<R>>;
};

/**
 * The row object of a row schema read through a parameter the engine sets,
 * `$before.` in a `DELETE … RETURN` or `$input.` in `ON DUPLICATE KEY
 * UPDATE`: every path starts with `prefix`.
 *
 * @internal
 */
export const rowAt = <R extends RowCodecIn>(
	schema: R,
	prefix: string,
): RowOf<FieldsOf<R>> => {
	const fields = fieldsOf(schema);
	if (fields === undefined) {
		throw new TypeError('rowAt takes a generated row schema (a struct)');
	}
	return build(fields, prefix, false, undefined) as RowOf<FieldsOf<R>>;
};

/** The codec stored rows decode with: NONE reads as an absent key. */
export type RowCodec<S extends Codec> = ReturnType<typeof NoneAsAbsent<S>>;

/** @internal */
export const rowCodec = <S extends Codec>(schema: S): RowCodec<S> =>
	NoneAsAbsent<S>(schema);

/** The row a projection yields: one key per alias. */
export type Projection = { readonly [alias: string]: Expr };

/**
 * A projection as its callback returned it, `F`, checked: a value that is
 * not an expression is typed as a message saying so.
 */
export type CheckedProjection<F> = {
	readonly [K in keyof F]: F[K] extends Expr
		? F[K]
		: 'a projected field is an expression: inline a constant with `literal`';
};

/** The struct a projection decodes with. */
export type ProjectedFields<F> = {
	readonly [K in keyof F]: CodecOf<F[K]>;
};

/** The codec of a projected row. */
export type ProjectedCodec<F> = Schema.declareConstructor<
	Schema.Struct<ProjectedFields<F>>['Type'],
	Schema.Struct<ProjectedFields<F>>['Encoded'],
	readonly [Schema.Struct<ProjectedFields<F>>]
>;

/**
 * Decodes a projected row. SurrealDB answers a projected NONE with the key
 * present and no value, where a stored row leaves the key out; both read as
 * `undefined` here, and NONE inside a projected value is an absent key.
 *
 * @internal
 */
export const projectedCodec = <F extends Projection>(
	fields: ProjectedFields<F>,
): ProjectedCodec<F> => {
	const keys = Object.keys(fields);
	return wrap(Schema.Struct(fields), {
		id: 'effect-surrealkit/ProjectedRow',
		expected: 'a projected SurrealDB row',
		preprocess: (input) => {
			if (!isPlainObject(input)) return input;
			const next: Record<string, unknown> = {};
			for (const [key, value] of Object.entries(input)) {
				next[key] = stripNone(value);
			}
			for (const key of keys) {
				if (!(key in next)) next[key] = undefined;
			}
			return next;
		},
		code: ([inner]) =>
			runtimeCode(
				`NoneAsAbsent(${inner?.runtime ?? ''})`,
				inner?.Type ?? 'unknown',
			),
	}) as ProjectedCodec<F>;
};
