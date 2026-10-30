/**
 * Descriptors that generated modules build their tables, edges and functions
 * with, and the refinement hooks applied to them.
 *
 * A table descriptor carries one schema per operation (`select`, `create`,
 * `merge`, `content`), derived from schema document v2's field-presence
 * matrix, together with the shared field schemas and each field's presence
 * per operation, so refinements can rebuild every variant consistently.
 */
import { Schema } from 'effect';

/** An operation with its own generated schema. */
export type Operation = 'select' | 'create' | 'merge' | 'content';

/**
 * How a field takes part in one operation (schema document v2, section 7):
 *
 * - `required` — the key must be present.
 * - `optional` — the key may be omitted (`Schema.optionalKey`).
 * - `removable` — omission leaves the value; `undefined` (NONE) removes it.
 * - `resettable` — omission leaves the value; `undefined` re-applies the
 *   default (`DEFAULT ALWAYS`).
 * - `excluded` — the key is not part of the operation's schema.
 */
export type Presence =
	| 'required'
	| 'optional'
	| 'removable'
	| 'resettable'
	| 'excluded';

export type FieldPresence = { readonly [O in Operation]: Presence };

/** `true` (FULL), `false` (NONE) or a `WHERE` expression. */
export type Permission = boolean | string;

export interface TablePermissions {
	readonly select: Permission;
	readonly create: Permission;
	readonly update: Permission;
	readonly delete: Permission;
}

/** A closed row schema, or an open one (schemaless table). */
export type RowSchema =
	| Schema.Struct<Schema.Struct.Fields>
	| Schema.StructWithRest<
			Schema.Struct<Schema.Struct.Fields>,
			Schema.StructWithRest.Records
	  >;

/** A relation table's endpoints, for compile-time `relate(from, to)` checks. */
export interface EdgeDescriptor {
	readonly table: string;
	readonly in: ReadonlyArray<Schema.Top>;
	readonly out: ReadonlyArray<Schema.Top>;
	readonly enforced: boolean;
	readonly lightweight: boolean;
}

/** The record ids a relation accepts as its `in` endpoint. */
export type EdgeIn<E extends EdgeDescriptor> = E['in'][number]['Type'];
/** The record ids a relation accepts as its `out` endpoint. */
export type EdgeOut<E extends EdgeDescriptor> = E['out'][number]['Type'];

export interface TableDescriptor {
	readonly name: string;
	readonly type: 'normal' | 'any' | 'relation';
	readonly schemafull: boolean;
	readonly drop: boolean;
	readonly id: Schema.Top;
	readonly select: RowSchema;
	/** Absent for lightweight relations, which take no content. */
	readonly create?: RowSchema;
	readonly merge?: RowSchema;
	readonly content?: RowSchema;
	/** Field schemas identical in every operation: the ones `refine` accepts. */
	readonly fields: { readonly [key: string]: Schema.Top };
	readonly presence: { readonly [key: string]: FieldPresence };
	/** Relation tables that use this table as an endpoint. */
	readonly relations: {
		readonly in: ReadonlyArray<string>;
		readonly out: ReadonlyArray<string>;
	};
	readonly permissions: TablePermissions;
	readonly edge?: EdgeDescriptor;
	/** The schema document's semanticDigest. */
	readonly digest: string;
}

/** A view: an id and its query; its row shape is not derived. */
export interface ViewDescriptor {
	readonly name: string;
	readonly type: 'view';
	readonly id: Schema.Top;
	readonly view: string;
	readonly permissions: TablePermissions;
	readonly digest: string;
}

export interface FunctionDescriptor {
	readonly name: `fn::${string}`;
	/** A `Schema.Tuple`; omittable trailing arguments are optional elements. */
	readonly args: Schema.Top;
	/** `Schema.Unknown` when the function declares no return kind. */
	readonly returns: Schema.Top;
}

/** The argument tuple a function accepts. */
export type FunctionArgs<F extends FunctionDescriptor> = F['args']['Type'];
/** What a function call decodes to. */
export type FunctionResult<F extends FunctionDescriptor> = F['returns']['Type'];

/** Declares a generated table descriptor (identity at runtime). */
export const table = <const T extends TableDescriptor>(descriptor: T): T =>
	descriptor;
/** Declares a generated view descriptor (identity at runtime). */
export const view = <const T extends ViewDescriptor>(descriptor: T): T =>
	descriptor;
/** Declares a generated relation endpoint descriptor (identity at runtime). */
export const edge = <const E extends EdgeDescriptor>(descriptor: E): E =>
	descriptor;
/** Declares a generated function descriptor (identity at runtime). */
export const fn = <const F extends FunctionDescriptor>(descriptor: F): F =>
	descriptor;

/** Applies a presence to a field schema, as the generated variants do. */
export const withPresence = (schema: Schema.Top, presence: Presence) => {
	switch (presence) {
		case 'required':
			return schema;
		case 'optional':
			return Schema.optionalKey(schema);
		case 'removable':
		case 'resettable':
			return Schema.optional(schema);
		case 'excluded':
			throw new RangeError(
				'an excluded field has no schema in this operation',
			);
	}
};

/**
 * A schema a field may be replaced with: no services, and no presence of its
 * own (`Schema.optionalKey` / `Schema.optional`), because each variant's
 * presence comes from the table.
 */
export type FieldSchema = Schema.Top & {
	readonly DecodingServices: never;
	readonly EncodingServices: never;
	readonly '~type.optionality': 'required';
	readonly '~encoded.optionality': 'required';
};

/** A schema that narrows `S` without changing its runtime representation. */
export type Narrowing<S extends Schema.Top> = FieldSchema & {
	readonly Type: S['Type'];
	readonly Encoded: S['Encoded'];
};

type Fields<T extends TableDescriptor> = T['fields'];

/** One callback per shared field; unknown keys are rejected. */
export type Refinements<T extends TableDescriptor> = {
	readonly [K in keyof Fields<T>]?: (
		schema: Fields<T>[K],
	) => Narrowing<Fields<T>[K]>;
};

type NoExtraKeys<R, Allowed> = R & {
	readonly [K in Exclude<keyof R, keyof Allowed>]: never;
};

type Rewrap<Old, New> = New extends Schema.Top
	? Old extends Schema.optional<Schema.Top>
		? Schema.optional<New>
		: Old extends Schema.optionalKey<Schema.Top>
			? Schema.optionalKey<New>
			: New
	: never;

type AsFields<F> = F extends Schema.Struct.Fields ? F : never;

type ReplaceFields<F, Replacements> = AsFields<{
	readonly [K in keyof F]: K extends keyof Replacements
		? Rewrap<F[K], Replacements[K]>
		: F[K];
}>;

type ReplaceVariant<V, Replacements> =
	V extends Schema.StructWithRest<Schema.Struct<infer F>, infer R>
		? Schema.StructWithRest<
				Schema.Struct<ReplaceFields<F, Replacements>>,
				R
			>
		: V extends Schema.Struct<infer F>
			? Schema.Struct<ReplaceFields<F, Replacements>>
			: V;

/** A table descriptor with some shared field schemas replaced. */
export type Replaced<T extends TableDescriptor, Replacements> = {
	readonly [K in keyof T]: K extends 'fields'
		? {
				readonly [F in keyof T['fields']]: F extends keyof Replacements
					? Replacements[F]
					: T['fields'][F];
			}
		: K extends Operation
			? ReplaceVariant<T[K], Replacements>
			: T[K];
};

type Returned<R> = {
	readonly [K in keyof R]: R[K] extends (schema: never) => infer S
		? S
		: never;
};

const isStructWithRest = (
	row: RowSchema,
): row is Extract<RowSchema, { readonly records: unknown }> =>
	'records' in row && 'schema' in row;

const replaceInRow = (
	row: RowSchema,
	operation: Operation,
	presence: TableDescriptor['presence'],
	replacements: { readonly [key: string]: Schema.Top },
): RowSchema => {
	const struct = isStructWithRest(row) ? row.schema : row;
	const fields: Record<string, Schema.Constraint> = {};
	for (const [key, field] of Object.entries(struct.fields)) {
		const replacement = replacements[key];
		const fieldPresence = presence[key]?.[operation];
		fields[key] =
			replacement === undefined || fieldPresence === undefined
				? field
				: withPresence(replacement, fieldPresence);
	}
	const rebuilt = Schema.Struct(fields);
	return isStructWithRest(row)
		? Schema.StructWithRest(rebuilt, row.records)
		: rebuilt;
};

const operations: ReadonlyArray<Operation> = [
	'select',
	'create',
	'merge',
	'content',
];

const assertSharedFields = (
	descriptor: TableDescriptor,
	keys: ReadonlyArray<string>,
): void => {
	for (const key of keys) {
		if (!Object.hasOwn(descriptor.fields, key)) {
			throw new RangeError(
				`\`${key}\` is not a shared field of table \`${descriptor.name}\`; refine and replaceField accept: ${Object.keys(descriptor.fields).join(', ')}`,
			);
		}
	}
};

const replaceFields = (
	descriptor: TableDescriptor,
	replacements: { readonly [key: string]: Schema.Top },
): TableDescriptor => {
	assertSharedFields(descriptor, Object.keys(replacements));
	for (const [key, schema] of Object.entries(replacements)) {
		if (schema.ast.context?.isOptional === true) {
			throw new TypeError(
				`the schema for \`${key}\` of table \`${descriptor.name}\` is optional; refine and replaceField take the base schema, and each variant applies the field's own presence`,
			);
		}
	}
	const next: Record<string, unknown> = {
		...descriptor,
		fields: { ...descriptor.fields, ...replacements },
	};
	for (const operation of operations) {
		const row = descriptor[operation];
		if (row !== undefined) {
			next[operation] = replaceInRow(
				row,
				operation,
				descriptor.presence,
				replacements,
			);
		}
	}
	return next as unknown as TableDescriptor;
};

/**
 * Refines shared fields of a generated table. Each callback receives the
 * field's base schema — before operation presence is applied — and its result
 * replaces that field in every variant (select, create, merge and content),
 * re-wrapped with the field's presence in each. A refinement may narrow the
 * type (checks, brands) but not change its representation; use
 * {@link replaceField} for that. Keys that are not shared fields of the table
 * are type errors, for every variant alike.
 *
 * @example
 * const People = refine(PersonTable, {
 *   name: (s) => s.check(Schema.isMinLength(1)),
 * });
 */
export const refine = <
	T extends TableDescriptor,
	const R extends Refinements<T>,
>(
	descriptor: T,
	refinements: NoExtraKeys<R, Fields<T>>,
): Replaced<T, Returned<R>> => {
	const entries = Object.entries(
		refinements as Record<string, (schema: Schema.Top) => Schema.Top>,
	);
	assertSharedFields(
		descriptor,
		entries.map(([key]) => key),
	);
	const replacements: Record<string, Schema.Top> = {};
	for (const [key, refinement] of entries) {
		replacements[key] = refinement(descriptor.fields[key] as Schema.Top);
	}
	return replaceFields(descriptor, replacements) as unknown as Replaced<
		T,
		Returned<R>
	>;
};

/**
 * Replaces one shared field of a generated table outright, in every variant.
 * Unlike {@link refine}, the new schema may change the field's type entirely,
 * which is why this is a separate, conspicuous call.
 */
export const replaceField = <
	T extends TableDescriptor,
	K extends keyof Fields<T> & string,
	S extends FieldSchema,
>(
	descriptor: T,
	key: K,
	schema: S,
): Replaced<T, { readonly [P in K]: S }> =>
	replaceFields(descriptor, { [key]: schema }) as unknown as Replaced<
		T,
		{ readonly [P in K]: S }
	>;
