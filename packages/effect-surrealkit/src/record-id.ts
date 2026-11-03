/**
 * `record<…>`, `table<…>` and `file<…>`.
 *
 * Record ids are checked by their SDK structure — `id.table.name` and the
 * key's runtime shape — never by parsing `table:key` text.
 */
import {
	Effect,
	Schema,
	SchemaGetter,
	SchemaIssue,
	SchemaParser,
	SchemaTransformation,
} from 'effect';
import {
	equals,
	type RecordIdValue,
	FileRef as SdkFileRef,
	RecordId as SdkRecordId,
	Table as SdkTable,
	Uuid as SdkUuid,
} from 'surrealdb';
import { Int } from './int.js';
import {
	codeOf,
	literalUnion,
	quote,
	quoteList,
	runtimeCode,
} from './internal/code.js';
import {
	invalid,
	isPlainObject,
	leaf,
	ok,
	wrap,
	wrongType,
} from './internal/declare.js';
import { toWellFormed } from './scalars.js';

/** Numeric record keys are integers: SurrealDB has no `t:1.5`. */
const isRecordKey = (key: unknown): key is RecordIdValue =>
	typeof key === 'string' ||
	(typeof key === 'number' && Number.isInteger(key)) ||
	typeof key === 'bigint' ||
	key instanceof SdkUuid ||
	Array.isArray(key) ||
	isPlainObject(key);

/** SurrealDB refuses `table:``` (`Found '' for the Record ID but this is not a valid id`). */
const EMPTY_KEY =
	'an empty string is not a valid record id key (SurrealDB rejects it)';

/**
 * Any record-id key the SDK produces: string, integer (number or bigint),
 * `Uuid`, array or object. Used when a table declares no `id` kind.
 */
export const RecordKey = leaf<RecordIdValue>({
	id: 'effect-surrealkit/RecordKey',
	expected:
		'a record id key (string, integer, bigint, Uuid, array or object)',
	code: runtimeCode('RecordKey', 'Surreal.RecordIdValue'),
	parse: (input) =>
		input === ''
			? invalid(EMPTY_KEY)
			: isRecordKey(input)
				? ok(input)
				: wrongType,
	equivalence: equals,
	arbitrary: () =>
		Schema.link<RecordIdValue>()(
			Schema.Union([Schema.String.check(Schema.isMinLength(1)), Int]),
			{
				decode: SchemaGetter.transform((key) =>
					typeof key === 'string' ? toWellFormed(key) : key,
				),
				encode: SchemaGetter.transform((key) =>
					typeof key === 'number' || typeof key === 'string'
						? key
						: String(key),
				),
			},
		),
});

/** A record id of table `Tb` whose key has the decoded type `Key`. */
export type RecordIdOf<Tb extends string, Key> = SdkRecordId<Tb> & {
	readonly id: Key;
};

/**
 * Whether two record keys are the same key: equal by SDK equality, an
 * integer as `number` and as `bigint` counting as one, at every depth.
 */
const sameKey = (a: unknown, b: unknown): boolean => {
	if (
		(typeof a === 'number' || typeof a === 'bigint') &&
		(typeof b === 'number' || typeof b === 'bigint')
	) {
		return (
			(typeof a === 'number' ? Number.isInteger(a) : true) &&
			(typeof b === 'number' ? Number.isInteger(b) : true) &&
			BigInt(a) === BigInt(b)
		);
	}
	if (Array.isArray(a) && Array.isArray(b)) {
		return a.length === b.length && a.every((x, i) => sameKey(x, b[i]));
	}
	if (isPlainObject(a) && isPlainObject(b)) {
		const keys = Object.keys(a);
		return (
			keys.length === Object.keys(b).length &&
			keys.every((k) => Object.hasOwn(b, k) && sameKey(a[k], b[k]))
		);
	}
	return equals(a, b);
};

const tableMismatch = (expected: string, actual: string): string =>
	`expected a record id of table \`${expected}\`, got one of table \`${actual}\``;

const makeRecordIdFor = <Tb extends string, K extends Schema.Top, T, E>(
	table: Tb,
	key: K,
	keyCode: (keyRuntime: string) => string,
	typeCode: (keyType: string) => string,
) =>
	Schema.declareConstructor<T, E>()(
		[key],
		([keyCodec]) =>
			(input, ast, options) => {
				if (!(input instanceof SdkRecordId)) {
					return Effect.fail(
						new SchemaIssue.InvalidType(ast, input, options),
					);
				}
				if (input.id === '') {
					return Effect.fail(
						new SchemaIssue.InvalidValue(
							{ message: EMPTY_KEY },
							input,
							options,
						),
					);
				}
				if (input.table.name !== table) {
					return Effect.fail(
						new SchemaIssue.InvalidValue(
							{ message: tableMismatch(table, input.table.name) },
							input,
							options,
						),
					);
				}
				// A key is an identity: decode it exactly. Excess object
				// properties are an error, never dropped into another record.
				return Effect.flatMapEager(
					Effect.mapErrorEager(
						SchemaParser.decodeUnknownEffect(keyCodec)(input.id, {
							...options,
							onExcessProperty: 'error',
						}),
						(issue) =>
							new SchemaIssue.Composite(
								ast,
								[new SchemaIssue.Pointer(['id'], issue)],
								input,
								options,
							),
					),
					(decoded) => {
						if (decoded === input.id)
							return Effect.succeed(input as T);
						// Only a change of representation (int <-> bigint in
						// the integer profiles) may rebuild the id.
						return sameKey(decoded, input.id)
							? Effect.succeed(
									new SdkRecordId(
										table,
										decoded as RecordIdValue,
									) as T,
								)
							: Effect.fail(
									new SchemaIssue.InvalidValue(
										{
											message: `the key of ${table}:${String(input.id)} decodes to a different key`,
										},
										input,
										options,
									),
								);
					},
				);
			},
		{
			expected: `a record id of table ${table}`,
			representation: {
				id: 'effect-surrealkit/RecordIdFor',
				payload: { table },
			},
			toCode: ({ typeParameters: [keyParameter] }) =>
				runtimeCode(
					keyCode(keyParameter?.runtime ?? ''),
					typeCode(keyParameter?.Type ?? ''),
				),
			toEquivalence: () => equals,
			toCodecArbitrary: ({ typeParameters: [keyType] }) =>
				Schema.link<T>()(keyType, {
					// An empty string is never a valid key; generate one that is.
					decode: SchemaGetter.transform(
						(k) =>
							new SdkRecordId(
								table,
								(k === '' ? 'a' : k) as RecordIdValue,
							) as T,
					),
					encode: SchemaGetter.transform(
						(id) => (id as SdkRecordId).id as typeof keyType.Type,
					),
				}),
		},
	);

/**
 * `record<table>`: an SDK `RecordId` whose `table.name` is `table`.
 *
 * With `key`, the record's key is decoded and encoded through that schema
 * (for example `Int` for `DEFINE FIELD id ON t TYPE int`, or a `Struct` for
 * an object key); without it, any key the SDK produces is accepted.
 *
 * Brand the result per logical store in generated code:
 * `RecordIdFor("person").pipe(Schema.brand("app/person"))`.
 */
export function RecordIdFor<const Tb extends string>(
	table: Tb,
): Schema.declareConstructor<
	SdkRecordId<Tb>,
	SdkRecordId<Tb>,
	readonly [typeof RecordKey]
>;
export function RecordIdFor<const Tb extends string, K extends Schema.Top>(
	table: Tb,
	options: { readonly key: K },
): Schema.declareConstructor<
	RecordIdOf<Tb, K['Type']>,
	RecordIdOf<Tb, K['Encoded']>,
	readonly [K]
>;
export function RecordIdFor(
	table: string,
	options?: { readonly key: Schema.Top },
): Schema.Top {
	if (options === undefined) {
		return makeRecordIdFor(
			table,
			RecordKey,
			() => `RecordIdFor(${quote(table)})`,
			() => `Surreal.RecordId<${quote(table)}>`,
		);
	}
	return makeRecordIdFor(
		table,
		options.key,
		(key) => `RecordIdFor(${quote(table)}, { key: ${key} })`,
		(key) => `SurrealSchema.RecordIdOf<${quote(table)}, ${key}>`,
	);
}

/** Bare `record`: an SDK `RecordId` of any table. */
export const RecordIdAny = leaf<SdkRecordId>({
	id: 'effect-surrealkit/RecordIdAny',
	expected: 'a record id',
	code: runtimeCode('RecordIdAny', 'Surreal.RecordId'),
	parse: (input) =>
		input instanceof SdkRecordId && isRecordKey(input.id)
			? input.id === ''
				? invalid(EMPTY_KEY)
				: ok(input)
			: wrongType,
	equivalence: equals,
	arbitrary: () =>
		Schema.link<SdkRecordId>()(
			Schema.Tuple([
				Schema.String.check(Schema.isPattern(/^[a-z_][a-z0-9_]*$/)),
				Schema.String.check(Schema.isMinLength(1)),
			]),
			{
				decode: SchemaGetter.transform(
					([tb, key]) => new SdkRecordId(tb, toWellFormed(key)),
				),
				encode: SchemaGetter.transform(
					(id) => [id.table.name, String(id.id)] as const,
				),
			},
		),
});

type Names<Tbs extends ReadonlyArray<string>> = Tbs extends readonly []
	? string
	: Tbs[number];

/** `table` / `table<a | b>`: an SDK `Table` naming one of `tables` (any when empty). */
export const TableFor = <const Tbs extends ReadonlyArray<string>>(
	tables: Tbs,
) =>
	leaf<SdkTable<Names<Tbs>>>({
		id: 'effect-surrealkit/TableFor',
		payload: [...tables],
		expected:
			tables.length === 0
				? 'a table'
				: `one of the tables ${tables.join(', ')}`,
		code: runtimeCode(
			`TableFor(${quoteList(tables)})`,
			`Surreal.Table<${literalUnion(tables)}>`,
		),
		parse: (input) => {
			if (!(input instanceof SdkTable)) return wrongType;
			if (tables.length > 0 && !tables.includes(input.name)) {
				return invalid(
					`table \`${input.name}\` is not one of ${tables.join(', ')}`,
				);
			}
			return ok(input as SdkTable<Names<Tbs>>);
		},
		equivalence: (self, that) => self.equals(that),
		arbitrary: () =>
			Schema.link<SdkTable<Names<Tbs>>>()(
				tables.length === 0
					? Schema.String.check(
							Schema.isPattern(/^[a-z_][a-z0-9_]*$/),
						)
					: Schema.Literals(tables),
				{
					decode: SchemaGetter.transform(
						(name) => new SdkTable(name) as SdkTable<Names<Tbs>>,
					),
					encode: SchemaGetter.transform((table) => table.name),
				},
			),
	});

/** `file` / `file<a | b>`: an SDK `FileRef` in one of `buckets` (any when empty). */
export const FileRefFor = <const Bs extends ReadonlyArray<string>>(
	buckets: Bs,
) =>
	leaf<SdkFileRef>({
		id: 'effect-surrealkit/FileRefFor',
		payload: [...buckets],
		expected:
			buckets.length === 0
				? 'a file reference'
				: `a file reference in ${buckets.join(', ')}`,
		code: runtimeCode(
			`FileRefFor(${quoteList(buckets)})`,
			'Surreal.FileRef',
		),
		parse: (input) => {
			if (!(input instanceof SdkFileRef)) return wrongType;
			if (buckets.length > 0 && !buckets.includes(input.bucket)) {
				return invalid(
					`file bucket \`${input.bucket}\` is not one of ${buckets.join(', ')}`,
				);
			}
			return ok(input);
		},
		equivalence: (self, that) => self.equals(that),
		arbitrary: () =>
			Schema.link<SdkFileRef>()(
				Schema.Tuple([
					buckets.length === 0
						? Schema.String.check(
								Schema.isPattern(/^[a-z_][a-z0-9_]*$/),
							)
						: Schema.Literals(buckets),
					Schema.String.check(Schema.isPattern(/^\/[a-z0-9_./-]*$/)),
				]),
				{
					decode: SchemaGetter.transform(
						([bucket, key]) => new SdkFileRef(bucket, key),
					),
					encode: SchemaGetter.transform(
						(file) => [file.bucket, file.key] as const,
					),
				},
			),
	});

type RecordIdSchemas = { readonly [table: string]: Schema.Top };

/** The application value of {@link taggedRecordId}: `{ _tag: table, id }`. */
export type TaggedRecordId<M extends RecordIdSchemas> = {
	readonly [K in keyof M & string]: {
		readonly _tag: K;
		readonly id: M[K]['Type'];
	};
}[keyof M & string];

/**
 * An application representation of `record<a | b>`: decodes a record id into
 * `{ _tag: "<table>", id }` so the union can be matched on `_tag`, and
 * encodes it back to the bare id. The tag is never stored. Encoding rejects
 * a tag that disagrees with the id's table.
 */
export const taggedRecordId = <const M extends RecordIdSchemas>(ids: M) => {
	const tables = Object.keys(ids).sort();
	if (tables.length === 0) {
		throw new RangeError('taggedRecordId needs at least one table');
	}
	const idSchemas = tables.map((table) => ids[table] as Schema.Top);
	const tagged = tables.map((table) =>
		Schema.TaggedStruct(table, {
			id: Schema.toType(ids[table] as Schema.Top),
		}),
	);
	const inner = Schema.Union(idSchemas).pipe(
		Schema.decodeTo(
			Schema.Union(tagged),
			SchemaTransformation.transformEffect<
				{ readonly _tag: string; readonly id: unknown },
				unknown
			>({
				// The union in front of this transformation only lets record ids
				// of the listed tables through.
				decode: (id) =>
					Effect.succeed({
						_tag: (id as SdkRecordId).table.name,
						id,
					}),
				encode: (value) => {
					const table = (value.id as SdkRecordId).table.name;
					return table === value._tag
						? Effect.succeed(value.id)
						: Effect.fail(
								new SchemaIssue.InvalidValue({
									message: `tag \`${value._tag}\` does not match the id's table \`${table}\``,
								}),
							);
				},
			}),
		),
	);
	return wrap(inner, {
		id: 'effect-surrealkit/TaggedRecordId',
		payload: tables,
		expected: `a record id of ${tables.join(', ')}`,
		// The wrapped union only shows the encoded side, so print the map.
		code: () => {
			const entries = tables.map((table) => {
				const code = codeOf(ids[table] as Schema.Top);
				return { table, code };
			});
			const runtimeMap = entries
				.map(({ table, code }) => `${quote(table)}: ${code.runtime}`)
				.join(', ');
			const typeMap = entries
				.map(
					({ table, code }) =>
						`${quote(table)}: Schema.Codec<${code.Type}>`,
				)
				.join('; ');
			return {
				runtime: `SurrealSchema.taggedRecordId({ ${runtimeMap} })`,
				Type: `SurrealSchema.TaggedRecordId<{ ${typeMap} }>`,
				importDeclarations: runtimeCode('', '').importDeclarations,
			};
		},
		equivalence: (self, that) =>
			(self as { _tag: string })._tag ===
				(that as { _tag: string })._tag &&
			equals((self as { id: unknown }).id, (that as { id: unknown }).id),
	}) as unknown as Schema.declareConstructor<
		TaggedRecordId<M>,
		M[keyof M]['Encoded'],
		readonly [Schema.Codec<unknown, unknown>]
	>;
};
