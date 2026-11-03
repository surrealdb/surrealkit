/**
 * Schema document v2 (docs/typegen/schema-document-v2.md), decoded strictly:
 * every property the contract requires must be present, nothing else may be,
 * and every closed vocabulary is checked. A document that does not decode is
 * rejected before anything is generated.
 */
import { Data, Effect, Schema, SchemaIssue } from 'effect';

const Name = Schema.String.check(Schema.isMinLength(1));
const Expr = Schema.String;
const Path = Schema.Array(Name).check(Schema.isMinLength(1));
const Names = Schema.Array(Name);
const NonNegativeInt = Schema.Number.check(
	Schema.isInt(),
	Schema.isGreaterThanOrEqualTo(0),
);

export const Permission = Schema.Union([Schema.Boolean, Expr]);
export type Permission = typeof Permission.Type;

export const TablePermissions = Schema.Struct({
	select: Permission,
	create: Permission,
	update: Permission,
	delete: Permission,
});
export type TablePermissions = typeof TablePermissions.Type;

export const FieldPermissions = Schema.Struct({
	select: Permission,
	create: Permission,
	update: Permission,
});
export type FieldPermissions = typeof FieldPermissions.Type;

const PrimitiveKindName = Schema.Literals([
	'any',
	'none',
	'null',
	'bool',
	'int',
	'float',
	'decimal',
	'number',
	'string',
	'datetime',
	'duration',
	'uuid',
	'bytes',
	'regex',
	'range',
	'function',
	'object',
]);
export type PrimitiveKindName = typeof PrimitiveKindName.Type;

export const GeometryType = Schema.Literals([
	'collection',
	'line',
	'multiline',
	'multipoint',
	'multipolygon',
	'point',
	'polygon',
]);

export const LiteralType = Schema.Literals([
	'string',
	'int',
	'float',
	'decimal',
	'bool',
	'duration',
]);

export interface LiteralKind {
	readonly kind: 'literal';
	readonly type: typeof LiteralType.Type;
	readonly value: string | boolean;
}

export type Kind =
	| { readonly kind: PrimitiveKindName }
	| { readonly kind: 'record'; readonly tables: ReadonlyArray<string> }
	| { readonly kind: 'table'; readonly tables: ReadonlyArray<string> }
	| { readonly kind: 'file'; readonly buckets: ReadonlyArray<string> }
	| {
			readonly kind: 'geometry';
			readonly types: ReadonlyArray<typeof GeometryType.Type>;
	  }
	| { readonly kind: 'array'; readonly item: Kind; readonly length?: number }
	| { readonly kind: 'set'; readonly item: Kind; readonly length?: number }
	| { readonly kind: 'tuple'; readonly items: ReadonlyArray<Kind> }
	| {
			readonly kind: 'objectLiteral';
			readonly fields: ReadonlyArray<{
				readonly name: string;
				readonly kind: Kind;
				readonly optional: boolean;
			}>;
	  }
	| LiteralKind
	| { readonly kind: 'option'; readonly item: Kind }
	| { readonly kind: 'union'; readonly variants: ReadonlyArray<Kind> }
	| { readonly kind: 'unknown'; readonly source: string };

const KindRef = Schema.suspend((): Schema.Codec<Kind> => KindSchema);

const LiteralKindSchema = Schema.Struct({
	kind: Schema.Literal('literal'),
	type: LiteralType,
	value: Schema.Union([Schema.String, Schema.Boolean]),
}).check(
	Schema.makeFilter(
		(literal) =>
			(literal.type === 'bool') ===
				(typeof literal.value === 'boolean') ||
			'a bool literal has a boolean value; every other literal a string',
	),
);

export const KindSchema: Schema.Codec<Kind> = Schema.Union([
	Schema.Struct({ kind: PrimitiveKindName }),
	Schema.Struct({ kind: Schema.Literal('record'), tables: Names }),
	Schema.Struct({ kind: Schema.Literal('table'), tables: Names }),
	Schema.Struct({ kind: Schema.Literal('file'), buckets: Names }),
	Schema.Struct({
		kind: Schema.Literal('geometry'),
		types: Schema.Array(GeometryType),
	}),
	Schema.Struct({
		kind: Schema.Literal('array'),
		item: KindRef,
		length: Schema.optionalKey(NonNegativeInt),
	}),
	Schema.Struct({
		kind: Schema.Literal('set'),
		item: KindRef,
		length: Schema.optionalKey(NonNegativeInt),
	}),
	Schema.Struct({
		kind: Schema.Literal('tuple'),
		items: Schema.Array(KindRef),
	}),
	Schema.Struct({
		kind: Schema.Literal('objectLiteral'),
		fields: Schema.Array(
			Schema.Struct({
				name: Name,
				kind: KindRef,
				optional: Schema.Boolean,
			}),
		),
	}),
	LiteralKindSchema,
	Schema.Struct({ kind: Schema.Literal('option'), item: KindRef }),
	Schema.Struct({
		kind: Schema.Literal('union'),
		variants: Schema.Array(KindRef).check(Schema.isMinLength(2)),
	}),
	Schema.Struct({ kind: Schema.Literal('unknown'), source: Schema.String }),
]);

const Bound = Schema.Struct({
	value: Schema.String.check(Schema.isMinLength(1)),
	inclusive: Schema.Boolean,
});

export const Refinement = Schema.Union([
	Schema.Struct({
		type: Schema.Literal('literals'),
		values: Schema.Array(LiteralKindSchema).check(Schema.isMinLength(1)),
	}),
	Schema.Struct({
		type: Schema.Literal('range'),
		valueKind: Schema.Literals(['int', 'float', 'decimal', 'duration']),
		min: Schema.optionalKey(Bound),
		max: Schema.optionalKey(Bound),
	}),
	Schema.Struct({
		type: Schema.Literal('length'),
		target: Schema.Literals(['string', 'array']),
		min: Schema.optionalKey(NonNegativeInt),
		max: Schema.optionalKey(NonNegativeInt),
	}),
	Schema.Struct({ type: Schema.Literal('email') }),
]);
export type Refinement = typeof Refinement.Type;

export const Field = Schema.Struct({
	path: Path,
	kind: Schema.optionalKey(KindSchema),
	optional: Schema.Boolean,
	flexible: Schema.Boolean,
	readonly: Schema.Boolean,
	inline: Schema.Boolean,
	default: Schema.optionalKey(
		Schema.Struct({
			mode: Schema.Literals(['create', 'always']),
			expr: Expr,
		}),
	),
	value: Schema.optionalKey(
		Schema.Struct({ expr: Expr, readsInput: Schema.Boolean }),
	),
	computed: Schema.optionalKey(Schema.Struct({ expr: Expr })),
	assert: Schema.optionalKey(
		Schema.Struct({
			expr: Expr,
			recognized: Schema.optionalKey(
				Schema.Array(Refinement).check(Schema.isMinLength(1)),
			),
		}),
	),
	reference: Schema.optionalKey(
		Schema.Union([
			Schema.Struct({
				onDelete: Schema.Literals([
					'reject',
					'ignore',
					'cascade',
					'unset',
				]),
			}),
			Schema.Struct({ onDelete: Schema.Literal('then'), expr: Expr }),
		]),
	),
	permissions: FieldPermissions,
	comment: Schema.optionalKey(Schema.String),
	define: Schema.optionalKey(Schema.String),
});
export type Field = typeof Field.Type;

const Index = Schema.Struct({
	name: Name,
	columns: Schema.Array(Path),
	type: Schema.Literals([
		'standard',
		'unique',
		'fulltext',
		'hnsw',
		'diskann',
		'count',
	]),
	clause: Schema.String,
	fulltext: Schema.optionalKey(
		Schema.Struct({ analyzer: Name, highlights: Schema.Boolean }),
	),
	vector: Schema.optionalKey(
		Schema.Struct({
			dimension: Schema.Number.check(
				Schema.isInt(),
				Schema.isGreaterThanOrEqualTo(1),
			),
			distance: Name,
			elementType: Name,
		}),
	),
	where: Schema.optionalKey(Expr),
	comment: Schema.optionalKey(Schema.String),
});

const Event = Schema.Struct({
	name: Name,
	when: Expr,
	// biome-ignore lint/suspicious/noThenProperty: the contract names the event body `then`
	then: Schema.Array(Expr).check(Schema.isMinLength(1)),
	async: Schema.Boolean,
	retry: Schema.optionalKey(NonNegativeInt),
	maxDepth: Schema.optionalKey(NonNegativeInt),
	comment: Schema.optionalKey(Schema.String),
});

export const TableType = Schema.Union([
	Schema.Struct({ kind: Schema.Literals(['normal', 'any']) }),
	Schema.Struct({
		kind: Schema.Literal('relation'),
		in: Names,
		out: Names,
		enforced: Schema.Boolean,
		lightweight: Schema.Boolean,
	}),
]);
export type TableType = typeof TableType.Type;

export const Table = Schema.Struct({
	name: Name,
	type: TableType,
	schemafull: Schema.Boolean,
	drop: Schema.Boolean,
	view: Schema.optionalKey(Schema.Struct({ select: Expr })),
	changefeed: Schema.optionalKey(
		Schema.Struct({
			expiry: Schema.String.check(Schema.isMinLength(1)),
			includeOriginal: Schema.Boolean,
		}),
	),
	permissions: TablePermissions,
	idKey: Schema.optionalKey(KindSchema),
	comment: Schema.optionalKey(Schema.String),
	fields: Schema.Array(Field),
	indexes: Schema.Array(Index),
	events: Schema.Array(Event),
	define: Schema.optionalKey(Schema.String),
});
export type Table = typeof Table.Type;

export const FunctionArg = Schema.Struct({
	name: Name,
	kind: KindSchema,
	optional: Schema.Boolean,
	omittable: Schema.Boolean,
});

export const SurrealFunction = Schema.Struct({
	name: Schema.String.check(Schema.isPattern(/^fn::/)),
	args: Schema.Array(FunctionArg),
	returns: Schema.optionalKey(KindSchema),
	block: Expr,
	permissions: Permission,
	comment: Schema.optionalKey(Schema.String),
});
export type SurrealFunction = typeof SurrealFunction.Type;

const Durations = Schema.Struct({
	grant: Schema.optionalKey(Schema.String),
	token: Schema.optionalKey(Schema.String),
	session: Schema.optionalKey(Schema.String),
});

const Middleware = Schema.Array(
	Schema.Struct({ name: Name, args: Schema.Array(Schema.String) }),
);

const Sha256Hex = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));

export const Provenance = Schema.Struct({
	generator: Schema.Struct({
		name: Schema.Literal('surrealkit'),
		version: Schema.String.check(Schema.isMinLength(1)),
		commit: Schema.optionalKey(
			Schema.String.check(Schema.isPattern(/^[0-9a-f]{7,40}$/)),
		),
	}),
	engine: Schema.Struct({
		version: Schema.String.check(
			Schema.isPattern(/^[0-9]+\.[0-9]+\.[0-9]+$/),
		),
		build: Schema.optionalKey(Schema.String.check(Schema.isMinLength(1))),
		mode: Schema.Literals(['embedded', 'remote']),
	}),
	adapter: Schema.Literals(['structure-3.2', 'structure-3.3']),
	source: Schema.Union([
		Schema.Struct({
			kind: Schema.Literal('compile'),
			modules: Names,
			files: Schema.Array(
				Schema.Struct({
					path: Schema.String.check(Schema.isPattern(/^[^\\]+$/)),
					sha256: Sha256Hex,
				}),
			),
		}),
		Schema.Struct({
			kind: Schema.Literal('introspect'),
			namespace: Schema.String,
			database: Schema.String,
		}),
	]),
	capabilities: Schema.optionalKey(
		Schema.Struct({
			experimental: Names,
			network: Schema.Boolean,
		}),
	),
});
export type Provenance = typeof Provenance.Type;

export const Diagnostic = Schema.Struct({
	severity: Schema.Literals(['error', 'warning', 'info']),
	code: Schema.String.check(
		Schema.isPattern(/^[a-z][a-z0-9]*(-[a-z0-9]+)*$/),
	),
	table: Schema.optionalKey(Name),
	field: Schema.optionalKey(Path),
	function: Schema.optionalKey(
		Schema.String.check(Schema.isPattern(/^fn::/)),
	),
	source: Schema.String,
	span: Schema.optionalKey(
		Schema.Struct({ start: NonNegativeInt, end: NonNegativeInt }),
	),
	reason: Schema.String.check(Schema.isMinLength(1)),
});
export type Diagnostic = typeof Diagnostic.Type;

const verify = Schema.Struct({ alg: Schema.String });
const verifyUrl = Schema.Struct({ url: Schema.String });

export const SchemaDocument = Schema.Struct({
	version: Schema.Literal(2),
	semanticDigest: Schema.String.check(
		Schema.isPattern(/^sha256:[0-9a-f]{64}$/),
	),
	provenance: Provenance,
	tables: Schema.Array(Table),
	functions: Schema.Array(SurrealFunction),
	params: Schema.Array(
		Schema.Struct({
			name: Schema.String.check(Schema.isPattern(/^[^$]/)),
			value: Schema.String,
			permissions: Permission,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	accesses: Schema.Array(
		Schema.Struct({
			name: Name,
			method: Schema.Literals(['record', 'jwt', 'bearer']),
			signup: Schema.optionalKey(Expr),
			signin: Schema.optionalKey(Expr),
			authenticate: Schema.optionalKey(Expr),
			refresh: Schema.optionalKey(Schema.Boolean),
			verify: Schema.optionalKey(Schema.Union([verify, verifyUrl])),
			subject: Schema.optionalKey(Schema.Literals(['record', 'user'])),
			durations: Durations,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	analyzers: Schema.Array(
		Schema.Struct({
			name: Name,
			function: Schema.optionalKey(
				Schema.String.check(Schema.isPattern(/^fn::/)),
			),
			tokenizers: Schema.Array(Schema.String),
			filters: Schema.Array(Schema.String),
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	sequences: Schema.Array(
		Schema.Struct({
			name: Name,
			batch: Schema.String.check(Schema.isPattern(/^[0-9]+$/)),
			start: Schema.String.check(Schema.isPattern(/^-?[0-9]+$/)),
			timeout: Schema.optionalKey(Schema.String),
		}),
	),
	buckets: Schema.Array(
		Schema.Struct({
			name: Name,
			backend: Schema.String,
			readonly: Schema.Boolean,
			permissions: Permission,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	apis: Schema.Array(
		Schema.Struct({
			path: Schema.String.check(Schema.isPattern(/^\//)),
			actions: Schema.Array(
				Schema.Struct({
					methods: Schema.Array(
						Schema.Literals([
							'delete',
							'get',
							'patch',
							'post',
							'put',
							'trace',
						]),
					).check(Schema.isMinLength(1)),
					middleware: Middleware,
					permissions: Permission,
					action: Expr,
				}),
			),
			middleware: Middleware,
			permissions: Permission,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	modules: Schema.Array(
		Schema.Struct({
			name: Schema.String.check(Schema.isPattern(/^mod::/)),
			executable: Schema.Struct({
				type: Schema.String,
				bucket: Schema.optionalKey(Schema.String),
				key: Schema.optionalKey(Schema.String),
			}),
			permissions: Permission,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	configs: Schema.Array(
		Schema.Union([
			Schema.Struct({
				type: Schema.Literal('graphql'),
				tables: Schema.String,
				functions: Schema.String,
			}),
			Schema.Struct({
				type: Schema.Literal('api'),
				middleware: Middleware,
				permissions: Permission,
			}),
		]),
	),
	users: Schema.Array(
		Schema.Struct({
			name: Name,
			roles: Schema.Array(Schema.String),
			durations: Durations,
			comment: Schema.optionalKey(Schema.String),
		}),
	),
	diagnostics: Schema.Array(Diagnostic),
});
export type SchemaDocument = typeof SchemaDocument.Type;

/** A document that failed strict decoding. */
export class DocumentError extends Data.TaggedError('DocumentError')<{
	readonly message: string;
}> {}

const formatter = SchemaIssue.makeFormatterDefault();

/**
 * Decodes an unknown value (parsed JSON) as a schema document v2, rejecting
 * unknown properties anywhere in the tree.
 */
export const decodeDocument = (
	input: unknown,
): Effect.Effect<SchemaDocument, DocumentError> =>
	Schema.decodeUnknownEffect(SchemaDocument)(input, {
		onExcessProperty: 'error',
		errors: 'all',
	}).pipe(
		Effect.mapError(
			(error) =>
				new DocumentError({
					message: `not a valid schema document v2:\n${formatter(error.issue)}`,
				}),
		),
	);
