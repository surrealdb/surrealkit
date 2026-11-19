/**
 * Record links and graph traversal, typed from the generated descriptors
 * and the store registry.
 *
 * A record link (`record<person>`) is followed to its target's fields with
 * {@link follow}, materialised with {@link rows} and destructured with
 * {@link destructure}; the target is found by name in the store's `tables`.
 * A graph step ({@link outgoing}, {@link incoming}, {@link both}) is typed
 * from the store's `edges`: it is legal only from a table the relation
 * accepts on that side, and yields the records on the other side, always as
 * a list. Recursive paths repeat a step ({@link recurse}, {@link collect},
 * {@link shortest}).
 *
 * Everything here is an expression, so it goes wherever expressions go: a
 * condition, a projection, `SELECT VALUE`.
 */
import { Schema, SchemaAST } from 'effect';
import { escapeIdent, type RecordId } from 'surrealdb';

import {
	admit,
	type CodecOf,
	type Context,
	codecOf,
	contextOf,
	derive,
	type Expr,
	joinContexts,
	Level,
	makeExpr,
	makePath,
	mergeParams,
	type OrNone,
	operand,
	orNone,
	type ParamsOf,
	type ParamsOfAll,
	paramsOf,
	RowScope,
	sqlOf,
} from './expr.js';
import {
	type Fields,
	fieldsOf,
	type ProjectedCodec,
	type Projection,
	type RowCodec,
	type RowCodecIn,
	type RowOf,
	rowCodec,
} from './row.js';
import type { Codec, NoParams, Params } from './statement.js';
import {
	type Condition,
	type PureTable,
	project,
	type RowExprs,
	readerOf,
} from './table.js';

// ---- the store

/** A relation's endpoints, as a store registry holds them. */
export interface GraphEdge {
	readonly table: string;
	readonly in: ReadonlyArray<Codec>;
	readonly out: ReadonlyArray<Codec>;
	readonly enforced: boolean;
}

/**
 * What the graph functions read from a generated store registry
 * (`_store.ts`): every table and every relation's endpoints, by name.
 */
export interface GraphStore {
	readonly tables: { readonly [name: string]: PureTable };
	readonly edges: { readonly [name: string]: GraphEdge };
}

// ---- what a link is, at the type level

/** The tables a record id type, or a list of them, names. */
export type LinkTables<E> =
	E extends RecordId<infer Tb>
		? Tb
		: E extends ReadonlyArray<infer I>
			? LinkTables<I>
			: never;

type EncodedOf<R> = CodecOf<R>['Encoded'];

type TableNames<S> = S extends { readonly tables: infer M } ? keyof M : never;

/** The descriptor of each table in `Tb`, as a union. */
export type TableIn<S, Tb> = S extends { readonly tables: infer M }
	? Tb extends keyof M
		? M[Tb]
		: never
	: never;

type SelectOf<T> = T extends { readonly select: infer R extends RowCodecIn }
	? R
	: never;

/** The select schema of every table a link of type `E` can point at. */
export type TargetSelect<S, E> = SelectOf<TableIn<S, LinkTables<E>>>;

/** The field schemas of every target, as a union: its keys are the common ones. */
export type TargetFields<S, E> = FieldsOf<TargetSelect<S, E>>;

type FieldsOf<R> = R extends { readonly schema: { readonly fields: infer F } }
	? F
	: R extends { readonly fields: infer F }
		? F
		: never;

const traversalMarker = '~effect-surrealkit/Traversal';
/** Marks the expressions graph steps and recursive paths yield. */
export const TraversalTypeId: typeof traversalMarker = traversalMarker;

/** @internal */
export interface TraversalData<
	Present extends boolean = boolean,
	Edge extends Codec = Codec,
> {
	/**
	 * Every record the list holds exists and is readable: the engine drops
	 * an edge whose endpoint the session may not read, and an `ENFORCED`
	 * relation has no edge to a missing record. A relation that is not
	 * enforced can point at a missing record, which reads as NONE.
	 */
	readonly present: Present;
	/** The codec of one record of the list. */
	readonly item: Codec;
	/** The edges a step walks (`id->member_of`), for {@link edges}. */
	readonly edges?: {
		readonly sql: string;
		readonly codec: Edge;
		readonly item: Codec;
		/**
		 * The step filters the records it reaches, which its edges do not:
		 * they would include the edges to the records it leaves out.
		 */
		readonly filtered?: boolean;
	};
}

/**
 * The list of records a graph step or a recursive path yields. It is an
 * expression like any other (its codec decodes the record ids), and the
 * link functions read it as a list of links.
 */
export interface Traversal<
	S extends Codec = Codec,
	P extends Params = Params,
	Present extends boolean = boolean,
	Edge extends Codec = never,
> extends Expr<S, P> {
	readonly [TraversalTypeId]: TraversalData<Present, Edge>;
}

type PresentOf<R> = R extends {
	readonly [TraversalTypeId]: TraversalData<infer B, Codec>;
}
	? B
	: false;

type AsCodec<C> = Extract<C, Codec>;

/**
 * The codec of what reading `Leaf` through a link of type `E` yields: a
 * list for each list, NONE where the link may be NONE, and NONE for a
 * target that may be missing or unreadable (`Present` false), which is
 * what SurrealDB 3.3 and 4.0 answer for one.
 */
export type Lift<E, Present extends boolean, Leaf extends Codec> = [
	undefined,
] extends [E]
	? OrNone<AsCodec<LiftDefined<Exclude<E, undefined>, Present, Leaf>>>
	: LiftDefined<E, Present, Leaf>;

// NONE is handled once, outside: a conditional `OrNone` of a recursive
// alias sends the checker into an unbounded expansion.
type LiftDefined<E, Present extends boolean, Leaf extends Codec> = [E] extends [
	ReadonlyArray<infer I>,
]
	? Schema.$Array<AsCodec<LiftDefined<I, Present, Leaf>>>
	: Present extends true
		? Leaf
		: OrNone<Leaf>;

/** Why `E` cannot be followed through store `S`, or `never`. */
export type LinkProblem<S, E> = [LinkTables<E>] extends [never]
	? 'this expression is not a record link: follow a field typed `record<table>`, a list of them, or a graph step'
	: string extends LinkTables<E>
		? 'this link can point at any table (`record`), so its target has no known fields; declare it `record<table>`'
		: [Exclude<LinkTables<E>, TableNames<S>>] extends [never]
			? never
			: `this link points at \`${Exclude<LinkTables<E>, TableNames<S>> & string}\`, which is not a table of this store`;

/** `R`, when it is a link store `S` can follow, or else a message saying why not. */
export type Followable<S, R> = [LinkProblem<S, EncodedOf<R>>] extends [never]
	? R
	: LinkProblem<S, EncodedOf<R>>;

type Keys<F> = Extract<keyof F, string>;

type Unwrapped<C> = C extends { readonly schema: infer I extends Codec }
	? C extends { readonly '~type.optionality': 'optional' }
		? I
		: C
	: C;

type IsOptional<C> = C extends { readonly '~type.optionality': 'optional' }
	? true
	: false;

type Base<C> = AsCodec<Unwrapped<C>>;

/**
 * The fields of a link's target(s), one expression each: every field the
 * targets have in common, with the codecs of all of them. A field read
 * through a link may be NONE (the target may be missing or unreadable, or
 * the field optional); through a list of links it is a list.
 */
export type Linked<
	F,
	E,
	Present extends boolean,
	Optional extends boolean = false,
> = {
	readonly [K in Keys<F>]: LinkedField<
		F[K],
		E,
		Present,
		Optional extends true
			? true
			: true extends IsOptional<F[K]>
				? true
				: false
	>;
};

type LinkedField<
	C,
	E,
	Present extends boolean,
	Optional extends boolean,
> = Expr<
	AsCodec<
		Lift<E, Present, Optional extends true ? OrNone<Base<C>> : Base<C>>
	>,
	NoParams
> &
	([Base<C>] extends [RowCodecIn]
		? Linked<FieldsOf<Base<C>>, E, Present, Optional>
		: unknown);

/** The fields {@link follow} yields for the link `R`. */
export type FollowedOf<S, R> = Linked<
	TargetFields<S, EncodedOf<R>>,
	EncodedOf<R>,
	PresentOf<R>
>;

// ---- what a link is, at run time

/** @internal */
export type LinkShape =
	| { readonly kind: 'id'; readonly tables: ReadonlyArray<string> | 'any' }
	| { readonly kind: 'optional'; readonly of: LinkShape }
	| { readonly kind: 'list'; readonly of: LinkShape };

const representation = (ast: SchemaAST.AST): string | undefined => {
	const value = (
		ast.annotations as
			| { readonly representation?: { readonly id?: unknown } }
			| undefined
	)?.representation?.id;
	return typeof value === 'string' ? value : undefined;
};

const tableOf = (ast: SchemaAST.AST): string | undefined => {
	const payload = (
		ast.annotations as
			| { readonly representation?: { readonly payload?: unknown } }
			| undefined
	)?.representation?.payload;
	return typeof payload === 'object' &&
		payload !== null &&
		'table' in payload &&
		typeof payload.table === 'string'
		? payload.table
		: undefined;
};

const mergeIds = (shapes: ReadonlyArray<LinkShape>): LinkShape | undefined => {
	const tables = new Set<string>();
	for (const shape of shapes) {
		if (shape.kind !== 'id') return undefined;
		if (shape.tables === 'any') return { kind: 'id', tables: 'any' };
		for (const table of shape.tables) tables.add(table);
	}
	return { kind: 'id', tables: [...tables] };
};

const shapeOfAst = (ast: SchemaAST.AST): LinkShape | undefined => {
	if (SchemaAST.isDeclaration(ast)) {
		const id = representation(ast);
		if (id === 'effect-surrealkit/RecordIdAny') {
			return { kind: 'id', tables: 'any' };
		}
		const table = tableOf(ast);
		return id === 'effect-surrealkit/RecordIdFor' && table !== undefined
			? { kind: 'id', tables: [table] }
			: undefined;
	}
	if (SchemaAST.isUnion(ast)) {
		const members = ast.types.filter(
			(type) => !SchemaAST.isUndefined(type),
		);
		const shapes = members.map(shapeOfAst);
		if (shapes.some((shape) => shape === undefined)) return undefined;
		const merged =
			shapes.length === 1
				? shapes[0]
				: mergeIds(shapes as ReadonlyArray<LinkShape>);
		if (merged === undefined) return undefined;
		return members.length < ast.types.length
			? { kind: 'optional', of: merged }
			: merged;
	}
	if (
		SchemaAST.isArrays(ast) &&
		ast.elements.length === 0 &&
		ast.rest.length === 1
	) {
		const [item] = ast.rest;
		const of = item === undefined ? undefined : shapeOfAst(item);
		return of === undefined ? undefined : { kind: 'list', of };
	}
	return undefined;
};

/**
 * The shape of a link codec: one id, NONE or an id, or a list, and the
 * tables the ids can be of; `undefined` for a codec that is not a link.
 *
 * @internal
 */
export const linkShape = (codec: Codec): LinkShape | undefined =>
	shapeOfAst(codec.ast);

/** The tables a link shape names. @internal */
export const shapeTables = (shape: LinkShape): ReadonlyArray<string> | 'any' =>
	shape.kind === 'id' ? shape.tables : shapeTables(shape.of);

const optionalOf = (shape: LinkShape): boolean => shape.kind === 'optional';

const shapeTablesText = (shape: LinkShape): string => {
	const tables = shapeTables(shape);
	return tables === 'any' ? 'any table' : tables.join(' | ');
};

const lift = (shape: LinkShape, present: boolean, leaf: Codec): Codec => {
	switch (shape.kind) {
		case 'id':
			return present ? leaf : orNone(leaf);
		case 'optional':
			return orNone(lift(shape.of, present, leaf));
		case 'list':
			return Schema.Array(lift(shape.of, present, leaf));
	}
};

/** @internal */
export const traversalOf = (value: unknown): TraversalData | undefined =>
	typeof value === 'object' && value !== null && TraversalTypeId in value
		? (value as { readonly [TraversalTypeId]: TraversalData })[
				TraversalTypeId
			]
		: undefined;

/** Whether `value` is the list a graph step or recursive path yields. */
export const isTraversal = (value: unknown): value is Traversal<Codec> =>
	traversalOf(value) !== undefined;

/** One codec for several, a union unless they are the same. @internal */
export const union = (codecs: ReadonlyArray<Codec>): Codec => {
	const distinct = [...new Set(codecs)];
	return distinct.length === 1
		? (distinct[0] as Codec)
		: (Schema.Union(distinct) as unknown as Codec);
};

const describeLink = (records: Expr): string =>
	`\`${sqlOf(records) || '(the node)'}\``;

/**
 * The reader of the store view a link, a step or FETCH reads records
 * through, which the statement checks against its own table's.
 *
 * @internal
 */
export const viewOf = (store: GraphStore): Context => ({
	readers: new Set([readerOf(store)]),
});

/** The descriptors a link can point at, found in the store by name. */
const targetsOf = (
	store: GraphStore,
	records: Expr,
): {
	readonly shape: LinkShape;
	readonly tables: ReadonlyArray<PureTable>;
} => {
	const shape = linkShape(codecOf(records));
	if (shape === undefined) {
		throw new TypeError(
			`${describeLink(records)} is not a record link: follow a field typed \`record<table>\`, a list of them, or a graph step`,
		);
	}
	const names = shapeTables(shape);
	if (names === 'any') {
		throw new TypeError(
			`${describeLink(records)} can point at any table, so its target has no known fields`,
		);
	}
	const tables = names.map((name) => {
		const table = store.tables[name];
		if (table === undefined) {
			throw new RangeError(`\`${name}\` is not a table of this store`);
		}
		return table;
	});
	return { shape, tables };
};

/** @internal */
export const isOptionalField = (codec: Codec): boolean =>
	codec.ast.context?.isOptional === true;

/** @internal */
export const unwrapField = (codec: Codec): Codec =>
	isOptionalField(codec) && 'schema' in codec && Schema.isSchema(codec.schema)
		? (codec.schema as Codec)
		: codec;

/** The keys every field map has, in the order of the first. */
const commonKeys = (maps: ReadonlyArray<Fields>): ReadonlyArray<string> => {
	const [first, ...rest] = maps;
	return first === undefined
		? []
		: Object.keys(first).filter((key) =>
				rest.every((map) => Object.hasOwn(map, key)),
			);
};

const buildLinked = (
	maps: ReadonlyArray<Fields>,
	prefix: string,
	optional: boolean,
	wrap: (codec: Codec) => Codec,
	params: Params,
	context: Context | undefined,
): Record<string, Expr> => {
	const linked: Record<string, Expr> = {};
	for (const key of commonKeys(maps)) {
		const declared = maps.map((map) => map[key] as Codec);
		const mayBeNone = optional || declared.some(isOptionalField);
		const codecs = declared.map(unwrapField);
		const codec = union(codecs);
		const sql = `${prefix}${escapeIdent(key)}`;
		const nested = codecs.map(fieldsOf);
		const children = nested.every((fields) => fields !== undefined)
			? buildLinked(
					nested as ReadonlyArray<Fields>,
					`${sql}.`,
					mayBeNone,
					wrap,
					params,
					context,
				)
			: {};
		linked[key] = Object.assign(
			children,
			makeExpr(
				wrap(mayBeNone ? orNone(codec) : codec),
				sql,
				params,
				Level.atom,
				context,
			),
		);
	}
	return linked;
};

/** @internal */
export const selectFields = (table: PureTable): Fields => {
	const fields = fieldsOf(table.select);
	if (fields === undefined) {
		throw new TypeError(
			`table \`${table.name}\` has no row schema with declared fields`,
		);
	}
	return fields;
};

/**
 * The fields of the record(s) `records` links to: `manager.name`,
 * `watchers.name`, `id->member_of->?.name`. The target is the store's
 * table of that name; a link to several tables (`record<org | team>`)
 * exposes the fields all of them have, and a link to any table (`record`)
 * exposes none.
 *
 * A field read through a link may be NONE, because SurrealDB answers NONE
 * for a link whose record is missing or the session may not read (3.3 and
 * 4.0 alike); read through a list of links it is a list, with NONE for each
 * such record. The records a graph step over an `ENFORCED` relation yields
 * always exist and are readable, so their fields are NONE only where the
 * field itself may be.
 *
 * @example
 * select(DocTable).where((row) =>
 *   eq(follow(store, row.owner).name, param('owner', SurrealSchema.String)),
 * );
 * // SELECT * FROM ⟨doc⟩ WHERE owner.name = $owner
 */
export const follow = <S extends GraphStore, const R extends Expr>(
	store: S,
	records: Followable<S, R>,
): FollowedOf<S, R> => {
	const link = records as Expr;
	const { shape, tables } = targetsOf(store, link);
	const present = traversalOf(link)?.present ?? false;
	return buildLinked(
		tables.map(selectFields),
		`${operand(link)}.`,
		false,
		(codec) => lift(shape, present, codec),
		paramsOf(link),
		joinContexts([contextOf(link), viewOf(store)]),
	) as unknown as FollowedOf<S, R>;
};

/** The codec a target row decodes with, for one or several tables. @internal */
export const targetRow = (tables: ReadonlyArray<PureTable>): Codec =>
	union(tables.map((table) => rowCodec(table.select) as Codec));

/** What {@link rows} yields for the link `R`: the target rows. */
export type RowsOf<S, R> = AsCodec<
	Lift<EncodedOf<R>, PresentOf<R>, RowCodec<TargetSelect<S, EncodedOf<R>>>>
>;

/**
 * The record(s) `records` links to, materialised (`x.*`): each decodes
 * with its table's select schema, so a list of ids becomes a list of rows.
 * A missing or unreadable record is NONE, as for {@link follow}.
 *
 * @example
 * select(PersonTable).project((row) => ({
 *   friends: rows(store, outgoing(store, row.id, 'knows')),
 * }));
 * // SELECT id->knows->?.* AS friends FROM ⟨person⟩
 */
export const rows = <S extends GraphStore, const R extends Expr>(
	store: S,
	records: Followable<S, R>,
): Expr<RowsOf<S, R>, ParamsOf<R>> => {
	const link = records as Expr;
	const { shape, tables } = targetsOf(store, link);
	const present = traversalOf(link)?.present ?? false;
	return makeExpr(
		lift(shape, present, targetRow(tables)),
		`${operand(link)}.*`,
		paramsOf(link),
		Level.atom,
		joinContexts([contextOf(link), viewOf(store)]),
	) as unknown as Expr<RowsOf<S, R>, ParamsOf<R>>;
};

/** The row object a destructuring callback receives for the link `R`. */
export type TargetRow<S, R> = RowOf<TargetFields<S, EncodedOf<R>>>;

const targetRowExprs = (
	tables: ReadonlyArray<PureTable>,
	scope: RowScope,
): RowOf<Fields> => {
	const build = (
		maps: ReadonlyArray<Fields>,
		prefix: string,
		optional: boolean,
		root: { readonly root: string; readonly rootSql: string } | undefined,
	): Record<string, Expr> => {
		const row: Record<string, Expr> = {};
		for (const key of commonKeys(maps)) {
			const declared = maps.map((map) => map[key] as Codec);
			const mayBeNone = optional || declared.some(isOptionalField);
			const codecs = declared.map(unwrapField);
			const codec = union(codecs);
			const path = `${prefix}${escapeIdent(key)}`;
			const start = root ?? { root: key, rootSql: path };
			const nested = codecs.map(fieldsOf);
			row[key] = Object.assign(
				nested.every((fields) => fields !== undefined)
					? build(
							nested as ReadonlyArray<Fields>,
							`${path}.`,
							mayBeNone,
							start,
						)
					: {},
				makePath(mayBeNone ? orNone(codec) : codec, path, start, scope),
			);
		}
		return row;
	};
	return build(
		tables.map(selectFields),
		'',
		false,
		undefined,
	) as RowOf<Fields>;
};

/** The projection a destructuring callback returned, checked. */
export type CheckedShape<F> = {
	readonly [K in keyof F]: F[K] extends Expr
		? F[K]
		: 'a destructured field is an expression: inline a constant with `literal`';
};

/** What {@link destructure} yields: one projected object per target. */
export type DestructuredOf<R, F> = AsCodec<
	Lift<EncodedOf<R>, PresentOf<R>, ProjectedCodec<F>>
>;

/**
 * Picks fields of the record(s) `records` links to, as SurrealQL
 * destructuring (`x.{ alias: field, … }`): a list of links yields a list of
 * projected objects, and one link one object, or NONE when its record is
 * missing or unreadable. The callback receives the target's fields, as
 * paths relative to it; any expression may be projected, a step from the
 * target included.
 *
 * Inside the callback, a field path always reads the target, so a field of
 * the outer row (which would read the target's field of that name) is
 * refused when the destructuring is built: project it beside the
 * destructured object, or bind the value with `param`. `parent(row.field)`
 * is refused here too, because SurrealDB 3.3 binds `$parent` inside a
 * destructuring to the outer row and 4.0 to the destructured record.
 *
 * @example
 * select(PersonTable).project((row) => ({
 *   teams: destructure(store, outgoing(store, row.id, 'member_of'), (team) => ({
 *     id: team.id,
 *     title: team.name,
 *   })),
 * }));
 * // SELECT id->member_of->?.{ id: id, title: name } AS teams FROM ⟨person⟩
 */
export const destructure = <
	S extends GraphStore,
	const R extends Expr,
	const F,
>(
	store: S,
	records: Followable<S, R>,
	shape: (target: TargetRow<S, R>) => CheckedShape<F>,
): Expr<DestructuredOf<R, F>, ParamsOf<R> & ParamsOfAll<F[keyof F]>> => {
	const link = records as Expr;
	const targets = targetsOf(store, link);
	const present = traversalOf(link)?.present ?? false;
	const scope = new RowScope(
		'target',
		targets.tables.map((table) => table.name).join(' | '),
	);
	const fields = shape(
		targetRowExprs(targets.tables, scope) as unknown as TargetRow<S, R>,
	) as Projection;
	const entries = Object.entries(fields);
	const projected = project(fields);
	const inner = admit(
		{
			own: scope,
			what: 'a destructuring callback',
			parents: 'refuse',
			refused:
				'SurrealDB 3.3 binds `$parent` inside a destructuring to the outer row and 4.0 to the destructured record; project the step beside the destructured object',
		},
		Object.values(fields),
	);
	return makeExpr(
		lift(targets.shape, present, projected.codec),
		`${operand(link)}.{ ${entries
			.map(([alias, value]) => `${escapeIdent(alias)}: ${sqlOf(value)}`)
			.join(', ')} }`,
		mergeParams(paramsOf(link), projected.params),
		Level.atom,
		joinContexts([contextOf(link), inner, viewOf(store)]),
	) as unknown as Expr<
		DestructuredOf<R, F>,
		ParamsOf<R> & ParamsOfAll<F[keyof F]>
	>;
};

// ---- graph steps

/** A direction of a graph step: `->`, `<-` or `<->`. */
export type Direction = 'out' | 'in' | 'both';

const arrows: Readonly<Record<Direction, string>> = {
	out: '->',
	in: '<-',
	both: '<->',
};

type EdgeOf<S, N> = S extends { readonly edges: infer M }
	? N extends keyof M
		? M[N]
		: never
	: never;

type EndpointsOf<S, N, Side extends 'in' | 'out'> =
	EdgeOf<S, N> extends {
		readonly [K in Side]: ReadonlyArray<infer C extends Codec>;
	}
		? C
		: never;

/** The record ids a step in direction `D` along `N` starts from. */
export type StepStarts<S, N, D extends Direction> = D extends 'out'
	? EndpointsOf<S, N, 'in'>
	: D extends 'in'
		? EndpointsOf<S, N, 'out'>
		: EndpointsOf<S, N, 'in'> | EndpointsOf<S, N, 'out'>;

/**
 * The record ids a step in direction `D` along `N` yields. `<->` yields
 * both endpoints of every edge it walks, so the record it starts from too.
 */
export type StepEnds<S, N, D extends Direction> = D extends 'out'
	? EndpointsOf<S, N, 'out'>
	: D extends 'in'
		? EndpointsOf<S, N, 'in'>
		: EndpointsOf<S, N, 'in'> | EndpointsOf<S, N, 'out'>;

type EncodedOfCodec<C> = C extends Codec ? C['Encoded'] : never;

type Accepts<S, N, D extends Direction> = LinkTables<
	EncodedOfCodec<StepStarts<S, N, D>>
>;

type Enforced<S, N> =
	EdgeOf<S, N> extends { readonly enforced: infer B extends boolean }
		? B
		: false;

type Phrase<D extends Direction> = D extends 'out'
	? 'out of'
	: D extends 'in'
		? 'into'
		: 'at';

type IsList<E> = [Exclude<E, undefined>] extends [never]
	? false
	: [Exclude<E, undefined>] extends [ReadonlyArray<unknown>]
		? true
		: false;

/** Why a step along `N` cannot start from `R`, or `never`. */
export type StepProblem<S, R, N, D extends Direction> =
	IsList<EncodedOf<R>> extends true
		? R extends Traversal<Codec, Params, boolean, infer Edge>
			? [Edge] extends [never]
				? 'a graph step chains only from another step: from the records of a recursive path or the edges of a step SurrealDB yields one list per record'
				: StartProblem<S, EncodedOf<R>, N, D>
			: 'a graph step starts from one record or from another step: from a list of record links SurrealDB yields one list per link'
		: StartProblem<S, EncodedOf<R>, N, D>;

type StartProblem<S, E, N, D extends Direction> = [LinkTables<E>] extends [
	never,
]
	? 'a graph step starts from a record id: `row.id`, a record link, a binding of a record id, or another step'
	: string extends Accepts<S, N, D>
		? never
		: string extends LinkTables<E>
			? `this record can be of any table, and \`${N & string}\` has edges only ${Phrase<D>} \`${Accepts<S, N, D> & string}\``
			: [Exclude<LinkTables<E>, Accepts<S, N, D>>] extends [never]
				? never
				: `\`${N & string}\` has no edges ${Phrase<D>} \`${Exclude<LinkTables<E>, Accepts<S, N, D>> & string}\``;

/** `R`, when a step along `N` may start from it, or else a message saying why not. */
export type StepFrom<S, R, N, D extends Direction> = [
	StepProblem<S, R, N, D>,
] extends [never]
	? R
	: StepProblem<S, R, N, D>;

type ListOf<E, Item extends Codec> = [undefined] extends [E]
	? Schema.UndefinedOr<Schema.$Array<Item>>
	: Schema.$Array<Item>;

/** The codec of the list a step along `N` from `R` yields. */
export type StepCodec<S, R, N, D extends Direction> = ListOf<
	EncodedOf<R>,
	StepEnds<S, N, D>
>;

type EdgeTable<S, N> = TableIn<S, N>;

type EdgeIdOf<S, N> =
	EdgeTable<S, N> extends { readonly id: infer C extends Codec } ? C : never;

/** What a step along `N` from `R` yields. */
export type Step<S, R, N, D extends Direction, P extends Params> = Traversal<
	StepCodec<S, R, N, D>,
	ParamsOf<R> & P,
	Enforced<S, N>,
	ListOf<EncodedOf<R>, EdgeIdOf<S, N>>
>;

/** A step's options. */
export interface StepOptions<
	T extends PureTable,
	P extends Params,
	Target = never,
	Q extends Params = NoParams,
> {
	/**
	 * Walks only the edges that satisfy this condition, which reads the
	 * edge's fields: `->(member_of WHERE role = $role)->?`. A field path of
	 * another row would read the edge's field of that name, so it is
	 * refused when the step is built; read the row of the select the step
	 * sits directly under with `parent(row.field)`, or bind the value.
	 */
	readonly where?: (edge: RowExprs<T>) => Condition<P>;
	/**
	 * Keeps only the records reached that satisfy this condition, which
	 * reads their fields (those every table on that side has):
	 * `->member_of->(? WHERE name = $name)`. A field path of another row is
	 * refused, as in `where`, and so is `parent(row.field)`. The step's
	 * `edges` would include the edges to the records it leaves out, so
	 * {@link edges} refuses a step with this filter.
	 */
	readonly whereTarget?: (record: Target) => Condition<Q>;
}

/** The row object a step's `whereTarget` receives: the records it reaches. */
export type ReachedRow<S, N, D extends Direction> = RowOf<
	TargetFields<S, EncodedOfCodec<StepEnds<S, N, D>>>
>;

type EdgeRowTable<S, N> = Extract<EdgeTable<S, N>, PureTable>;

const accepted = (
	codecs: ReadonlyArray<Codec>,
): ReadonlyArray<string> | 'any' => {
	const tables = new Set<string>();
	for (const codec of codecs) {
		const shape = linkShape(codec);
		const names = shape === undefined ? 'any' : shapeTables(shape);
		if (names === 'any') return 'any';
		for (const name of names) tables.add(name);
	}
	return [...tables];
};

/** The start of a step, a recursive path or `edges`: one record or a list. */
const startOf = (
	from: Expr,
	what: string,
): { readonly shape: LinkShape; readonly optional: boolean } => {
	const shape = linkShape(codecOf(from));
	if (shape === undefined) {
		throw new TypeError(
			`${describeLink(from)} is not a record id, so ${what} cannot start from it`,
		);
	}
	const inner = optionalOf(shape) ? (shape as { of: LinkShape }).of : shape;
	if (inner.kind === 'list' && traversalOf(from)?.edges === undefined) {
		throw new TypeError(
			`${describeLink(from)} is a list of records from which SurrealDB yields one list per record; ${what} starts from one record or chains from another step`,
		);
	}
	return { shape, optional: optionalOf(shape) };
};

const step = (
	direction: Direction,
	store: GraphStore,
	from: Expr,
	name: string,
	options: StepOptions<PureTable, Params, unknown, Params> | undefined,
): Traversal<Codec, Params, boolean, Codec> => {
	const edge = store.edges[name];
	const table = store.tables[name];
	if (edge === undefined || table === undefined) {
		throw new RangeError(`\`${name}\` is not a relation of this store`);
	}
	const start = startOf(from, `a step along \`${name}\``);
	const sides =
		direction === 'out'
			? edge.in
			: direction === 'in'
				? edge.out
				: [...edge.in, ...edge.out];
	const allowed = accepted(sides);
	const tables = shapeTables(start.shape);
	if (allowed !== 'any') {
		const wrong =
			tables === 'any'
				? ['any table']
				: tables.filter((table) => !allowed.includes(table));
		if (wrong.length > 0) {
			throw new RangeError(
				`\`${name}\` has no edges ${
					direction === 'out'
						? 'out of'
						: direction === 'in'
							? 'into'
							: 'at'
				} ${wrong.map((table) => `\`${table}\``).join(', ')}`,
			);
		}
	}
	const scope = new RowScope('edge', name);
	const condition = options?.where?.(
		makeRow(table, scope) as RowExprs<PureTable>,
	);
	const filtered =
		condition === undefined
			? undefined
			: admit(
					{ own: scope, what: "a step's `where`", parents: 'place' },
					[condition],
				);
	const arrow = arrows[direction];
	const through =
		condition === undefined
			? escapeIdent(name)
			: `(${escapeIdent(name)} WHERE ${sqlOf(condition)})`;
	const edgesSql = `${operand(from)}${arrow}${through}`;
	const ends =
		direction === 'out'
			? edge.out
			: direction === 'in'
				? edge.in
				: [...edge.in, ...edge.out];
	const item = union(ends);
	const list = (codec: Codec): Codec =>
		start.optional ? orNone(Schema.Array(codec)) : Schema.Array(codec);
	const reached = options?.whereTarget;
	let test: Expr | undefined;
	let kept: Context | undefined;
	if (reached !== undefined) {
		const names = accepted(ends);
		if (names === 'any') {
			throw new TypeError(
				`the records \`${name}\` reaches can be of any table, so a filter on them has no fields to read`,
			);
		}
		const targets = names.map((target) => {
			const found = store.tables[target];
			if (found === undefined) {
				throw new RangeError(
					`\`${target}\` is not a table of this store`,
				);
			}
			return found;
		});
		const own = new RowScope('reached', names.join(' | '));
		test = reached(targetRowExprs(targets, own)) as Expr;
		kept = admit(
			{
				own,
				what: "a step's `whereTarget`",
				parents: 'refuse',
				refused:
					'it reads the select’s row only from a filter on the edges of a step directly under it',
			},
			[test],
		);
	}
	const params = mergeParams(
		paramsOf(from),
		condition === undefined ? {} : paramsOf(condition),
		test === undefined ? {} : paramsOf(test),
	);
	return {
		...makeExpr(
			list(item),
			`${edgesSql}${arrow}${test === undefined ? '?' : `(? WHERE ${sqlOf(test)})`}`,
			params,
			Level.atom,
			joinContexts([contextOf(from), filtered, kept, viewOf(store)]),
		),
		[TraversalTypeId]: {
			present: edge.enforced,
			item,
			edges: {
				sql: edgesSql,
				codec: list(table.id),
				item: table.id,
				...(test !== undefined && { filtered: true }),
			},
		},
	};
};

const makeRow = (table: PureTable, scope: RowScope): RowOf<Fields> =>
	targetRowExprs([table], scope);

type StepFunction<D extends Direction> = <
	S extends GraphStore,
	const R extends Expr,
	const N extends keyof S['edges'] & string,
	P extends Params = NoParams,
	Q extends Params = NoParams,
>(
	store: S,
	from: StepFrom<S, R, N, D>,
	edge: N,
	options?: StepOptions<EdgeRowTable<S, N>, P, ReachedRow<S, N, D>, Q>,
) => Step<S, R, N, D, P & Q>;

const stepIn =
	<D extends Direction>(direction: D): StepFunction<D> =>
	(store, from, edge, options) =>
		step(
			direction,
			store,
			from as Expr,
			edge,
			options as
				| StepOptions<PureTable, Params, unknown, Params>
				| undefined,
		) as never;

/**
 * `from->edge->?`: the records the relation `edge` leads to from `from`,
 * as a list of their ids. `from` is a record id (`row.id`, a record link, a
 * binding) of a table the relation accepts as its `in`, or another step,
 * whose records the engine walks one after another into one flat list.
 * The records are those on the relation's `out` side; read them with
 * {@link follow}, {@link rows} or {@link destructure}, and the edges
 * themselves with {@link edges}.
 *
 * @example
 * select(PersonTable).project((row) => ({
 *   teams: outgoing(store, row.id, 'member_of', {
 *     where: (edge) => eq(edge.role, 'lead'),
 *   }),
 * }));
 * // SELECT id->(member_of WHERE role = s"lead")->? AS teams FROM ⟨person⟩
 */
export const outgoing: StepFunction<'out'> = stepIn('out');

/**
 * `from<-edge<-?`: the records on the relation's `in` side whose edges
 * lead to `from`, a record of a table the relation accepts as its `out`.
 *
 * @example
 * select(TeamTable).value((row) => incoming(store, row.id, 'member_of'));
 * // SELECT VALUE id<-member_of<-? FROM ⟨team⟩
 */
export const incoming: StepFunction<'in'> = stepIn('in');

/**
 * `from<->edge<->?`: both endpoints of every edge of `edge` at `from`, in
 * either direction. SurrealDB yields both endpoints of each edge it walks,
 * so `from` itself is in the list once per edge.
 *
 * @example
 * select(PersonTable).value((row) => both(store, row.id, 'knows'));
 * // SELECT VALUE id<->knows<->? FROM ⟨person⟩
 */
export const both: StepFunction<'both'> = stepIn('both');

type EdgesCodec<R> =
	R extends Traversal<Codec, Params, boolean, infer E> ? E : never;

/**
 * The edges a step walks (`from->member_of`), as a list of their ids,
 * honouring the step's filter: read their fields with {@link follow}
 * (`id->member_of.role`) or {@link rows}.
 *
 * @example
 * select(PersonTable).project((row) => ({
 *   roles: follow(store, edges(outgoing(store, row.id, 'member_of'))).role,
 * }));
 * // SELECT id->member_of.role AS roles FROM ⟨person⟩
 */
export const edges = <const R extends Traversal<Codec, Params, boolean, Codec>>(
	traversal: [EdgesCodec<R>] extends [never]
		? 'only a graph step walks edges; a recursive path has none to read'
		: R,
): Traversal<EdgesCodec<R>, ParamsOf<R>, true> => {
	const data = traversalOf(traversal);
	if (data?.edges === undefined) {
		throw new TypeError(
			'only a graph step walks edges; a recursive path has none to read',
		);
	}
	if (data.edges.filtered === true) {
		throw new RangeError(
			'the edges of a step with a `whereTarget` filter would include the edges to the records it leaves out; filter the edges with `where` (`follow(store, edge.out)` reads the record an edge leads to)',
		);
	}
	return {
		...derive(data.edges.codec, data.edges.sql, Level.atom, [
			traversal as Expr,
		]),
		[TraversalTypeId]: { present: true, item: data.edges.item },
	} as unknown as Traversal<EdgesCodec<R>, ParamsOf<R>, true>;
};

// ---- recursive paths

type Element<V> =
	Exclude<V, undefined> extends ReadonlyArray<infer I>
		? Exclude<I, undefined>
		: Exclude<V, undefined>;

type NodeCodec<R> = Schema.Codec<
	Element<CodecOf<R>['Type']>,
	Element<EncodedOf<R>>,
	never,
	never
>;

/** The record a recursive step starts from, each time it is repeated. */
export type Node<R> = Expr<NodeCodec<R>, NoParams>;

type ItemOfTraversal<T> = CodecOf<T>['Encoded'] extends infer E
	? Exclude<E, undefined> extends ReadonlyArray<unknown>
		? CodecOf<T> extends Schema.$Array<infer I>
			? I
			: CodecOf<T> extends Schema.UndefinedOr<Schema.$Array<infer I>>
				? I
				: never
		: never
	: never;

/** Why recursion cannot start from `R`, or `never`. */
export type RecursionStart<R> =
	IsList<EncodedOf<R>> extends true
		? 'a recursive path starts from one record: `row.id`, a record link or a binding'
		: [LinkTables<EncodedOf<R>>] extends [never]
			? 'a recursive path starts from a record id: `row.id`, a record link or a binding'
			: R;

/** The step a recursive path repeats, when it leads back to the table it starts from. */
export type RecursionStep<R, T> =
	T extends Traversal<Codec, Params, boolean, Codec>
		? [
				Exclude<
					LinkTables<EncodedOfCodec<ItemOfTraversal<T>>>,
					LinkTables<EncodedOf<R>>
				>,
			] extends [never]
			? T
			: 'a recursive path repeats a step that leads back to the table it starts from'
		: 'a recursive path repeats a graph step from the node it is given, like `(node) => outgoing(store, node, "knows")`';

/** What a recursive path from `R` repeating `T` yields. */
export type RecursionResult<
	R,
	T,
	Optional extends boolean = false,
	Extra extends Params = NoParams,
> = Traversal<
	Optional extends true
		? Schema.UndefinedOr<Schema.$Array<AsCodec<ItemOfTraversal<T>>>>
		: ListOf<EncodedOf<R>, AsCodec<ItemOfTraversal<T>>>,
	ParamsOf<R> & ParamsOf<T> & Extra,
	PresentOf<T>
>;

/** A depth bound: an integer from 1 to 256 (the engine's limit). */
const bound = (value: number, what: string): number => {
	if (!Number.isInteger(value) || value < 1 || value > 256) {
		throw new RangeError(
			`${what} is ${value}; a recursion depth is an integer from 1 to 256`,
		);
	}
	return value;
};

const recursion = (
	from: Expr,
	repeat: (node: Expr) => unknown,
	instruction: (inner: Traversal<Codec>) => {
		readonly depth: string;
		/** An expression the depth reads (a shortest path's target). */
		readonly reads?: Expr;
		readonly optional?: boolean;
	},
): Traversal<Codec, Params, boolean> => {
	const start = startOf(from, 'a recursive path');
	const single =
		start.shape.kind === 'optional' ? start.shape.of : start.shape;
	if (single.kind === 'list') {
		throw new TypeError(
			'a recursive path starts from one record: `row.id`, a record link or a binding',
		);
	}
	// The node renders as nothing: the step text follows the depth.
	const node = makeExpr(codecOf(from), '', {}, Level.atom);
	const inner = repeat(node);
	const data = traversalOf(inner);
	if (data === undefined || !/^(?:->|<-)/u.test(sqlOf(inner as Expr))) {
		throw new TypeError(
			'a recursive path repeats a graph step from the node it is given, like `(node) => outgoing(store, node, "knows")`',
		);
	}
	const traversal = inner as Traversal<Codec>;
	const repeated = admit(
		{
			own: new RowScope('node', shapeTablesText(start.shape)),
			what: 'the step a recursive path repeats',
			parents: 'refuse',
			refused:
				'SurrealDB 3.3 and 4.0 bind `$parent` differently under a recursive path; bind the value with `param`',
		},
		[traversal],
	);
	const nodeTables = shapeTables(start.shape);
	const itemShape = linkShape(data.item);
	const itemTables = itemShape === undefined ? 'any' : shapeTables(itemShape);
	if (
		nodeTables !== 'any' &&
		(itemTables === 'any' ||
			itemTables.some((table) => !nodeTables.includes(table)))
	) {
		throw new RangeError(
			'a recursive path repeats a step that leads back to the table it starts from',
		);
	}
	const { depth, reads, optional } = instruction(traversal);
	const list = Schema.Array(data.item);
	return {
		...makeExpr(
			optional === true || start.optional ? orNone(list) : list,
			`${operand(from)}.{${depth}}(${sqlOf(traversal)})`,
			mergeParams(
				paramsOf(from),
				paramsOf(traversal),
				reads === undefined ? {} : paramsOf(reads),
			),
			Level.atom,
			joinContexts([
				contextOf(from),
				repeated,
				reads === undefined ? undefined : contextOf(reads),
			]),
		),
		[TraversalTypeId]: { present: data.present, item: data.item },
	};
};

/** A bounded depth range: at least `min` steps and at most `max`. */
export interface Depth {
	readonly min: number;
	readonly max: number;
}

/**
 * `from.{min..max}(step)`: repeats `step` from `from`, at least `min` and at
 * most `max` times, and yields the records the last repetition reached, as
 * one flat list (duplicates kept). SurrealDB stops early where a
 * repetition reaches no record, keeping the records of the last one that
 * did, and yields an empty list when fewer than `min` repetitions reach
 * any. `step` must lead back to the table `from` is of.
 *
 * @example
 * select(PersonTable).value((row) =>
 *   recurse(row.id, { min: 1, max: 3 }, (node) => outgoing(store, node, 'knows')),
 * );
 * // SELECT VALUE id.{1..3}(->knows->?) FROM ⟨person⟩
 */
export const recurse = <const R extends Expr, const T>(
	from: RecursionStart<R>,
	depth: Depth,
	repeat: (node: Node<R>) => RecursionStep<R, T>,
): RecursionResult<R, T> => {
	const min = bound(depth.min, 'the least depth');
	const max = bound(depth.max, 'the greatest depth');
	if (min > max) {
		throw new RangeError(
			`the least depth ${min} is greater than the greatest ${max}; SurrealDB would answer NONE`,
		);
	}
	return recursion(from as Expr, repeat as (node: Expr) => unknown, () => ({
		depth: `${min}..${max}`,
	})) as unknown as RecursionResult<R, T>;
};

/** Options of {@link collect}. */
export interface CollectOptions {
	/** The least number of steps before records are collected (default 1). */
	readonly min?: number;
	/** The greatest number of steps (default: until no record is reached, at most 256). */
	readonly max?: number;
	/** Collect `from` too (`+inclusive`). */
	readonly inclusive?: boolean;
}

/**
 * `from.{..+collect}(step)`: every distinct record `step` reaches when
 * repeated from `from`, once each, as one flat list (`+inclusive` adds
 * `from`). `step` must lead back to the table `from` is of.
 *
 * @example
 * select(PersonTable).value((row) =>
 *   collect(row.id, (node) => outgoing(store, node, 'knows')),
 * );
 * // SELECT VALUE id.{..+collect}(->knows->?) FROM ⟨person⟩
 */
export const collect = <const R extends Expr, const T>(
	from: RecursionStart<R>,
	repeat: (node: Node<R>) => RecursionStep<R, T>,
	options: CollectOptions = {},
): RecursionResult<R, T> => {
	const min =
		options.min === undefined
			? undefined
			: bound(options.min, 'the least depth');
	const max =
		options.max === undefined
			? undefined
			: bound(options.max, 'the greatest depth');
	if (min !== undefined && max !== undefined && min > max) {
		throw new RangeError(
			`the least depth ${min} is greater than the greatest ${max}`,
		);
	}
	const range = max === undefined ? `${min ?? ''}..` : `${min ?? 1}..${max}`;
	return recursion(from as Expr, repeat as (node: Expr) => unknown, () => ({
		depth: `${range}+collect${options.inclusive === true ? '+inclusive' : ''}`,
	})) as unknown as RecursionResult<R, T>;
};

/** Why `to` cannot be the target of a shortest path from `R` along `T`, or `never`. */
export type ShortestTarget<R, To> = [LinkTables<EncodedOf<To>>] extends [never]
	? 'a shortest path leads to a record id: a binding or a constant'
	: IsList<EncodedOf<To>> extends true
		? 'a shortest path leads to one record'
		: [
					Exclude<
						LinkTables<EncodedOf<To>>,
						LinkTables<EncodedOf<R>>
					>,
				] extends [never]
			? To
			: 'a shortest path leads to a record of the table it starts from';

/**
 * `from.{..+shortest=to}(step)`: the records of a shortest path from `from`
 * to `to` repeating `step`, `to` last and `from` left out unless
 * `inclusive`, or NONE when `to` cannot be reached.
 *
 * @example
 * select(PersonTable, param('from', PersonId)).only().value((row) =>
 *   shortest(row.id, param('to', PersonId), (node) => outgoing(store, node, 'knows')),
 * );
 * // SELECT VALUE id.{..+shortest=$to}(->knows->?) FROM ONLY $from
 */
export const shortest = <const R extends Expr, const To extends Expr, const T>(
	from: RecursionStart<R>,
	to: ShortestTarget<R, To>,
	repeat: (node: Node<R>) => RecursionStep<R, T>,
	options: { readonly inclusive?: boolean } = {},
): RecursionResult<R, T, true, ParamsOf<To>> => {
	const target = to as Expr;
	const shape = linkShape(codecOf(target));
	if (shape === undefined || shape.kind !== 'id') {
		throw new TypeError(
			'a shortest path leads to one record id: a binding or a constant',
		);
	}
	return recursion(from as Expr, repeat as (node: Expr) => unknown, () => ({
		depth: `..+shortest=${operand(target)}${options.inclusive === true ? '+inclusive' : ''}`,
		reads: target,
		optional: true,
	})) as unknown as RecursionResult<R, T, true, ParamsOf<To>>;
};
