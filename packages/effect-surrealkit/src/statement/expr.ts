/**
 * Expressions: SurrealQL fragments as plain values.
 *
 * An expression carries its static text, the codec of the value it yields
 * (in the native world, so a returned value decodes with it) and the codec of
 * every binding it reads. Combining expressions combines their bindings, so
 * a statement cannot read a binding it did not declare.
 */
import { Schema } from 'effect';
import {
	FileRef,
	Future,
	Geometry,
	Range,
	RecordId,
	RecordIdRange,
	DateTime as SdkDateTime,
	Decimal as SdkDecimal,
	Duration as SdkDuration,
	Uuid as SdkUuid,
	StringRecordId,
	Table,
	toSurqlString,
} from 'surrealdb';

import { Decimal, SurrealNumber } from '../decimal.js';
import { Int, int64 } from '../int.js';
import { isPlainObject } from '../internal/declare.js';
import { SurrealString, Uuid } from '../scalars.js';
import type { FunctionDescriptor } from '../table.js';
import { DateTime, Duration, dateTimeToString } from '../temporal.js';
import { assertNotNested, joinRefs, withRefs } from './refs.js';
import {
	assertBindingName,
	type Codec,
	type NoParams,
	type Params,
} from './statement.js';

export const ExprTypeId = '~effect-surrealkit/Expr';

interface ExprData<S extends Codec, P extends Params> {
	readonly codec: S;
	readonly sql: string;
	readonly params: P;
	/** How loosely the text binds, which decides where it needs parentheses. */
	readonly level: Level;
	/** Set when the text is a field path of the row. */
	readonly path?: PathData;
	/** What the text reads besides its bindings; absent when nothing. */
	readonly context?: Context;
}

/**
 * The row a row object's field paths read: a select's or a write's row, the
 * edge a step's `where` reads, the record a destructuring reads, or the node
 * a recursive path repeats its step from. A path's text is relative, so it
 * reads whichever row is current where it is placed; the scope says which
 * row it was written for.
 *
 * @internal
 */
export class RowScope {
	constructor(
		readonly kind: 'select' | 'write' | 'edge' | 'target' | 'node',
		readonly table: string,
	) {}
}

/**
 * Which row of each table a statement reads: the generated `select` row of
 * a privileged reader, or the `visible` row of a session whose field
 * permissions may hide fields (`readAs`).
 */
export type Reader = 'privileged' | 'restricted';

/**
 * What an expression reads besides its bindings, merged wherever expressions
 * are combined ({@link derive}) and checked where a callback's result is
 * placed ({@link admit}) and where a statement is built.
 *
 * @internal
 */
export interface Context {
	/** Each row whose field paths the text reads, with the first field read. */
	readonly reads?: ReadonlyMap<RowScope, string>;
	/** `parent(row.field)` reads not yet inside a step's `where`. */
	readonly loose?: ReadonlyMap<RowScope, string>;
	/** `parent(row.field)` reads inside a step's `where`, awaiting their select. */
	readonly placed?: ReadonlyMap<RowScope, string>;
	/** The readers of the store views the text reads records through. */
	readonly readers?: ReadonlySet<Reader>;
}

/**
 * Where a field path starts: the top-level field it reads, as the row
 * schema names it and as the text spells it.
 *
 * @internal
 */
export interface PathData {
	readonly root: string;
	readonly rootSql: string;
}

/**
 * Binding strength, loosest last: a field, binding, literal or call; a
 * comparison; a conjunction; a disjunction; raw text of unknown shape.
 *
 * @internal
 */
export const Level = {
	atom: 0,
	comparison: 1,
	and: 2,
	or: 3,
	raw: 4,
} as const;
/** @internal */
export type Level = (typeof Level)[keyof typeof Level];

/**
 * A SurrealQL expression yielding a value of codec `S`, reading the bindings
 * `P`. Its data sits under one key so that a row's nested fields can be
 * plain properties without ever colliding with it.
 */
export interface Expr<S extends Codec = Codec, P extends Params = Params> {
	readonly [ExprTypeId]: ExprData<S, P>;
}

/**
 * A field path of the row (`name`, `address.city`, `phones[0]`): what
 * `ORDER BY` accepts. The row's field expressions are paths, and so is an
 * element of one ({@link at}); nothing else is.
 */
export interface Path<S extends Codec = Codec> extends Expr<S, NoParams> {
	readonly [ExprTypeId]: ExprData<S, NoParams> & { readonly path: PathData };
}

/** The codec of the value an expression yields. */
export type CodecOf<E> = E extends Expr<infer S, Params> ? S : never;

/** The bindings an expression reads. */
export type ParamsOf<E> = E extends Expr<Codec, infer P> ? P : never;

type UnionToIntersection<U> = (
	U extends unknown
		? (value: U) => void
		: never
) extends (value: infer I) => void
	? I
	: never;

/** The bindings several expressions read together. */
export type ParamsOfAll<E> = [ParamsOf<E>] extends [never]
	? NoParams
	: UnionToIntersection<ParamsOf<E>> extends infer I extends Params
		? I
		: NoParams;

export const isExpr = (value: unknown): value is Expr =>
	typeof value === 'object' && value !== null && ExprTypeId in value;

/** Whether `value` is a field path of a row. */
export const isPath = (value: unknown): value is Path =>
	isExpr(value) && value[ExprTypeId].path !== undefined;

/** The codec of the value `expr` yields. */
export const codecOf = <S extends Codec>(expr: Expr<S, Params>): S =>
	expr[ExprTypeId].codec;

/** The SurrealQL text of `expr`. */
export const sqlOf = (expr: Expr): string => expr[ExprTypeId].sql;

/** The codec of each binding `expr` reads. */
export const paramsOf = <P extends Params>(expr: Expr<Codec, P>): P =>
	expr[ExprTypeId].params;

/**
 * An expression that combines no other: a binding, a constant, a local, raw
 * text. A combinator builds with {@link derive}, which carries what its
 * operands read.
 *
 * @internal
 */
export const makeExpr = <S extends Codec, P extends Params>(
	codec: S,
	sql: string,
	params: P,
	level: Level,
	context?: Context,
): Expr<S, P> => ({
	[ExprTypeId]: {
		codec,
		sql,
		params,
		level,
		...(context !== undefined && { context }),
	},
});

/** What `expr` reads besides its bindings. @internal */
export const contextOf = (expr: Expr): Context | undefined =>
	expr[ExprTypeId].context;

const joinMaps = <K, V>(
	maps: ReadonlyArray<ReadonlyMap<K, V> | undefined>,
): ReadonlyMap<K, V> | undefined => {
	let joined: Map<K, V> | undefined;
	let only: ReadonlyMap<K, V> | undefined;
	for (const map of maps) {
		if (map === undefined || map.size === 0) continue;
		if (only === undefined && joined === undefined) {
			only = map;
			continue;
		}
		joined ??= new Map(only);
		for (const [key, value] of map) {
			if (!joined.has(key)) joined.set(key, value);
		}
	}
	return joined ?? only;
};

const joinSets = <V>(
	sets: ReadonlyArray<ReadonlySet<V> | undefined>,
): ReadonlySet<V> | undefined => {
	const present = sets.filter(
		(set): set is ReadonlySet<V> => set !== undefined && set.size > 0,
	);
	if (present.length <= 1) return present[0];
	return new Set(present.flatMap((set) => [...set]));
};

/**
 * One context from several: what each reads, together.
 *
 * @internal
 */
export const joinContexts = (
	contexts: ReadonlyArray<Context | undefined>,
): Context | undefined => {
	const present = contexts.filter(
		(context): context is Context => context !== undefined,
	);
	if (present.length <= 1) return present[0];
	const reads = joinMaps(present.map((context) => context.reads));
	const loose = joinMaps(present.map((context) => context.loose));
	const placed = joinMaps(present.map((context) => context.placed));
	const readers = joinSets(present.map((context) => context.readers));
	return reads === undefined &&
		loose === undefined &&
		placed === undefined &&
		readers === undefined
		? undefined
		: {
				...(reads !== undefined && { reads }),
				...(loose !== undefined && { loose }),
				...(placed !== undefined && { placed }),
				...(readers !== undefined && { readers }),
			};
};

/**
 * An expression whose text combines `operands`: it reads their bindings
 * (`params`, when given, is those bindings with search references added)
 * and everything else they read. Every combinator builds with this, so none
 * can drop what an operand reads.
 *
 * @internal
 */
export const derive = <S extends Codec, P extends Params = Params>(
	codec: S,
	sql: string,
	level: Level,
	operands: ReadonlyArray<Expr>,
	params?: Params,
): Expr<S, P> =>
	makeExpr(
		codec,
		sql,
		(params ?? mergeParams(...operands.map(paramsOf))) as P,
		level,
		joinContexts(operands.map(contextOf)),
	);

/** A copy of `expr` reading `context` instead. @internal */
export const withContext = <E extends Expr>(
	expr: E,
	context: Context | undefined,
): E => {
	const { context: _, ...data } = expr[ExprTypeId];
	return {
		...expr,
		[ExprTypeId]: { ...data, ...(context !== undefined && { context }) },
	};
};

/** @internal */
export const makePath = <S extends Codec>(
	codec: S,
	sql: string,
	path: PathData,
	scope?: RowScope,
): Path<S> => ({
	[ExprTypeId]: {
		codec,
		sql,
		params: {},
		level: Level.atom,
		path,
		...(scope !== undefined && {
			context: { reads: new Map([[scope, path.root]]) },
		}),
	},
});

/** The path data of `expr`, when it is a field path. @internal */
export const pathOf = (expr: Expr): PathData | undefined =>
	expr[ExprTypeId].path;

// ---- scopes: which row a callback's field paths read

/** What a callback's own row is, for messages. */
const ownRow = (scope: RowScope): string => {
	switch (scope.kind) {
		case 'edge':
			return `the edge (\`${scope.table}\`)`;
		case 'target':
			return `the record it destructures (\`${scope.table}\`)`;
		case 'node':
			return 'each record the path reaches';
		default:
			return `the row of \`${scope.table}\``;
	}
};

const otherRow = (scope: RowScope): string => {
	switch (scope.kind) {
		case 'edge':
			return `the edge \`${scope.table}\` of a step`;
		case 'target':
			return `the record of \`${scope.table}\` a destructuring reads`;
		case 'node':
			return 'the node of a recursive path';
		case 'select':
			return `the row of a select of \`${scope.table}\``;
		default:
			return `the row of a write to \`${scope.table}\``;
	}
};

/**
 * Where a callback's result is placed, and what it may read there:
 *
 * - `own`: the row its field paths read (none for a block or a bare
 *   `RETURN`, which have no row);
 * - `what`: the callback, for messages;
 * - `parents`: what becomes of a `parent(row.field)`: a step's `where`
 *   places it (`place`), a select's callback keeps one its step placed for
 *   that select's own row (`select`), and anywhere else it is refused, for
 *   the reason `refused` gives.
 *
 * @internal
 */
export interface Site {
	readonly own: RowScope | undefined;
	readonly what: string;
	readonly parents: 'place' | 'select' | 'refuse';
	readonly refused?: string;
}

const outerAdvice = (site: Site, field: string, from: RowScope): string => {
	const via =
		from.kind === 'select'
			? ` To read the select's row from a step's \`where\` directly under it, use \`parent(row.${field})\`; anywhere else, bind`
			: ' Bind';
	switch (site.own?.kind) {
		case 'target':
			return `${via} the value with \`param\`, or project the outer field beside the destructured object.`;
		default:
			return `${via} the value with \`param\`.`;
	}
};

/**
 * Checks what a callback returned against the row it may read, and returns
 * what its result reads outside: every field path must read `site.own`
 * (bindings, constants and locals read no row and are always legal), and a
 * `parent(row.field)` is placed, kept or refused as `site.parents` says.
 *
 * @internal
 */
export const admit = (
	site: Site,
	exprs: ReadonlyArray<Expr>,
): Context | undefined => {
	const context = joinContexts(exprs.map(contextOf));
	if (context === undefined) return undefined;
	for (const [scope, field] of context.reads ?? []) {
		if (scope === site.own) continue;
		throw new RangeError(
			site.own === undefined
				? `\`${field}\` is a field of ${otherRow(scope)}, and ${site.what} has no row to read it from.${outerAdvice(site, field, scope)}`
				: `\`${field}\` is a field of ${otherRow(scope)}, but in ${site.what} a field path reads ${ownRow(site.own)}, so it would read that row's \`${field}\` instead.${outerAdvice(site, field, scope)}`,
		);
	}
	const loose = context.loose ?? new Map<RowScope, string>();
	const placed = context.placed ?? new Map<RowScope, string>();
	if (site.parents === 'place') {
		const [nested] = placed.values();
		if (nested !== undefined) {
			throw new RangeError(
				`\`parent(row.${nested})\` is in a step inside ${site.what}; \`parent\` reads the select's row only from a step directly under it`,
			);
		}
	} else {
		const [field] = loose.values();
		if (field !== undefined) {
			throw new RangeError(
				site.parents === 'select'
					? `\`parent(row.${field})\` reads the select's row from inside a step's \`where\` (\`outgoing(store, row.id, 'edge', { where: (edge) => eq(edge.${field}, parent(row.${field})) })\`); in ${site.what}, read \`row.${field}\` itself`
					: `\`parent(row.${field})\` is refused in ${site.what}: ${site.refused ?? 'it reads the row of a select only from a step directly under it'}`,
			);
		}
		for (const [scope, field] of placed) {
			if (site.parents === 'select' && scope === site.own) continue;
			throw new RangeError(
				site.parents === 'select'
					? `\`parent(row.${field})\` reads ${otherRow(scope)}, and this step is in ${site.what}; place the step directly under that select`
					: `\`parent(row.${field})\` is refused in ${site.what}: ${site.refused ?? 'it reads the row of a select only from a step directly under it'}`,
			);
		}
	}
	const reads = new Map(context.reads);
	if (site.own !== undefined) reads.delete(site.own);
	const kept = new Map(placed);
	if (site.parents === 'place') for (const entry of loose) kept.set(...entry);
	if (site.parents === 'select' && site.own !== undefined) {
		kept.delete(site.own);
	}
	return reads.size === 0 && kept.size === 0 && context.readers === undefined
		? undefined
		: {
				...(reads.size > 0 && { reads }),
				...(kept.size > 0 && { placed: kept }),
				...(context.readers !== undefined && {
					readers: context.readers,
				}),
			};
};

/**
 * Refuses a statement whose text still reads a row it has none of, or a
 * `parent(row.field)` no select took; returns the readers it reads through.
 *
 * @internal
 */
export const settle = (
	context: Context | undefined,
	what: string,
): ReadonlySet<Reader> | undefined => {
	if (context === undefined) return undefined;
	admit({ own: undefined, what, parents: 'refuse' }, [
		makeExpr(Schema.Unknown, '', {}, Level.atom, context),
	]);
	return context.readers;
};

/**
 * `$parent.field`: a field of the row of the select a step sits directly
 * under, read from the step's `where`, whose own field paths read the edge.
 * `field` is a field path of that select's row.
 *
 * It is accepted only there: SurrealDB 3.3 and 4.0 agree on `$parent` in a
 * step's `where` directly under a select's row (chained steps and a step
 * from a binding included), and disagree inside a destructuring (3.3 binds
 * the outer row, 4.0 the destructured record), under a recursive path and
 * under a bare `RETURN` or a block, so each of those refuses it when the
 * statement is built.
 *
 * @example
 * select(PersonTable).project((row) => ({
 *   peers: outgoing(store, row.id, 'knows', {
 *     where: (edge) => ne(edge.out, parent(row.id)),
 *   }),
 * }));
 * // SELECT id->(knows WHERE out != $parent.id)->? AS peers FROM ⟨person⟩
 */
export const parent = <const F extends Expr>(
	field: F extends Path
		? F
		: '`parent` takes a field path of a select’s row, like `parent(row.name)`',
): Expr<CodecOf<F>, NoParams> => {
	const data = isExpr(field) ? field[ExprTypeId] : undefined;
	const [scope] = data?.context?.reads?.keys() ?? [];
	if (data?.path === undefined || scope === undefined) {
		throw new TypeError(
			'`parent` takes a field path of a select’s row, like `parent(row.name)`',
		);
	}
	if (scope.kind !== 'select') {
		throw new TypeError(
			`\`parent(${data.sql})\` reads a field of ${otherRow(scope)}; \`parent\` reads only a select's row`,
		);
	}
	return makeExpr(
		data.codec as CodecOf<F>,
		`$parent.${data.sql}`,
		{},
		Level.atom,
		{ loose: new Map([[scope, data.path.root]]) },
	);
};

/**
 * The text of `expr` as an operand of a comparison, call or clause: anything
 * that is not a single term is parenthesised.
 *
 * @internal
 */
export const operand = (expr: Expr): string => {
	const { level, sql } = expr[ExprTypeId];
	return level === Level.atom ? sql : `(${sql})`;
};

/**
 * The text of `expr` as an operand of `AND` or `OR`. Comparisons and the
 * same connective need no parentheses; the other connective gets them even
 * where precedence would allow it, so no reader has to recall precedence.
 *
 * @internal
 */
export const conjunct = (
	expr: Expr,
	within: typeof Level.and | typeof Level.or,
): string => {
	const { level, sql } = expr[ExprTypeId];
	return level <= Level.comparison || level === within ? sql : `(${sql})`;
};

/**
 * One set of bindings from several. The same name may appear twice only with
 * the same codec: two codecs would encode one value two ways.
 *
 * @internal
 */
export const mergeParams = (...sets: ReadonlyArray<Params>): Params => {
	const merged: Record<string, Codec> = {};
	for (const set of sets) {
		for (const [name, codec] of Object.entries(set)) {
			const existing = merged[name];
			if (existing !== undefined && existing !== codec) {
				throw new RangeError(
					`binding \`$${name}\` is declared with two different codecs; declare it once and reuse the expression`,
				);
			}
			merged[name] = codec;
		}
	}
	return withRefs(merged, joinRefs(sets));
};

/**
 * A named binding. The value is supplied when the statement runs and is
 * encoded through `codec` first, so a value the codec refuses never reaches
 * the engine.
 */
export const param = <const Name extends string, S extends Codec>(
	name: Name,
	codec: S,
): Expr<S, { readonly [K in Name]: S }> => {
	assertBindingName(name);
	return makeExpr(
		codec,
		`$${name}`,
		{ [name]: codec } as { readonly [K in Name]: S },
		Level.atom,
	);
};

/** A value SurrealQL can spell as a literal without a codec. */
export type Primitive = string | number | boolean | bigint | null | undefined;

/** The codec of an inline primitive. */
export type LiteralCodec<V> = [V] extends [null]
	? Schema.Null
	: [V] extends [undefined]
		? Schema.Undefined
		: V extends string | number | boolean | bigint
			? Schema.Literal<V>
			: never;

const noLiteral = (what: string): RangeError =>
	new RangeError(`${what} has no SurrealQL literal; bind it with \`param\``);

const describe = (value: object): string => {
	const name = (value as { constructor?: { name?: unknown } }).constructor
		?.name;
	return typeof name === 'string' && name.length > 0
		? `a ${name}`
		: 'this value';
};

/** A string as a constant: well-formed, as the string codec requires. */
const stringLiteral = (value: string): string =>
	JSON.stringify(Schema.encodeUnknownSync(SurrealString)(value));

/**
 * Checks what a record id or range spells inside its `r"…"` text, which the
 * SDK formats: its strings and integers as their codecs require, and no
 * datetime, which the SDK formats wrongly before 1970.
 */
const assertKey = (value: unknown, seen: Set<object>): void => {
	if (typeof value === 'string') {
		stringLiteral(value);
	} else if (typeof value === 'bigint') {
		int64(value);
	} else if (typeof value === 'object' && value !== null) {
		if (seen.has(value)) throw noLiteral('a cyclic value');
		seen.add(value);
		if (value instanceof SdkDateTime) {
			throw new RangeError(
				'a record id or range whose key holds a datetime has no faithful literal (the SDK spells the datetime); bind it with `param`',
			);
		}
		if (value instanceof RecordId) assertKey(value.id, seen);
		else if (value instanceof Range || value instanceof RecordIdRange) {
			assertKey(value.begin?.value, seen);
			assertKey(value.end?.value, seen);
		} else if (Array.isArray(value)) {
			for (const item of value) assertKey(item, seen);
		} else if (isPlainObject(value)) {
			for (const [key, item] of Object.entries(value)) {
				stringLiteral(key);
				assertKey(item, seen);
			}
		}
		seen.delete(value);
	}
};

/**
 * The SurrealQL literal of an encoded constant. Every part is checked as
 * its codec checks a binding (a string must be well-formed, an integer an
 * int64), datetimes are written with {@link dateTimeToString}, and what has
 * no faithful literal (a `Set`, a `Map`, bytes, a class instance, a
 * non-finite number) is refused.
 */
const inline = (value: unknown, seen: Set<object>): string => {
	switch (typeof value) {
		case 'string':
			return `s${stringLiteral(value)}`;
		case 'number':
			if (!Number.isFinite(value)) {
				throw new RangeError(
					`${String(value)} has no portable SurrealQL literal; bind it with \`param\``,
				);
			}
			return toSurqlString(value);
		case 'bigint':
			return String(int64(value));
		case 'boolean':
			return String(value);
		case 'undefined':
			return 'NONE';
		case 'object':
			break;
		default:
			throw noLiteral(`a ${typeof value}`);
	}
	if (value === null) return 'NULL';
	if (value instanceof Set || value instanceof Map) {
		throw new RangeError(
			'a Set or Map has no SurrealQL literal; bind it with `param`',
		);
	}
	if (seen.has(value)) throw noLiteral('a cyclic value');
	seen.add(value);
	try {
		if (Array.isArray(value)) {
			return `[ ${value.map((item) => inline(item, seen)).join(', ')} ]`;
		}
		if (isPlainObject(value)) {
			const entries = Object.entries(value).map(
				([key, item]) => `${stringLiteral(key)}: ${inline(item, seen)}`,
			);
			return `{ ${entries.join(', ')} }`;
		}
		if (value instanceof SdkDateTime) {
			return `d${JSON.stringify(dateTimeToString(value))}`;
		}
		if (
			value instanceof RecordId ||
			value instanceof StringRecordId ||
			value instanceof RecordIdRange ||
			value instanceof Range
		) {
			if (value instanceof StringRecordId)
				stringLiteral(value.toString());
			else assertKey(value, new Set());
			return toSurqlString(value);
		}
		if (
			value instanceof SdkUuid ||
			value instanceof SdkDecimal ||
			value instanceof SdkDuration ||
			value instanceof Table ||
			value instanceof FileRef ||
			value instanceof Future ||
			value instanceof Geometry ||
			value instanceof Date
		) {
			return toSurqlString(value);
		}
		throw noLiteral(describe(value));
	} finally {
		seen.delete(value);
	}
};

const primitiveCodec = (value: Primitive): Codec => {
	if (value === null) return Schema.Null;
	if (value === undefined) return Schema.Undefined;
	return Schema.Literal(value);
};

/**
 * A constant written into the statement text. The constant is part of the
 * statement, not input: it is fixed when the statement is defined, and is
 * spelt as SurrealQL spells it (`s"…"`, `d"…"`, `r"…"`).
 *
 * With a codec, the value is encoded through it first, so SDK values
 * (`DateTime`, `RecordId`, `Duration`, …) and nested data can be inlined.
 * Every part is then checked as a binding of its kind would be: a string
 * must be well-formed and an integer must fit a signed 64-bit int. A
 * datetime is written with `dateTimeToString`. A `Set`, a `Map`, bytes and
 * other values without a faithful literal are refused.
 * Never pass request input here: bind it with {@link param}.
 */
export function literal<const V extends Primitive>(
	value: V,
): Expr<LiteralCodec<V>, NoParams>;
export function literal<S extends Codec>(
	value: S['Type'],
	codec: S,
): Expr<S, NoParams>;
export function literal(value: unknown, codec?: Codec): Expr<Codec, NoParams> {
	if (codec === undefined && typeof value === 'object' && value !== null) {
		throw new TypeError(
			'only a primitive can be inlined without a codec; pass the codec, or bind the value with `param`',
		);
	}
	const encoded =
		codec === undefined ? value : Schema.encodeUnknownSync(codec)(value);
	const sql = inline(encoded, new Set());
	return makeExpr(
		codec ?? primitiveCodec(value as Primitive),
		sql,
		{},
		Level.atom,
	);
}

/** An expression, or a primitive to inline with {@link literal}. */
export type Operand = Expr | Primitive;

/** The stored type an operand compares as. */
export type OperandEncoded<V> =
	V extends Expr<infer S, Params> ? S['Encoded'] : V;

/** The bindings an operand reads. */
export type OperandParams<V> = V extends Expr<Codec, infer P> ? P : NoParams;

const lift = (value: Operand): Expr => (isExpr(value) ? value : literal(value));

/**
 * Raw SurrealQL with a declared codec and bindings: the escape hatch for
 * what the typed functions do not cover yet. Nothing checks the text, its
 * bindings or its codec, and the text is used as written, so it must never
 * be built from input.
 */
export const unsafeExpr = <
	S extends Codec,
	P extends Params = NoParams,
>(options: {
	readonly sql: string;
	readonly codec: S;
	readonly params?: P;
	/** Set when the text needs no parentheses as an operand. */
	readonly atomic?: boolean;
}): Expr<S, P> => {
	const params = (options.params ?? {}) as P;
	for (const name of Object.keys(params)) assertBindingName(name);
	return makeExpr(
		options.codec,
		options.sql,
		params,
		options.atomic === true ? Level.atom : Level.raw,
	);
};

/**
 * The right operand `V`, when its stored type `B` compares with the left
 * operand's stored type `A`: one is assignable to the other. Refinements
 * and brands sit on the decoded type, so a branded or narrowed binding
 * still compares with its column, while a string never compares with an
 * int. Otherwise the operand's expected type is a message saying so.
 */
export type Comparable<A, B, V> = [A] extends [B]
	? V
	: [B] extends [A]
		? V
		: 'these operands never compare: their stored types do not overlap';

type BooleanCodec = Schema.Codec<unknown, boolean, never, never>;

const binary =
	(operator: string) =>
	<A extends Codec, PA extends Params, const B extends Operand>(
		left: Expr<A, PA>,
		right: Comparable<A['Encoded'], OperandEncoded<B>, B>,
	): Expr<Schema.Boolean, PA & OperandParams<B>> => {
		const other = lift(right as Operand);
		return derive(
			Schema.Boolean,
			`${operand(left)} ${operator} ${operand(other)}`,
			Level.comparison,
			[left, other],
		);
	};

/** `left = right` */
export const eq = binary('=');
/** `left != right` */
export const ne = binary('!=');
/** `left < right` */
export const lt = binary('<');
/** `left <= right` */
export const lte = binary('<=');
/** `left > right` */
export const gt = binary('>');
/** `left >= right` */
export const gte = binary('>=');

const logical =
	(operator: 'AND' | 'OR') =>
	<
		const A extends readonly [
			Expr<BooleanCodec, Params>,
			...ReadonlyArray<Expr<BooleanCodec, Params>>,
		],
	>(
		...operands: A
	): Expr<Schema.Boolean, ParamsOfAll<A[number]>> => {
		const level = operator === 'AND' ? Level.and : Level.or;
		if (operator === 'OR') assertNotNested(operands.map(paramsOf), 'OR');
		return derive(
			Schema.Boolean,
			operands.map((item) => conjunct(item, level)).join(` ${operator} `),
			operands.length === 1 ? operands[0][ExprTypeId].level : level,
			operands,
		);
	};

/** Every operand holds. */
export const and = logical('AND');
/** At least one operand holds. */
export const or = logical('OR');

/** `!value` */
export const not = <P extends Params>(
	value: Expr<BooleanCodec, P>,
): Expr<Schema.Boolean, P> => {
	assertNotNested([paramsOf(value)], 'NOT');
	return derive(Schema.Boolean, `!${operand(value)}`, Level.atom, [value]);
};

/** `value = NONE`: the field is absent, or the expression yields nothing. */
export const isNone = <P extends Params>(
	value: Expr<Codec, P>,
): Expr<Schema.Boolean, P> =>
	derive(Schema.Boolean, `${operand(value)} = NONE`, Level.comparison, [
		value,
	]);

/** `value != NONE` */
export const isNotNone = <P extends Params>(
	value: Expr<Codec, P>,
): Expr<Schema.Boolean, P> =>
	derive(Schema.Boolean, `${operand(value)} != NONE`, Level.comparison, [
		value,
	]);

/** The element type of a stored array or set. */
export type ElementOf<E> =
	E extends ReadonlyArray<infer I>
		? I
		: E extends ReadonlySet<infer I>
			? I
			: never;

type ListItem = string | number | boolean | bigint | null;

/** An array or set expression, or primitives to inline as an array. */
export type Collection = Expr | ReadonlyArray<ListItem>;

type CollectionEncoded<V> = V extends Expr<infer S, Params> ? S['Encoded'] : V;

/** An inline list: every item goes through its own {@link literal}. */
const liftCollection = (value: Collection): Expr =>
	isExpr(value)
		? value
		: makeExpr(
				Schema.Array(Schema.Unknown),
				`[ ${value.map((item) => sqlOf(literal(item))).join(', ')} ]`,
				{},
				Level.atom,
			);

/** `needle INSIDE haystack`: the value is an element of the array or set. */
export const inside = <
	A extends Codec,
	PA extends Params,
	const B extends Collection,
>(
	needle: Expr<A, PA>,
	haystack: Comparable<A['Encoded'], ElementOf<CollectionEncoded<B>>, B>,
): Expr<Schema.Boolean, PA & OperandParams<B>> => {
	const collection = liftCollection(haystack as Collection);
	return derive(
		Schema.Boolean,
		`${operand(needle)} INSIDE ${operand(collection)}`,
		Level.comparison,
		[needle, collection],
	);
};

/** `haystack CONTAINS needle`: the array or set holds the value. */
export const contains = <
	A extends Codec,
	PA extends Params,
	const B extends Operand,
>(
	haystack: Expr<A, PA>,
	needle: Comparable<ElementOf<A['Encoded']>, OperandEncoded<B>, B>,
): Expr<Schema.Boolean, PA & OperandParams<B>> => {
	const value = lift(needle as Operand);
	return derive(
		Schema.Boolean,
		`${operand(haystack)} CONTAINS ${operand(value)}`,
		Level.comparison,
		[haystack, value],
	);
};

/** The codec an array's element decodes with. */
export type ItemOf<S extends Codec> =
	S['Encoded'] extends ReadonlyArray<unknown>
		? S extends { readonly value: infer I extends Codec }
			? I
			: never
		: never;

/** A codec that also accepts NONE, unless it already does. */
export type OrNone<S extends Codec> = undefined extends S['Type']
	? S
	: Schema.UndefinedOr<S>;

/** @internal */
export const orNone = <S extends Codec>(codec: S): OrNone<S> =>
	(Schema.is(codec)(undefined)
		? codec
		: Schema.UndefinedOr(codec)) as OrNone<S>;

type ArrayOnly<S extends Codec> = [ItemOf<S>] extends [never]
	? {
			readonly 'this expression is not an array, so it has no elements to index': S['Encoded'];
		}
	: unknown;

/**
 * `array[index]`: one element, or NONE past the end. A negative index is
 * not an offset from the end in SurrealQL, so it is refused. An element of
 * a field path is a field path.
 */
export function at<S extends Codec>(
	array: Path<S> & ArrayOnly<S>,
	index: number,
): Path<OrNone<ItemOf<S>>>;
export function at<S extends Codec, P extends Params>(
	array: Expr<S, P> & ArrayOnly<S>,
	index: number,
): Expr<OrNone<ItemOf<S>>, P>;
export function at(array: Expr, index: number): Expr {
	if (!Number.isInteger(index) || index < 0) {
		throw new RangeError(
			`${index} is not an array index: use a non-negative integer`,
		);
	}
	const codec: Codec = codecOf(array);
	if (!('value' in codec) || !Schema.isSchema(codec.value)) {
		throw new TypeError(
			`\`${sqlOf(array)}\` is not an array expression, so it has no elements to index`,
		);
	}
	const element = orNone(codec.value as Codec);
	const sql = `${operand(array)}[${index}]`;
	const path = pathOf(array);
	return path === undefined
		? derive(element, sql, Level.atom, [array])
		: withContext(makePath(element, sql, path), contextOf(array));
}

const casts = {
	bool: Schema.Boolean,
	datetime: DateTime,
	decimal: Decimal,
	duration: Duration,
	float: Schema.Number,
	int: Int,
	number: SurrealNumber,
	string: SurrealString,
	uuid: Uuid,
} as const;

/** The kinds {@link cast} converts to, and the codec each yields. */
export type CastKind = keyof typeof casts;

/**
 * `<kind>value`: SurrealQL's cast. The engine fails the statement when the
 * value cannot be converted.
 */
export const cast = <K extends CastKind, P extends Params>(
	kind: K,
	value: Expr<Codec, P>,
): Expr<(typeof casts)[K], P> =>
	derive(casts[kind], `<${kind}>${operand(value)}`, Level.atom, [value]);

/** A function descriptor whose codecs need no services. */
export type PureFunction = FunctionDescriptor & {
	readonly args: Codec;
	readonly returns: Codec;
};

type ArgumentOf<E> =
	| Expr<Schema.Codec<unknown, E, never, never>, Params>
	| Extract<E, Primitive>;

type ArgumentsOf<T extends ReadonlyArray<unknown>> = {
	readonly [I in keyof T]: ArgumentOf<T[I]>;
};

/** The arguments a generated function accepts, as expressions. */
export type CallArguments<F extends PureFunction> =
	F['args']['Encoded'] extends infer T extends ReadonlyArray<unknown>
		? ArgumentsOf<T>
		: never;

type Ordinals = readonly [
	'first',
	'second',
	'third',
	'fourth',
	'fifth',
	'sixth',
	'seventh',
	'eighth',
	'ninth',
	'tenth',
];

type OrdinalOf<I> = I extends keyof Ordinals ? Ordinals[I] : 'next';

/**
 * The function `F`, or a message when `A` leaves out an argument it needs.
 * An argument past the last is refused where it is ({@link CheckedArguments}).
 */
export type CallTarget<
	F extends PureFunction,
	A extends ReadonlyArray<unknown>,
> = A['length'] extends CallArguments<F>['length']
	? F
	: [Exclude<keyof A & `${number}`, keyof CallArguments<F>>] extends [never]
		? `\`${F['name']}\` needs more than ${A['length']} ${A['length'] extends 1 ? 'argument' : 'arguments'}`
		: F;

/**
 * The arguments `A` as the caller wrote them, each checked against the
 * parameter in its position: one that does not fit, or one past the last
 * parameter, is typed as a message naming the position.
 */
export type CheckedArguments<
	F extends PureFunction,
	A extends ReadonlyArray<unknown>,
> = {
	readonly [I in keyof A]: I extends keyof CallArguments<F>
		? A[I] extends CallArguments<F>[I]
			? A[I]
			: `\`${F['name']}\` does not accept this as its ${OrdinalOf<I>} argument`
		: `\`${F['name']}\` takes no ${OrdinalOf<I>} argument`;
};

/**
 * Calls a `DEFINE FUNCTION` through its generated descriptor: the arguments
 * are checked against the declared parameter kinds, trailing optional ones
 * may be left out, and the result decodes with the declared return kind.
 */
export const call = <
	F extends PureFunction,
	const A extends ReadonlyArray<unknown>,
>(
	fn: CallTarget<F, A>,
	...args: CheckedArguments<F, A>
): Expr<F['returns'], ParamsOfAll<A[number]>> => {
	const values = (args as ReadonlyArray<Operand>).map(lift);
	const target = fn as F;
	return derive(
		target.returns as F['returns'],
		`${target.name}(${values.map(sqlOf).join(', ')})`,
		Level.atom,
		values,
	);
};
