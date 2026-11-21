/**
 * `CREATE`, `UPDATE`, `UPSERT`, `DELETE`, `RELATE`, `INSERT` and
 * `INSERT RELATION` from generated table descriptors.
 *
 * Data is bound, never inlined, and must fit the schema the descriptor
 * declares for the operation (`create`, `merge`, `content`). The result
 * codec follows from the target and the `RETURN` clause:
 *
 * - one record that the statement makes (`CREATE ONLY`, `RELATE ONLY`,
 *   `UPSERT ONLY $id`): the row;
 * - one record by id (`UPDATE ONLY $id`, `DELETE ONLY $id`): the row, or
 *   NONE when the record does not exist or the condition does not hold;
 * - a table, and every `INSERT`: an array of rows.
 *
 * `RETURN BEFORE` is NONE for a record the statement made (an `UPSERT` or
 * `INSERT` that created it), `RETURN AFTER` NONE for a deleted one, and
 * `RETURN DIFF` each record's JSON Patch ({@link Diff}).
 */
import { Schema, SchemaTransformation } from 'effect';
import { escapeIdent } from 'surrealdb';

import { SurrealValue } from '../value.js';
import {
	admit,
	assertReader,
	type Context,
	codecOf,
	contextOf,
	type Expr,
	isExpr,
	joinContexts,
	literal,
	mergeParams,
	type Operand,
	type OperandEncoded,
	operand,
	type ParamsOfAll,
	type Primitive,
	paramsOf,
	RowScope,
	type Site,
	settle,
	sqlOf,
} from './expr.js';
import {
	type CheckedProjection,
	type FieldsOf,
	fieldsOf,
	type ProjectedCodec,
	type Projection,
	type RowCodec,
	rowAt,
	rowCodec,
} from './row.js';
import {
	type Codec,
	make,
	type NoParams,
	type Params,
	type Simplify,
	type Statement,
} from './statement.js';
import {
	andAll,
	type Condition,
	type IdOf,
	type PureTable,
	project,
	type RowExprs,
	readerOf,
	rowExprs,
	type StoredRow,
	storedRow,
	tableName,
} from './table.js';

/** How many records a write addresses. */
export type Shape = 'one' | 'maybe' | 'many';

/** The result of a write returning rows of codec `Row`. */
export type WriteResult<Row extends Codec, S extends Shape> = S extends 'one'
	? Row
	: S extends 'maybe'
		? Schema.UndefinedOr<Row>
		: Schema.$Array<Row>;

/** The result of a write with `RETURN NONE`. */
export type NoResult<S extends Shape> = S extends 'many'
	? Schema.Tuple<readonly []>
	: Schema.Undefined;

/** The schema a table declares for an operation, or `never` without one. */
export type OperationOf<
	T extends PureTable,
	O extends 'create' | 'merge' | 'content',
> = T extends { readonly [K in O]: infer C extends Codec } ? C : never;

/** The top-level fields `S` sends that `Target` does not have. */
type ExtraFields<S, Target> = S extends object
	? Exclude<keyof S, keyof Target> & string
	: never;

/** The top-level fields an object of type `A` always has. */
type RequiredFields<A> = {
	[K in keyof A]-?: Record<never, never> extends Pick<A, K> ? never : K;
}[keyof A];

/** The top-level fields both send, where `S` sends what `Target` refuses. */
type WrongFields<S, Target> = {
	[K in keyof S & keyof Target]-?: [S[K]] extends [Target[K]] ? never : K;
}[keyof S & keyof Target] &
	string;

/**
 * Data of encoded type `S` for a schema of encoded type `Target`: `V` when
 * it fits, otherwise a message naming what does not, read in this order: a
 * value that is not an object, a field `Target` requires that `S` may leave
 * out, a field whose value `Target` refuses, any other mismatch, and a field
 * `Target` does not have.
 */
/** Whether `A` is a union. */
type IsUnion<A, Whole = A> = A extends unknown
	? [Whole] extends [A]
		? false
		: true
	: never;

/**
 * The fields `S` sends below `Path` that `Target` does not have, as paths
 * (`settings.notify.x`, `phones[*].x`), when both are objects or arrays.
 * It descends only where the two types differ and `Target` is not a union,
 * so it follows the target schema's own nesting and stops with it: at a
 * type `S` shares, at an open record (whose keys are all accepted), and at
 * a union (whose member is not known).
 */
type ExtraBelow<S, Target, Path extends string> = [Target] extends [S]
	? never
	: true extends IsUnion<Target>
		? never
		: Target extends ReadonlyArray<infer TargetItem>
			? S extends ReadonlyArray<infer Item>
				? ExtraBelow<
						NonNullable<Item>,
						NonNullable<TargetItem>,
						`${Path}[*]`
					>
				: never
			: [Target] extends [object]
				? S extends object
					?
							| `${Path}.${Exclude<keyof S, keyof Target> & string}`
							| NestedExtra<S, Target, `${Path}.`>
					: never
				: never;

/** {@link ExtraBelow} for every field `S` and `Target` share. */
type NestedExtra<S, Target, Prefix extends string> = {
	[K in keyof S & keyof Target & string]-?: ExtraBelow<
		NonNullable<S[K]>,
		NonNullable<Target[K]>,
		`${Prefix}${K}`
	>;
}[keyof S & keyof Target & string];

/**
 * Data of encoded type `S` for a schema of encoded type `Target`: `V` when
 * it fits, otherwise a message naming what does not, read in this order: a
 * value that is not an object, a field `Target` requires that `S` may leave
 * out, a field whose value `Target` refuses, any other mismatch, a field
 * `Target` does not have, and one inside a nested object or an array
 * element (SurrealDB 3.3 and 4.0 refuse an undeclared key of a declared
 * child object at any depth; 4.0 refuses one in an array element, which 3.3
 * stores and the element's closed schema then cannot read).
 */
type FitsEncoded<S, Target, V, Table extends string, Clause extends string> = [
	S,
] extends [Target]
	? [ExtraFields<S, Target>] extends [never]
		? [NestedExtra<S, Target, ''>] extends [never]
			? V
			: `\`${NestedExtra<S, Target, ''>}\` is not a field \`${Table}\` accepts in ${Clause}`
		: `\`${ExtraFields<S, Target>}\` is not a field \`${Table}\` accepts in ${Clause}`
	: [S] extends [object]
		? [Exclude<RequiredFields<Target>, RequiredFields<S>>] extends [never]
			? [WrongFields<S, Target>] extends [never]
				? `this value is not what \`${Table}\` accepts in ${Clause}`
				: `\`${WrongFields<S, Target>}\` is not a value \`${Table}\` accepts in ${Clause}`
			: `\`${Exclude<RequiredFields<Target>, RequiredFields<S>> & string}\` is required by \`${Table}\` in ${Clause}`
		: `this value is not an object, which \`${Table}\` takes in ${Clause}`;

/**
 * `V`, the data expression of codec `S`, when `S` may be bound as the data
 * of an operation whose schema is `Target`: what it sends is what the
 * schema sends, with no field the schema does not have (SurrealDB refuses
 * one, and assignability alone would let it through). An open schema (a
 * `FLEXIBLE` object, a schemaless table) accepts any field. Otherwise the
 * data's expected type is a message naming the first field that does not
 * fit, with the table and the clause (`CREATE`, `MERGE`, …).
 */
export type Fits<
	Target extends Codec,
	S extends Codec,
	V,
	Table extends string,
	Clause extends string,
> = [Target] extends [never]
	? `\`${Table}\` takes no data in ${Clause}`
	: FitsEncoded<S['Encoded'], Target['Encoded'], V, Table, Clause>;

/** A table that is a relation with declared endpoints. */
export type RelationTable = PureTable & {
	readonly edge: NonNullable<PureTable['edge']>;
};

/**
 * `V`, the endpoint expression of codec `S`, when its decoded id is one of
 * the relation's declared endpoint ids (brands included) or the relation
 * declares none; otherwise the expected type is `Message`.
 */
export type Endpoint<
	Accepted extends ReadonlyArray<Codec>,
	S extends Codec,
	V,
	Message extends string,
> = [Accepted[number]] extends [never]
	? V
	: [S['Type']] extends [Accepted[number]['Type']]
		? V
		: Message;

/**
 * One operation of a JSON Patch, as SurrealDB writes them for `RETURN DIFF`:
 * `add`, `remove`, `replace` (the path `""` and the whole record for a
 * created one, NONE for a deleted one) and `change`, whose value is a text
 * patch of a string. SurrealDB 3.3 and 4.0 write these four and no other
 * (`test/conformance.test.ts` pins them). Values are native SDK values.
 */
export const PatchOperation = Schema.Union([
	Schema.Struct({
		op: Schema.Literal('add'),
		path: Schema.String,
		value: SurrealValue,
	}),
	Schema.Struct({ op: Schema.Literal('remove'), path: Schema.String }),
	Schema.Struct({
		op: Schema.Literal('replace'),
		path: Schema.String,
		value: Schema.optional(SurrealValue),
	}),
	Schema.Struct({
		op: Schema.Literal('change'),
		path: Schema.String,
		value: Schema.String,
	}),
]);
export type PatchOperation = typeof PatchOperation.Type;

/** What `RETURN DIFF` yields for one record: its JSON Patch. */
export const Diff = Schema.Array(PatchOperation);
export type Diff = typeof Diff.Type;
/** The codec of {@link Diff}. */
export type DiffCodec = typeof Diff;

type Verb = 'CREATE' | 'UPDATE' | 'UPSERT' | 'DELETE' | 'RELATE' | 'INSERT';

/** What a `RETURN` clause asks for. */
type Mode = 'after' | 'before' | 'diff' | 'fields' | 'value';

interface State {
	readonly verb: Verb;
	readonly table: PureTable;
	readonly row: RowExprs<PureTable>;
	/** The text after `VERB [ONLY]` (after `INSERT [RELATION] INTO`). */
	readonly target: string;
	readonly shape: Shape;
	/** `INSERT RELATION`. */
	readonly relation?: boolean;
	readonly data: string | undefined;
	/** `ON DUPLICATE KEY UPDATE` assignments. */
	readonly duplicate?: string;
	readonly where: ReadonlyArray<Expr>;
	/** The `RETURN` clause and one record's codec, or `undefined` for NONE. */
	readonly returning:
		| {
				readonly mode: Mode;
				readonly sql: string;
				readonly codec: Codec;
		  }
		| undefined;
	readonly params: Params;
	/** The row the statement's callbacks read. */
	readonly scope: RowScope;
	/** What its expressions read besides their bindings. */
	readonly context?: Context;
}

/** Where a write's callbacks place their results. */
const siteOf = (state: State): Site => ({
	own: state.scope,
	what: `${/^[AEIOU]/u.test(state.verb) ? 'an' : 'a'} ${state.verb}`,
	parents: 'refuse',
	refused:
		"it reads the row of a select from a step's `where` directly under that select",
});

/** `state`, reading what the callback results `exprs` read too. */
const reading = (state: State, exprs: ReadonlyArray<Expr>): State => {
	const context = joinContexts([state.context, admit(siteOf(state), exprs)]);
	return {
		...state,
		...(context !== undefined && { context }),
	};
};

/**
 * The `RETURN` clause. An `UPSERT` says `RETURN VALUE NONE` for nothing:
 * with `RETURN NONE`, SurrealDB 3.3 treats the statement as if no record
 * were there. `UPSERT <record> … RETURN NONE` checks its data against the
 * schema as if the record did not exist, so a MERGE of some fields of an
 * existing record fails; `UPSERT <table> … WHERE … RETURN NONE` changes
 * the records the condition holds for and creates one more. `RETURN VALUE
 * NONE` does neither, on 3.3 or 4.0 (`test/conformance.test.ts` pins both).
 */
const returnClause = (state: State): string => {
	if (state.returning !== undefined) return `RETURN ${state.returning.sql}`;
	return state.verb === 'UPSERT' ? 'RETURN VALUE NONE' : 'RETURN NONE';
};

/**
 * What `UPSERT <table> … RETURN VALUE NONE` answers, one NONE for each
 * record it changed or created, read as the empty list `NoResult` types.
 */
const NoRows = Schema.Array(Schema.Undefined).pipe(
	Schema.decodeTo(
		Schema.Tuple([]),
		SchemaTransformation.transform<readonly [], ReadonlyArray<undefined>>({
			decode: () => [],
			encode: () => [],
		}),
	),
);

const render = (state: State): string =>
	[
		state.verb === 'INSERT'
			? `INSERT ${state.relation === true ? 'RELATION ' : ''}INTO ${state.target}`
			: `${state.verb} ${state.shape === 'many' ? '' : 'ONLY '}${state.target}`,
		...(state.data === undefined ? [] : [state.data]),
		...(state.duplicate === undefined
			? []
			: [`ON DUPLICATE KEY UPDATE ${state.duplicate}`]),
		...(state.where.length > 0 ? [`WHERE ${andAll(state.where)}`] : []),
		returnClause(state),
	].join(' ');

/** What `RETURN` yields for one record of the statement. */
const elementOf = (state: State): Codec => {
	const returning = state.returning;
	if (returning === undefined) return Schema.Undefined;
	switch (returning.mode) {
		case 'after':
			// A deleted record has no state after the statement.
			return state.verb === 'DELETE' ? Schema.Undefined : returning.codec;
		case 'before':
			// A record UPSERT or INSERT makes had no state before it.
			return state.verb === 'UPSERT' || state.verb === 'INSERT'
				? Schema.UndefinedOr(returning.codec)
				: returning.codec;
		default:
			return returning.codec;
	}
};

const resultOf = (state: State): Codec => {
	// `RETURN NONE` is an empty list for many records and NONE for one, as
	// `NoResult` types it.
	if (state.returning === undefined) {
		if (state.shape !== 'many') return Schema.Undefined;
		return state.verb === 'UPSERT' ? NoRows : Schema.Tuple([]);
	}
	const element = elementOf(state);
	if (state.shape === 'one') return element;
	return state.shape === 'maybe'
		? Schema.UndefinedOr(element)
		: Schema.Array(element);
};

/**
 * The result is a row (or a patch) whenever the statement applies, so NONE
 * or an empty list means the session may not make the write: `CREATE ONLY`
 * and `RELATE ONLY` returning the record, its fields or its patch, and an
 * `UPSERT` of one record returning the record or its fields. A patch of an
 * upserted record that did not change is empty, and a `RETURN VALUE` may be
 * NONE or an empty list itself, so neither is checked.
 */
const nonEmpty = (state: State): boolean => {
	const mode = state.returning?.mode;
	if (state.verb === 'CREATE' || state.verb === 'RELATE') {
		return mode === 'after' || mode === 'fields' || mode === 'diff';
	}
	return (
		state.verb === 'UPSERT' &&
		state.shape === 'one' &&
		(mode === 'after' || mode === 'fields')
	);
};

const finish = <P extends Params, R extends Codec>(
	state: State,
	name: string | undefined,
): Statement<Simplify<P>, R> => {
	const label =
		name ??
		`${state.verb.toLowerCase()}${state.relation === true ? ' relation' : ''} ${state.table.name}`;
	assertReader(
		readerOf(state.table),
		settle(state.context, `statement \`${label}\``),
		`statement \`${label}\``,
	);
	return make({
		name: label,
		sql: `${render(state)};`,
		params: state.params as Simplify<P>,
		result: resultOf(state) as R,
		writes: true,
		nonEmpty: nonEmpty(state),
	});
};

const start = (
	verb: Verb,
	table: PureTable,
	options: {
		readonly target: string;
		readonly shape: Shape;
		readonly params: Params;
		readonly returning: 'AFTER' | 'NONE';
		readonly relation?: boolean;
		/** The expressions the target reads: a record id, endpoints. */
		readonly subject?: ReadonlyArray<Expr>;
	},
): State => {
	const scope = new RowScope('write', table.name);
	const context = joinContexts((options.subject ?? []).map(contextOf));
	return {
		verb,
		table,
		scope,
		...(context !== undefined && { context }),
		row: rowExprs(table, scope),
		target: options.target,
		shape: options.shape,
		...(options.relation === true && { relation: true }),
		data: undefined,
		where: [],
		returning:
			options.returning === 'NONE'
				? undefined
				: { mode: 'after', sql: 'AFTER', codec: storedRow(table) },
		params: options.params,
	};
};

const withData = (
	state: State,
	keyword: 'CONTENT' | 'MERGE' | undefined,
	data: Expr,
): State => ({
	...reading(state, [data]),
	data: keyword === undefined ? operand(data) : `${keyword} ${operand(data)}`,
	params: mergeParams(state.params, paramsOf(data)),
});

const withWhere = (
	state: State,
	predicate: (row: RowExprs<PureTable>) => Expr,
): State => {
	const condition = predicate(state.row);
	return {
		...reading(state, [condition]),
		where: [...state.where, condition],
		params: mergeParams(state.params, paramsOf(condition)),
	};
};

/**
 * The row a `RETURN` projection reads: the record after the statement, or
 * for a `DELETE`, which leaves none, the record before it (`$before.…`).
 */
const returnedRow = (state: State): RowExprs<PureTable> =>
	state.verb === 'DELETE'
		? (rowAt(state.table.select, '$before.') as RowExprs<PureTable>)
		: state.row;

type Returning =
	| 'none'
	| 'after'
	| 'before'
	| 'diff'
	| ((row: never) => Projection);

/**
 * `M`, the `RETURN` mode a caller wrote, when the statement has it;
 * otherwise a message saying why not.
 */
export type ReturnMode<M, Allowed extends string, Verb extends string> = [
	M,
] extends [Allowed]
	? M
	: M extends 'before'
		? `${Verb} has no RETURN BEFORE: a record it makes had no state before`
		: `\`${M & string}\` is not a RETURN mode of ${Verb}`;

const withReturning = (state: State, returning: Returning): State => {
	if (returning === 'none') return { ...state, returning: undefined };
	if (returning === 'diff') {
		return {
			...state,
			returning: { mode: 'diff', sql: 'DIFF', codec: Diff },
		};
	}
	if (typeof returning === 'string') {
		return {
			...state,
			returning: {
				mode: returning,
				sql: returning.toUpperCase(),
				codec: storedRow(state.table),
			},
		};
	}
	const projection = (returning as (row: RowExprs<PureTable>) => Projection)(
		returnedRow(state),
	);
	const projected = project(projection);
	return {
		...reading(state, Object.values(projection)),
		returning: {
			mode: 'fields',
			sql: projected.sql,
			codec: projected.codec,
		},
		params: mergeParams(state.params, projected.params),
	};
};

const withValue = (
	state: State,
	pick: (row: RowExprs<PureTable>) => Expr,
): State => {
	const picked = pick(returnedRow(state));
	return {
		...reading(state, [picked]),
		returning: {
			mode: 'value',
			sql: `VALUE ${sqlOf(picked)}`,
			codec: rowCodec(codecOf(picked)),
		},
		params: mergeParams(state.params, paramsOf(picked)),
	};
};

/** A `CREATE ONLY` under construction; `R` is the codec of its result. */
export interface Create<
	T extends PureTable,
	P extends Params,
	R extends Codec,
> {
	/** `CONTENT`: the new record's data, as the table's create schema. */
	readonly content: <S extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'create'>,
			S,
			Expr<S, P2>,
			T['name'],
			'CREATE'
		>,
	) => Create<T, P & P2, R>;

	/**
	 * `RETURN NONE`, `RETURN AFTER` (the default), `RETURN DIFF` (the
	 * record's JSON Patch) or chosen expressions. `RETURN BEFORE` is refused:
	 * a created record had no state before.
	 */
	readonly returning: {
		<const M extends string>(
			mode: ReturnMode<M, 'none' | 'after' | 'diff', 'CREATE'>,
		): Create<
			T,
			P,
			M extends 'none'
				? Schema.Undefined
				: M extends 'after'
					? StoredRow<T>
					: DiffCodec
		>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Create<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>>;
	};

	/** `RETURN VALUE`: one expression of the created record. */
	readonly returningValue: <S extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<S, P2>,
	) => Create<T, P & P2, RowCodec<S>>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const creating = <T extends PureTable, P extends Params, R extends Codec>(
	state: State,
): Create<T, P, R> => ({
	content: (data) => creating(withData(state, 'CONTENT', data as Expr)),
	returning: ((returning: Returning) =>
		creating(withReturning(state, returning))) as unknown as Create<
		T,
		P,
		R
	>['returning'],
	returningValue: (pick) =>
		creating(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/**
 * `CREATE ONLY table`, or `CREATE ONLY $id` with a record id expression of
 * that table. Returns the created row unless told otherwise.
 */
export const create: {
	<T extends PureTable>(table: T): Create<T, NoParams, StoredRow<T>>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): Create<T, P, StoredRow<T>>;
} = <T extends PureTable>(table: T, id?: Expr) =>
	creating<T, Params, StoredRow<T>>(
		start('CREATE', table, {
			target: id === undefined ? tableName(table) : operand(id),
			shape: 'one',
			params: id === undefined ? {} : paramsOf(id),
			subject: id === undefined ? [] : [id],
			returning: 'AFTER',
		}),
	);

/** An `UPDATE` under construction; `R` is the codec of its result. */
export interface Update<
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Update<T, P & P2, R, S>;

	/**
	 * `RETURN NONE`, `AFTER` (the default), `BEFORE`, `DIFF` (each record's
	 * JSON Patch) or chosen expressions.
	 */
	readonly returning: {
		<const M extends string>(
			mode: ReturnMode<M, 'none' | 'after' | 'before' | 'diff', 'UPDATE'>,
		): Update<
			T,
			P,
			M extends 'none'
				? NoResult<S>
				: M extends 'diff'
					? WriteResult<DiffCodec, S>
					: WriteResult<StoredRow<T>, S>,
			S
		>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Update<
			T,
			P & ParamsOfAll<F[keyof F]>,
			WriteResult<ProjectedCodec<F>, S>,
			S
		>;
	};

	/** `RETURN VALUE`: one expression of each changed record. */
	readonly returningValue: <V extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<V, P2>,
	) => Update<T, P & P2, WriteResult<RowCodec<V>, S>, S>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const updating = <
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
>(
	state: State,
): Update<T, P, R, S> => ({
	where: (predicate) =>
		updating(
			withWhere(state, predicate as (row: RowExprs<PureTable>) => Expr),
		),
	returning: ((returning: Returning) =>
		updating(withReturning(state, returning))) as Update<
		T,
		P,
		R,
		S
	>['returning'],
	returningValue: (pick) =>
		updating(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/** An `UPDATE` that still needs its data. */
export interface UpdateTarget<
	T extends PureTable,
	P extends Params,
	S extends Shape,
> {
	/**
	 * `MERGE`: omitted fields keep their value; `undefined` (NONE) removes a
	 * removable field. The data is the table's merge schema.
	 */
	readonly merge: <D extends Codec, P2 extends Params>(
		data: Fits<OperationOf<T, 'merge'>, D, Expr<D, P2>, T['name'], 'MERGE'>,
	) => Update<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
	/** `CONTENT`: replaces the writable fields, as the table's content schema. */
	readonly content: <D extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'content'>,
			D,
			Expr<D, P2>,
			T['name'],
			'CONTENT'
		>,
	) => Update<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
}

/**
 * `UPDATE table … WHERE`, returning every row it changed, or
 * `UPDATE ONLY $id` with a record id expression, returning the row or NONE
 * when the record does not exist. `UPDATE` never creates a record.
 */
export const update: {
	<T extends PureTable>(table: T): UpdateTarget<T, NoParams, 'many'>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): UpdateTarget<T, P, 'maybe'>;
} = <T extends PureTable>(
	table: T,
	id?: Expr,
): UpdateTarget<T, Params, Shape> => {
	const state = start('UPDATE', table, {
		target: id === undefined ? tableName(table) : operand(id),
		shape: id === undefined ? 'many' : 'maybe',
		params: id === undefined ? {} : paramsOf(id),
		subject: id === undefined ? [] : [id],
		returning: 'AFTER',
	});
	return {
		merge: (data) => updating(withData(state, 'MERGE', data as Expr)),
		content: (data) => updating(withData(state, 'CONTENT', data as Expr)),
	};
};

/**
 * `V`, the data expression of codec `S`, when `S` fits both `First` and
 * `Second`: an `UPSERT` creates the record when it is missing and changes
 * it when it is there, so its data must be what both accept. Otherwise the
 * message of the first that refuses it.
 */
export type FitsBoth<
	First extends Codec,
	Second extends Codec,
	S extends Codec,
	V,
	Table extends string,
	Clause extends string,
> =
	Fits<First, S, V, Table, Clause> extends V
		? Fits<Second, S, V, Table, Clause>
		: Fits<First, S, V, Table, Clause>;

/** An `UPSERT` under construction; `R` is the codec of its result. */
export interface Upsert<
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends 'one' | 'many',
> {
	/**
	 * Adds a condition (an `UPSERT` of a table only): the records it holds
	 * for are changed, and one is created when none does.
	 */
	readonly where: S extends 'many'
		? <P2 extends Params>(
				predicate: (row: RowExprs<T>) => Condition<P2>,
			) => Upsert<T, P & P2, R, S>
		: (
				predicate: 'an UPSERT of one record creates or changes that record; it takes no condition',
			) => never;

	/**
	 * `RETURN NONE`, `AFTER` (the default), `BEFORE` (NONE for a record the
	 * statement created), `DIFF` or chosen expressions.
	 */
	readonly returning: {
		<const M extends string>(
			mode: ReturnMode<M, 'none' | 'after' | 'before' | 'diff', 'UPSERT'>,
		): Upsert<
			T,
			P,
			M extends 'none'
				? NoResult<S>
				: M extends 'diff'
					? WriteResult<DiffCodec, S>
					: M extends 'before'
						? WriteResult<Schema.UndefinedOr<StoredRow<T>>, S>
						: WriteResult<StoredRow<T>, S>,
			S
		>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Upsert<
			T,
			P & ParamsOfAll<F[keyof F]>,
			WriteResult<ProjectedCodec<F>, S>,
			S
		>;
	};

	/** `RETURN VALUE`: one expression of each record. */
	readonly returningValue: <V extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<V, P2>,
	) => Upsert<T, P & P2, WriteResult<RowCodec<V>, S>, S>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const upserting = <
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends 'one' | 'many',
>(
	state: State,
): Upsert<T, P, R, S> => ({
	where: ((predicate: (row: RowExprs<PureTable>) => Expr) =>
		upserting(withWhere(state, predicate))) as unknown as Upsert<
		T,
		P,
		R,
		S
	>['where'],
	returning: ((returning: Returning) =>
		upserting(withReturning(state, returning))) as Upsert<
		T,
		P,
		R,
		S
	>['returning'],
	returningValue: (pick) =>
		upserting(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/** An `UPSERT` that still needs its data. */
export interface UpsertTarget<
	T extends PureTable,
	P extends Params,
	S extends 'one' | 'many',
> {
	/**
	 * `MERGE`: the record's fields to set. The data must fit the table's
	 * create schema (the record may be created) and its merge schema (it
	 * may be changed).
	 */
	readonly merge: <D extends Codec, P2 extends Params>(
		data: FitsBoth<
			OperationOf<T, 'create'>,
			OperationOf<T, 'merge'>,
			D,
			Expr<D, P2>,
			T['name'],
			'UPSERT … MERGE'
		>,
	) => Upsert<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
	/**
	 * `CONTENT`: the record's writable fields, replacing them when it
	 * exists. The data must fit the table's create and content schemas.
	 */
	readonly content: <D extends Codec, P2 extends Params>(
		data: FitsBoth<
			OperationOf<T, 'create'>,
			OperationOf<T, 'content'>,
			D,
			Expr<D, P2>,
			T['name'],
			'UPSERT … CONTENT'
		>,
	) => Upsert<T, P & P2, WriteResult<StoredRow<T>, S>, S>;
}

/**
 * `UPSERT ONLY $id`: creates the record when it is missing and changes it
 * when it is there, returning it; or `UPSERT table … WHERE`, changing every
 * record the condition holds for, or creating one when none does, and
 * returning them all. Without a condition, `UPSERT table` creates a record.
 *
 * @example
 * const touch = upsert(CounterTable, recordId(CounterTable, param('key', SurrealSchema.String)))
 *   .merge(param('changes', CounterMerge))
 *   .build();
 */
export const upsert: {
	<T extends PureTable>(table: T): UpsertTarget<T, NoParams, 'many'>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): UpsertTarget<T, P, 'one'>;
} = <T extends PureTable>(
	table: T,
	id?: Expr,
): UpsertTarget<T, Params, 'one' | 'many'> => {
	const state = start('UPSERT', table, {
		target: id === undefined ? tableName(table) : operand(id),
		shape: id === undefined ? 'many' : 'one',
		params: id === undefined ? {} : paramsOf(id),
		subject: id === undefined ? [] : [id],
		returning: 'AFTER',
	});
	return {
		merge: (data) => upserting(withData(state, 'MERGE', data as Expr)),
		content: (data) => upserting(withData(state, 'CONTENT', data as Expr)),
	};
};

/** A `DELETE` under construction; `R` is the codec of its result. */
export interface Delete<
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
> {
	/** Adds a condition. Several `where` calls must all hold. */
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Delete<T, P & P2, R, S>;

	/**
	 * `RETURN NONE` (the default), `BEFORE` (the deleted rows), `AFTER` (NONE
	 * for each deleted record), `DIFF` (each record's JSON Patch) or chosen
	 * expressions of each record as it was (`$before.…`).
	 */
	readonly returning: {
		<const M extends string>(
			mode: ReturnMode<M, 'none' | 'after' | 'before' | 'diff', 'DELETE'>,
		): Delete<
			T,
			P,
			M extends 'none'
				? NoResult<S>
				: M extends 'diff'
					? WriteResult<DiffCodec, S>
					: M extends 'after'
						? WriteResult<Schema.Undefined, S>
						: WriteResult<StoredRow<T>, S>,
			S
		>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Delete<
			T,
			P & ParamsOfAll<F[keyof F]>,
			WriteResult<ProjectedCodec<F>, S>,
			S
		>;
	};

	/** `RETURN VALUE`: one expression of each deleted record as it was. */
	readonly returningValue: <V extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<V, P2>,
	) => Delete<T, P & P2, WriteResult<RowCodec<V>, S>, S>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const deleting = <
	T extends PureTable,
	P extends Params,
	R extends Codec,
	S extends Shape,
>(
	state: State,
): Delete<T, P, R, S> => ({
	where: (predicate) =>
		deleting(
			withWhere(state, predicate as (row: RowExprs<PureTable>) => Expr),
		),
	returning: ((returning: Returning) =>
		deleting(withReturning(state, returning))) as Delete<
		T,
		P,
		R,
		S
	>['returning'],
	returningValue: (pick) =>
		deleting(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/**
 * `DELETE table … WHERE`, or `DELETE ONLY $id` with a record id expression.
 * Returns nothing unless asked for the rows as they were (`'before'`).
 */
export const deleteFrom: {
	<T extends PureTable>(
		table: T,
	): Delete<T, NoParams, NoResult<'many'>, 'many'>;
	<T extends PureTable, P extends Params>(
		table: T,
		id: IdOf<T, P>,
	): Delete<T, P, NoResult<'maybe'>, 'maybe'>;
} = <T extends PureTable>(table: T, id?: Expr) =>
	deleting<T, Params, never, never>(
		start('DELETE', table, {
			target: id === undefined ? tableName(table) : operand(id),
			shape: id === undefined ? 'many' : 'maybe',
			params: id === undefined ? {} : paramsOf(id),
			subject: id === undefined ? [] : [id],
			returning: 'NONE',
		}),
	);

/** A `RELATE ONLY` under construction; `R` is the codec of its result. */
export interface Relate<
	T extends PureTable,
	P extends Params,
	R extends Codec,
> {
	/** `CONTENT`: the edge's own fields, as the table's create schema. */
	readonly content: <S extends Codec, P2 extends Params>(
		data: Fits<
			OperationOf<T, 'create'>,
			S,
			Expr<S, P2>,
			T['name'],
			'RELATE'
		>,
	) => Relate<T, P & P2, R>;

	/**
	 * `RETURN NONE`, `RETURN AFTER` (the default), `RETURN DIFF` or chosen
	 * expressions. `RETURN BEFORE` is refused: a new edge had no state
	 * before.
	 */
	readonly returning: {
		<const M extends string>(
			mode: ReturnMode<M, 'none' | 'after' | 'diff', 'RELATE'>,
		): Relate<
			T,
			P,
			M extends 'none'
				? Schema.Undefined
				: M extends 'after'
					? StoredRow<T>
					: DiffCodec
		>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Relate<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>>;
	};

	/** `RETURN VALUE`: one expression of the new edge. */
	readonly returningValue: <S extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<S, P2>,
	) => Relate<T, P & P2, RowCodec<S>>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const relating = <T extends PureTable, P extends Params, R extends Codec>(
	state: State,
): Relate<T, P, R> => ({
	content: (data) => relating(withData(state, 'CONTENT', data as Expr)),
	returning: ((returning: Returning) =>
		relating(withReturning(state, returning))) as unknown as Relate<
		T,
		P,
		R
	>['returning'],
	returningValue: (pick) =>
		relating(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/**
 * A `RELATE` endpoint: a binding as it is, anything else parenthesised,
 * since SurrealDB 3.3 and 4.0 parse only a binding or a record literal
 * there (`type::record(…)` needs the parentheses).
 */
const endpoint = (value: Expr): string => {
	const sql = sqlOf(value);
	return /^\$[A-Za-z_][A-Za-z0-9_]*$/u.test(sql) ? sql : `(${sql})`;
};

/**
 * `RELATE ONLY $from->table->$to`: one edge between two records. Each
 * endpoint must be a record id the relation declares for that side.
 */
export const relate = <
	T extends RelationTable,
	From extends Codec,
	PF extends Params,
	To extends Codec,
	PT extends Params,
>(
	table: T,
	from: Endpoint<
		T['edge']['in'],
		From,
		Expr<From, PF>,
		`this record id is not an \`in\` endpoint of \`${T['name']}\``
	>,
	to: Endpoint<
		T['edge']['out'],
		To,
		Expr<To, PT>,
		`this record id is not an \`out\` endpoint of \`${T['name']}\``
	>,
): Relate<T, PF & PT, StoredRow<T>> => {
	const source = from as Expr;
	const target = to as Expr;
	return relating(
		start('RELATE', table, {
			target: `${endpoint(source)}->${tableName(table)}->${endpoint(target)}`,
			shape: 'one',
			params: mergeParams(paramsOf(source), paramsOf(target)),
			subject: [source, target],
			returning: 'AFTER',
		}),
	);
};

/** The encoded create row of `T`, or none for a table that takes no data. */
type CreateEncoded<T extends PureTable> = [OperationOf<T, 'create'>] extends [
	never,
]
	? Record<never, never>
	: OperationOf<T, 'create'>['Encoded'];

/** What one row of `INSERT INTO table` may hold: the create row and an `id`. */
export type InsertRow<T extends PureTable> = CreateEncoded<T> & {
	readonly id?: T['id']['Encoded'];
};

/** What one row of `INSERT RELATION INTO table` holds: its endpoints too. */
export type InsertRelationRow<T extends RelationTable> = InsertRow<T> & {
	readonly in: T['edge']['in'][number]['Encoded'];
	readonly out: T['edge']['out'][number]['Encoded'];
};

/**
 * `V`, the data expression of codec `S`, when `S` is one row `Row` or an
 * array of them; otherwise a message naming what does not fit.
 */
export type FitsRows<
	Row,
	S extends Codec,
	V,
	Table extends string,
	Clause extends string,
> =
	S['Encoded'] extends ReadonlyArray<infer Item>
		? FitsEncoded<Item, Row, V, Table, Clause>
		: FitsEncoded<S['Encoded'], Row, V, Table, Clause>;

/** The fields of the table's merge schema an assignment may set. */
type MergeFieldsOf<T extends PureTable> = FieldsOf<OperationOf<T, 'merge'>>;

/**
 * Assignments of `ON DUPLICATE KEY UPDATE` as the callback returned them,
 * `F`, checked: each key a field the table's merge schema has, each value an
 * expression or constant whose stored type is what that field holds.
 */
export type CheckedAssignments<T extends PureTable, F> = {
	readonly [K in keyof F]: K extends keyof MergeFieldsOf<T>
		? F[K] extends Expr | Primitive
			? [OperandEncoded<F[K]>] extends [
					(MergeFieldsOf<T>[K] & Codec)['Encoded'],
				]
				? F[K]
				: `\`${K & string}\` does not hold this value`
			: 'an assignment is an expression: inline a constant with `literal`'
		: `\`${K & string}\` is not a field \`${T['name']}\` accepts in ON DUPLICATE KEY UPDATE`;
};

/** An `INSERT` under construction; `R` is the codec of its result. */
export interface Insert<
	T extends PureTable,
	P extends Params,
	R extends Codec,
> {
	/**
	 * `ON DUPLICATE KEY UPDATE`: when a row's id or a unique index already
	 * holds its values, set these fields of the existing record instead.
	 * `input` is the row the statement tried to insert (`$input.…`); a value
	 * may also be a binding or a constant. The stored record's own fields
	 * are not offered: SurrealDB 3.3 reads them as NONE there and 4.0 reads
	 * the record, so a statement reading them would mean two things.
	 */
	readonly onDuplicate: <const F>(
		set: (input: RowExprs<T>) => CheckedAssignments<T, F>,
	) => Insert<T, P & ParamsOfAll<F[keyof F]>, R>;

	/**
	 * `RETURN NONE`, `AFTER` (the default), `BEFORE` (NONE for each row the
	 * statement inserted, the record for one it updated), `DIFF` or chosen
	 * expressions.
	 */
	readonly returning: {
		(mode: 'none'): Insert<T, P, NoResult<'many'>>;
		(mode: 'after'): Insert<T, P, WriteResult<StoredRow<T>, 'many'>>;
		(
			mode: 'before',
		): Insert<T, P, WriteResult<Schema.UndefinedOr<StoredRow<T>>, 'many'>>;
		(mode: 'diff'): Insert<T, P, WriteResult<DiffCodec, 'many'>>;
		<const F>(
			fields: (row: RowExprs<T>) => CheckedProjection<F>,
		): Insert<
			T,
			P & ParamsOfAll<F[keyof F]>,
			WriteResult<ProjectedCodec<F>, 'many'>
		>;
	};

	/** `RETURN VALUE`: one expression of each record. */
	readonly returningValue: <V extends Codec, P2 extends Params>(
		pick: (row: RowExprs<T>) => Expr<V, P2>,
	) => Insert<T, P & P2, WriteResult<RowCodec<V>, 'many'>>;

	/** The finished statement. */
	readonly build: (name?: string) => Statement<Simplify<P>, R>;
}

const inserting = <T extends PureTable, P extends Params, R extends Codec>(
	state: State,
): Insert<T, P, R> => ({
	onDuplicate: (set) => {
		if (state.duplicate !== undefined) {
			throw new RangeError(
				'an INSERT has one ON DUPLICATE KEY UPDATE clause',
			);
		}
		const input = rowAt(
			state.table.create ?? state.table.select,
			'$input.',
		) as RowExprs<T>;
		const assignments = Object.entries(
			set(input) as Readonly<Record<string, Operand>>,
		);
		if (assignments.length === 0) {
			throw new RangeError(
				'ON DUPLICATE KEY UPDATE needs at least one assignment',
			);
		}
		const merge = state.table.merge;
		const settable = merge === undefined ? undefined : fieldsOf(merge);
		const values = assignments.map(([field, value]) => {
			if (settable !== undefined && !Object.hasOwn(settable, field)) {
				throw new RangeError(
					`\`${field}\` is not a field \`${state.table.name}\` accepts in ON DUPLICATE KEY UPDATE`,
				);
			}
			return [field, isExpr(value) ? value : literal(value)] as const;
		});
		return inserting({
			...reading(
				state,
				values.map(([, value]) => value),
			),
			duplicate: values
				.map(
					([field, value]) =>
						`${escapeIdent(field)} = ${operand(value)}`,
				)
				.join(', '),
			params: mergeParams(
				state.params,
				...values.map(([, value]) => paramsOf(value)),
			),
		});
	},
	returning: ((returning: Returning) =>
		inserting(withReturning(state, returning))) as unknown as Insert<
		T,
		P,
		R
	>['returning'],
	returningValue: (pick) =>
		inserting(withValue(state, pick as (row: RowExprs<PureTable>) => Expr)),
	build: (name) => finish(state, name),
});

/**
 * `INSERT INTO table $data`: one row, or an array of rows, each the table's
 * create row with an optional `id`. Returns every inserted row (an `INSERT`
 * always returns a list). A row whose id or unique index values are taken
 * fails the statement, unless `.onDuplicate(…)` says what to update.
 *
 * @example
 * const add = insert(PersonTable, param('people', Schema.Array(PersonCreate)))
 *   .onDuplicate((input) => ({ name: input.name }))
 *   .build();
 */
export const insert = <T extends PureTable, D extends Codec, P extends Params>(
	table: T,
	data: FitsRows<InsertRow<T>, D, Expr<D, P>, T['name'], 'INSERT'>,
): Insert<T, P, WriteResult<StoredRow<T>, 'many'>> =>
	inserting(
		withData(
			start('INSERT', table, {
				target: tableName(table),
				shape: 'many',
				params: {},
				returning: 'AFTER',
			}),
			undefined,
			data as Expr,
		),
	);

/**
 * `INSERT RELATION INTO table $data`: one edge, or an array of edges, each
 * with its `in` and `out` endpoints (ids the relation declares) and the
 * table's create row. Returns every inserted edge.
 */
export const insertRelation = <
	T extends RelationTable,
	D extends Codec,
	P extends Params,
>(
	table: T,
	data: FitsRows<
		InsertRelationRow<T>,
		D,
		Expr<D, P>,
		T['name'],
		'INSERT RELATION'
	>,
): Insert<T, P, WriteResult<StoredRow<T>, 'many'>> =>
	inserting(
		withData(
			start('INSERT', table, {
				target: tableName(table),
				shape: 'many',
				params: {},
				returning: 'AFTER',
				relation: true,
			}),
			undefined,
			data as Expr,
		),
	);
