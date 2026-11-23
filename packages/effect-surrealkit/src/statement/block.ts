/**
 * Blocks: a multi-step script (named locals, guards that `THROW`, branches
 * that return) written in TypeScript and built into an ordinary
 * {@link Statement}.
 *
 * A block renders to static SurrealQL: every value still enters as a named
 * binding, and a local is a `LET` whose value is an expression or a built
 * statement. The whole block is one SurrealQL statement, `{ … }`, whose
 * value is its last expression: a value, or one `IF … ELSE IF … ELSE` whose
 * branches end with theirs. SurrealDB 3.3 and 4.0 agree on what that buys
 * (`test/conformance.test.ts`):
 *
 * - a guard's `THROW` stops the rest of the block and undoes what the block
 *   wrote, even outside a transaction, where a `THROW` in a statement of its
 *   own would let the statements after it run;
 * - its locals are local to it, so it defines nothing in the scope the
 *   statements of a transaction share;
 * - it never writes `RETURN`, which inside an `IF` ends a transaction on
 *   SurrealDB 3 without the SDK seeing it.
 *
 * Like the other builders, this module does not import the analyzer; the
 * test suite checks every block it builds against the analyzer instead.
 */
import { Schema, SchemaAST } from 'effect';
import { escapeIdent } from 'surrealdb';

import { isPlainObject } from '../internal/declare.js';
import {
	admit,
	derive,
	type Expr,
	ExprTypeId,
	isExpr,
	Level,
	type LiteralCodec,
	literal,
	makeExpr,
	mergeParams,
	type OrNone,
	operand,
	orNone,
	type ParamsOfAll,
	type Primitive,
	paramsOf,
	sqlOf,
} from './expr.js';
import {
	type FieldsOf,
	type ProjectedCodec,
	projectedCodec,
	type RowCodecIn,
} from './row.js';
import {
	type AnyStatement,
	assertBindingName,
	type Codec,
	isStatement,
	make,
	type NoParams,
	type Params,
	type Result,
	type Simplify,
	type Statement,
} from './statement.js';
import type { Condition } from './table.js';

// ---- locals as expressions

/** The codec of each local a block has defined, by name. */
export type Locals = { readonly [name: string]: Codec };

/** No locals. */
export type NoLocals = Record<never, never>;

/** The codec `S` without its NONE member, when it has one. */
type NoneMember<S> = S extends {
	readonly members: readonly [infer X extends Codec, Schema.Undefined];
}
	? X
	: never;

/** `S` without NONE: what a local holds once a guard has refused NONE. */
export type SomeOf<S extends Codec> = [NoneMember<S>] extends [never]
	? S
	: NoneMember<S>;

/**
 * The row schema behind `S`: a struct, or the struct a stored-row or
 * projected-row codec wraps (which decodes to the same type).
 */
type RowIn<S> = S extends RowCodecIn
	? S
	: S extends {
				readonly '~type.parameters': readonly [
					infer I extends RowCodecIn,
				];
				readonly Type: infer T;
			}
		? [T] extends [I['Type']]
			? I
			: never
		: never;

type IsOptional<S> = S extends { readonly '~type.optionality': 'optional' }
	? true
	: false;

type Unwrapped<S> = S extends { readonly schema: infer I extends Codec }
	? S extends { readonly '~type.optionality': 'optional' }
		? I
		: S
	: S;

type FieldsIn<S, Optional extends boolean> = [RowIn<S>] extends [never]
	? unknown
	: LocalRow<FieldsOf<RowIn<S>>, Optional>;

type FieldsOfValue<S, Optional extends boolean> = [NoneMember<S>] extends [
	never,
]
	? FieldsIn<S, Optional>
	: FieldsIn<NoneMember<S>, true>;

/** A field of a local row: `$name.field`, NONE when the row may be. */
export type LocalField<S extends Codec, Optional extends boolean> = Expr<
	Optional extends true ? OrNone<S> : S,
	NoParams
> &
	FieldsOfValue<S, Optional>;

type LocalRow<F, Optional extends boolean> = {
	readonly [K in keyof F & string]: LocalField<
		Unwrapped<F[K]> & Codec,
		Optional extends true ? true : IsOptional<F[K]>
	>;
};

/**
 * A local as an expression: `$name`, and, when it holds a row (a struct, a
 * stored row or a projection), one expression per field, NONE wherever the
 * row may be NONE.
 */
export type Local<S extends Codec> = Expr<S, NoParams> &
	FieldsOfValue<S, false>;

/** One expression per local: what a step's callback receives. */
export type Scope<L extends Locals> = {
	readonly [K in keyof L]: Local<L[K]>;
};

/** The locals `L`, with `N` holding {@link SomeOf} its value. */
export type Narrowed<L extends Locals, N extends keyof L> = {
	readonly [K in keyof L]: K extends N ? SomeOf<L[K]> : L[K];
};

const noneMember = (codec: Codec): Codec | undefined => {
	const { members } = codec as { readonly members?: unknown };
	return Array.isArray(members) &&
		members.length === 2 &&
		members[1] === Schema.Undefined &&
		Schema.isSchema(members[0])
		? (members[0] as Codec)
		: undefined;
};

const someOf = (codec: Codec): Codec => noneMember(codec) ?? codec;

const rowWrappers: ReadonlySet<string> = new Set([
	'effect-surrealkit/NoneAsAbsent',
	'effect-surrealkit/ProjectedRow',
]);

const representationOf = (ast: SchemaAST.AST): string | undefined => {
	const { representation } = (ast.annotations ?? {}) as {
		readonly representation?: { readonly id?: unknown };
	};
	return typeof representation?.id === 'string'
		? representation.id
		: undefined;
};

/** The struct a codec decodes with, through the row wrappers. */
const objectsOf = (ast: SchemaAST.AST): SchemaAST.Objects | undefined => {
	let at = ast;
	while (
		SchemaAST.isDeclaration(at) &&
		at.typeParameters.length === 1 &&
		rowWrappers.has(representationOf(at) ?? '')
	) {
		at = at.typeParameters[0] as SchemaAST.AST;
	}
	return SchemaAST.isObjects(at) ? at : undefined;
};

/** `expr`, with one expression per field when `codec` decodes a row. */
const withFields = (
	expr: Expr,
	codec: Codec,
	sql: string,
	optional: boolean,
): Expr => {
	const none = noneMember(codec);
	const objects = objectsOf((none ?? codec).ast);
	if (objects === undefined) return expr;
	const mayBeNone = optional || none !== undefined;
	const fields: Record<string, Expr> = {};
	for (const property of objects.propertySignatures) {
		if (typeof property.name !== 'string') continue;
		const fieldOptional = mayBeNone || SchemaAST.isOptional(property.type);
		const base = Schema.make<Codec>(property.type);
		const path = `${sql}.${escapeIdent(property.name)}`;
		fields[property.name] = withFields(
			makeExpr(fieldOptional ? orNone(base) : base, path, {}, Level.atom),
			base,
			path,
			fieldOptional,
		);
	}
	return Object.assign(fields, expr);
};

const localExpr = (name: string, codec: Codec): Expr =>
	withFields(
		makeExpr(codec, `$${name}`, {}, Level.atom),
		codec,
		`$${name}`,
		false,
	);

const scopeOf = (locals: ReadonlyMap<string, Codec>): Record<string, Expr> =>
	Object.fromEntries(
		[...locals].map(([name, codec]) => [name, localExpr(name, codec)]),
	);

// ---- objects

/** The fields of an object expression: expressions, or primitives to inline. */
export type ObjectFields = { readonly [key: string]: Expr | Primitive };

/** An expression, or a primitive as its {@link literal}. */
export type Lifted<V> = V extends Expr ? V : Expr<LiteralCodec<V>, NoParams>;

/** The codec of an object expression: a struct of its fields' codecs. */
export type ObjectCodec<F> = ProjectedCodec<{
	readonly [K in keyof F]: Lifted<F[K]>;
}>;

const key = /^[A-Za-z_][A-Za-z0-9_]*$/u;

const lift = (value: Expr | Primitive): Expr =>
	isExpr(value) ? value : literal(value);

/**
 * `{ key: value, … }`: an object of expressions (a primitive is inlined as
 * its {@link literal}), decoding as a struct of their codecs. NONE in a
 * field reads as `undefined`. Keys are identifiers.
 *
 * @example
 * create(PaymentTable).content(
 *   object({ wallet: $.wallet.id, amount: param('amount', Int), key: $.key }),
 * )
 */
export const object = <const F extends ObjectFields>(
	fields: F,
): Expr<ObjectCodec<F>, ParamsOfAll<Extract<F[keyof F], Expr>>> => {
	const entries = Object.entries(fields).map(
		([name, value]) => [name, lift(value)] as const,
	);
	if (entries.length === 0) {
		throw new RangeError(
			'an object expression needs at least one field: `{}` reads as an empty block',
		);
	}
	for (const [name] of entries) {
		if (!key.test(name)) {
			throw new RangeError(
				`\`${name}\` is not an object key a block writes: use letters, digits and underscores, not starting with a digit`,
			);
		}
	}
	return derive(
		projectedCodec(
			Object.fromEntries(
				entries.map(([name, value]) => [name, value[ExprTypeId].codec]),
			),
		),
		`{ ${entries.map(([name, value]) => `${name}: ${operand(value)}`).join(', ')} }`,
		Level.atom,
		entries.map(([, value]) => value),
	) as Expr<ObjectCodec<F>, ParamsOfAll<Extract<F[keyof F], Expr>>>;
};

// ---- the builder's state

/** The facts a finished part of a block carries, for its parent. */
interface Facts<P extends Params, Throws extends string, R> {
	readonly params: P;
	readonly throws: Throws;
	readonly result: R;
}

export const BlockTypeId = '~effect-surrealkit/Block';

/**
 * What a branch ends with, as a parent reads it: an open block (its result
 * is NONE), a block that returned or threw, or branches.
 */
export interface Arm {
	readonly [BlockTypeId]: Facts<Params, string, unknown>;
}

type ArmParams<A extends Arm> = A[typeof BlockTypeId]['params'];
type ArmThrows<A extends Arm> = A[typeof BlockTypeId]['throws'];
type ArmResult<A extends Arm> = A[typeof BlockTypeId]['result'];

/** One part of a block's text, rendered, with what it reads and does. */
interface Body {
	/** The steps, each one SurrealQL statement without its `;`. */
	readonly steps: ReadonlyArray<string>;
	/** The final statement, and the codecs of what it can yield. */
	readonly terminal?: {
		readonly sql: string;
		readonly results: ReadonlyArray<Codec>;
	};
	readonly params: Params;
	/** The locals visible here, with their codecs. */
	readonly locals: ReadonlyMap<string, Codec>;
	/** The locals this body's own steps define. */
	readonly own: ReadonlyArray<string>;
	/** The locals defined in branches below this body, at any depth. */
	readonly inner: ReadonlyArray<string>;
	readonly throws: ReadonlyArray<string>;
	/** A statement of the block certainly writes. */
	readonly writes: boolean;
}

const bodyOf = (arm: Arm): Body =>
	(arm as unknown as { readonly [BlockTypeId]: Body })[BlockTypeId];

const resultsOf = (body: Body): ReadonlyArray<Codec> =>
	body.terminal?.results ?? [Schema.Undefined];

const statementsOf = (body: Body): ReadonlyArray<string> => [
	...body.steps,
	...(body.terminal === undefined ? [] : [body.terminal.sql]),
];

/** The text of a branch: `{ steps; value }`, or `{ NONE }` when empty. */
const armText = (body: Body): string => {
	const statements = statementsOf(body);
	return `{ ${statements.length === 0 ? 'NONE' : statements.join('; ')} }`;
};

const conditionText = (condition: Expr): string =>
	assertInBlock(condition)[ExprTypeId].level === Level.raw
		? `(${sqlOf(condition)})`
		: sqlOf(condition);

const messageText = (message: string): string => {
	if (typeof message !== 'string' || message.length === 0) {
		throw new RangeError('a THROW message is a non-empty string');
	}
	return sqlOf(literal(message));
};

const unique = <A>(values: ReadonlyArray<A>): ReadonlyArray<A> => [
	...new Set(values),
];

const quotes: Readonly<Record<string, string>> = {
	"'": "'",
	'"': '"',
	'`': '`',
	'⟨': '⟩',
};

/**
 * Whether `sql` holds a `RETURN` statement at any depth: the word where a
 * statement starts (at the start, after `;`, `{` or `(`, or after `THEN` or
 * `ELSE`), outside strings, quoted names and comments. A `RETURN` clause of
 * a write (`… RETURN AFTER`) follows an expression and is not one. A lexical
 * scan, not a parser: the analyzer stays out of builder bundles.
 */
const hasReturn = (sql: string): boolean => {
	let previous = ';';
	let index = 0;
	while (index < sql.length) {
		const char = sql[index] ?? '';
		const two = sql.slice(index, index + 2);
		if (/\s/u.test(char)) {
			index += 1;
		} else if (two === '--' || two === '//' || char === '#') {
			const end = sql.indexOf('\n', index);
			index = end === -1 ? sql.length : end + 1;
		} else if (two === '/*') {
			const end = sql.indexOf('*/', index + 2);
			index = end === -1 ? sql.length : end + 2;
		} else if (quotes[char] !== undefined) {
			const close = quotes[char];
			index += 1;
			while (index < sql.length && sql[index] !== close) {
				index += sql[index] === '\\' ? 2 : 1;
			}
			index += 1;
			previous = 'value';
		} else if (/[A-Za-z0-9_]/u.test(char)) {
			let end = index;
			while (end < sql.length && /[A-Za-z0-9_]/u.test(sql[end] ?? '')) {
				end += 1;
			}
			const word = sql.slice(index, end).toUpperCase();
			if (
				word === 'RETURN' &&
				(';{('.includes(previous) ||
					previous === 'THEN' ||
					previous === 'ELSE')
			) {
				return true;
			}
			previous = word;
			index = end;
		} else {
			previous = char;
			index += 1;
		}
	}
	return false;
};

/**
 * Refuses what a block's step reads that a block has none of: a field of a
 * row (a block has no current row; read a select's result through a local),
 * and `parent(row.field)`.
 */
const assertInBlock = (expr: Expr): Expr => {
	admit(
		{
			own: undefined,
			what: 'a block',
			parents: 'refuse',
			refused:
				'SurrealDB 3.3 and 4.0 bind `$parent` differently in a block with no enclosing select',
		},
		[expr],
	);
	return expr;
};

/** The value of a local or a step: an expression or a one-result statement. */
const valueText = (
	value: unknown,
	what: string,
): {
	readonly sql: string;
	readonly codec: Codec;
	readonly params: Params;
	readonly throws: ReadonlyArray<string>;
	readonly writes: boolean;
} => {
	if (isExpr(value)) {
		assertInBlock(value);
		return {
			sql: operand(value),
			codec: value[ExprTypeId].codec,
			params: paramsOf(value),
			throws: [],
			writes: false,
		};
	}
	if (!isStatement(value)) {
		throw new TypeError(`${what} is an expression or a built statement`);
	}
	if (value.slots !== 1 || value.defines.length > 0 || value.transactional) {
		throw new RangeError(
			`${what} is a statement of one result that defines nothing; \`${value.name}\` returns ${value.slots} results${value.defines.length > 0 ? `, defines ${value.defines.map((name) => `\`$${name}\``).join(', ')}` : ''}${value.transactional ? ' and writes its own transaction' : ''}`,
		);
	}
	let text = value.sql.trimEnd();
	while (text.endsWith(';')) text = text.slice(0, -1).trimEnd();
	if (hasReturn(text)) {
		throw new RangeError(
			`${what} is a statement without RETURN: \`${value.name}\` writes one, and a RETURN in a LET's value ends the block on SurrealDB 4 and is the value on 3; end the statement with its value instead`,
		);
	}
	const lastLine = text.slice(text.lastIndexOf('\n') + 1);
	return {
		// A line comment at the end would swallow the closing parenthesis.
		sql: /--|\/\/|#/u.test(lastLine) ? `(${text}\n)` : `(${text})`,
		codec: value.result,
		params: value.params,
		throws: value.throws,
		writes: value.writes === true,
	};
};

/** The codec of every result a block can yield, as one codec. */
const unionOf = (results: ReadonlyArray<Codec>): Codec => {
	const members = unique(results);
	if (members.length === 0) return Schema.Never;
	if (members.length === 1) return members[0] as Codec;
	return Schema.Union(members) as unknown as Codec;
};

// ---- the builder's types

/**
 * Names SurrealDB sets: protected everywhere (`$auth`, …), or inside blocks
 * and clauses (`$this`, `$value`, …), where SurrealDB 4 refuses a `LET` of
 * them and SurrealDB 3 shadows the engine's value.
 */
type EngineName =
	| 'auth'
	| 'access'
	| 'session'
	| 'token'
	| 'this'
	| 'self'
	| 'parent'
	| 'value'
	| 'input'
	| 'before'
	| 'after'
	| 'event';

/**
 * `N`, when a block may define a local of that name here; otherwise a
 * message saying why not: it is already a local, a binding the block has
 * read so far, or a name SurrealDB sets.
 */
export type LocalName<N extends string, P, L> = N extends keyof L
	? `local \`$${N}\` is already defined in this block`
	: N extends keyof P
		? `\`$${N}\` is a binding of this block, and a local of that name would shadow it`
		: N extends EngineName
			? `\`$${N}\` is set by SurrealDB, so a block cannot define it`
			: N;

/** What a local's value is: an expression or a built statement. */
export type LetValue = Expr | AnyStatement;

type ValueCodec<V> =
	V extends Statement<Params, infer R, string, string>
		? R
		: V extends Expr<infer S, Params>
			? S
			: never;

type ValueParams<V> =
	V extends Statement<infer P, Codec, string, string>
		? P
		: V extends Expr<Codec, infer P>
			? P
			: never;

type ValueThrows<V> =
	V extends Statement<Params, Codec, infer T, string> ? T : never;

/**
 * What a block or branch can return: an expression, or an object of
 * expressions and primitives. A constant on its own is a `literal`, whose
 * type stays the constant (a callback's own return type would widen it).
 */
export type Returned = Expr | ObjectFields;

type ReturnedCodec<V> = V extends Expr<infer S, Params> ? S : ObjectCodec<V>;

type ReturnedParams<V> =
	V extends Expr<Codec, infer P> ? P : ParamsOfAll<Extract<V[keyof V], Expr>>;

/**
 * `S`, a statement run for its effect, when its result is NONE; otherwise a
 * message saying to bind its result.
 */
export type ForEffect<S extends AnyStatement> = [Result<S>] extends [undefined]
	? S
	: 'a step run for its effect returns NONE (a write by id with `.returning("none")`); bind a result with `let`';

/** The result codec of a block whose branches yield the codecs `R`. */
export type BlockResult<R> = [R] extends [never]
	? Schema.Never
	: Schema.Codec<TypeOf<R>, EncodedOf<R>, never, never>;

type TypeOf<R> = R extends { readonly Type: infer T } ? T : never;
type EncodedOf<R> = R extends { readonly Encoded: infer E } ? E : never;

/**
 * A finished block or branch. `P` is the bindings it reads, `Throws` the
 * messages it can raise and `R` the codecs of what it can yield, a union.
 */
export interface Ended<P extends Params, Throws extends string, R> {
	readonly [BlockTypeId]: Facts<P, Throws, R>;

	/**
	 * The statement, one result: it decodes with the union of what the
	 * block can yield, its `throws` are every message it can raise, and it
	 * defines nothing (its locals stay inside it). It certainly writes when a
	 * statement it runs does; otherwise what it writes is unknown, unless
	 * `options.writes` declares it (`false` is refused when it writes).
	 *
	 * @throws RangeError when a local is named like one of its bindings.
	 */
	readonly build: (
		name?: string,
		options?: { readonly writes?: boolean },
	) => Statement<Simplify<P>, BlockResult<R>, Throws>;
}

/**
 * An `IF … ELSE IF …` under construction. Without `else`, a block whose
 * conditions all fail yields NONE, so its result includes `undefined`.
 */
export interface Branches<
	P extends Params,
	L extends Locals,
	Throws extends string,
	R,
> extends Ended<P, Throws, R | Schema.Undefined> {
	/** `ELSE IF condition { … }`. */
	readonly elseIf: <P2 extends Params, A extends Arm>(
		condition: ($: Scope<L>) => Condition<P2>,
		then: (arm: Block<P & P2, L, never>) => A,
	) => Branches<
		P & P2 & ArmParams<A>,
		L,
		Throws | ArmThrows<A>,
		R | ArmResult<A>
	>;

	/** `ELSE { … }`. */
	readonly else: <A extends Arm>(
		then: (arm: Block<P, L, never>) => A,
	) => Ended<P & ArmParams<A>, Throws | ArmThrows<A>, R | ArmResult<A>>;
}

/**
 * A block under construction: `P` is the bindings it reads so far, `L` its
 * locals, `Throws` the messages it can raise. Each step's callback receives
 * the locals as expressions (`$.invite`, `$.invite.status`). Left open, a
 * block (or branch) yields NONE.
 */
export interface Block<
	P extends Params,
	L extends Locals,
	Throws extends string,
> extends Ended<P, Throws, Schema.Undefined> {
	/**
	 * `LET $name = value`: an expression, or a built statement of one result
	 * (a select, a write), whose result later steps read as `$[name]`. The
	 * name must not be a local already, a binding of the block or a name
	 * SurrealDB sets. A local of a branch is visible only in that branch.
	 */
	readonly let: <const N extends string, V extends LetValue>(
		name: LocalName<N, P, L>,
		value: ($: Scope<L>) => V,
	) => Block<
		P & ValueParams<V>,
		L & { readonly [K in N]: ValueCodec<V> },
		Throws | ValueThrows<V>
	>;

	/** `IF condition { THROW message }`: the message joins `throws`. */
	readonly throwIf: <P2 extends Params, const M extends string>(
		condition: ($: Scope<L>) => Condition<P2>,
		message: M,
	) => Block<P & P2, L, Throws | M>;

	/**
	 * `IF $name = NONE { THROW message }`: later steps read the local
	 * without NONE (`$.invite.team` is the team, not the team or NONE).
	 */
	readonly throwIfNone: <
		const N extends keyof L & string,
		const M extends string,
	>(
		name: N,
		message: M,
	) => Block<P, Narrowed<L, N>, Throws | M>;

	/** A statement run for its effect, whose result is NONE. */
	readonly do: <S extends AnyStatement>(
		step: ($: Scope<L>) => ForEffect<S>,
	) => Block<
		P & S['params'],
		L,
		| Throws
		| (S extends Statement<Params, Codec, infer T, string> ? T : never)
	>;

	/**
	 * Ends the block or branch with a value: an expression, or an object of
	 * expressions and primitives (each inlined as its `literal`), which
	 * decodes as a struct (`{ kind: 'joined', id: $.created.id }`).
	 */
	readonly return: <const V extends Returned>(
		value: ($: Scope<L>) => V,
	) => Ended<P & ReturnedParams<V>, Throws, ReturnedCodec<V>>;

	/** Ends the block or branch with `THROW message`. */
	readonly throw: <const M extends string>(
		message: M,
	) => Ended<P, Throws | M, never>;

	/** `IF condition { … }`, which ends the block or branch. */
	readonly if: <P2 extends Params, A extends Arm>(
		condition: ($: Scope<L>) => Condition<P2>,
		then: (arm: Block<P & P2, L, never>) => A,
	) => Branches<
		P & P2 & ArmParams<A>,
		L,
		Throws | ArmThrows<A>,
		ArmResult<A>
	>;

	/**
	 * `IF $name != NONE { … }`, which ends the block or branch; inside, the
	 * local is read without NONE.
	 */
	readonly ifNotNone: <const N extends keyof L & string, A extends Arm>(
		name: N,
		then: (arm: Block<P, Narrowed<L, N>, never>) => A,
	) => Branches<P & ArmParams<A>, L, Throws | ArmThrows<A>, ArmResult<A>>;
}

// ---- the builder

interface Arms {
	readonly before: Body;
	readonly arms: ReadonlyArray<{
		readonly condition: string;
		readonly body: Body;
	}>;
	readonly otherwise?: Body;
}

/** The body an `IF … ELSE IF … ELSE` ends `before` with. */
const closeArms = ({ before, arms, otherwise }: Arms): Body => {
	const bodies = [
		...arms.map((arm) => arm.body),
		...(otherwise ? [otherwise] : []),
	];
	const sql = [
		...arms.map(
			(arm, index) =>
				`${index === 0 ? 'IF' : 'ELSE IF'} ${arm.condition} ${armText(arm.body)}`,
		),
		...(otherwise === undefined ? [] : [`ELSE ${armText(otherwise)}`]),
	].join(' ');
	return {
		...before,
		terminal: {
			sql,
			results: [
				...bodies.flatMap(resultsOf),
				...(otherwise === undefined ? [Schema.Undefined] : []),
			],
		},
		params: mergeParams(
			before.params,
			...bodies.map((body) => body.params),
		),
		inner: unique([
			...before.inner,
			...bodies.flatMap((body) => [...body.own, ...body.inner]),
		]),
		throws: unique([
			...before.throws,
			...bodies.flatMap((body) => body.throws),
		]),
		writes: before.writes || bodies.some((body) => body.writes),
	};
};

/** A branch's body: what `before` sees, and nothing of its own yet. */
const armStart = (before: Body, locals = before.locals): Body => ({
	steps: [],
	params: before.params,
	locals,
	own: [],
	inner: [],
	throws: [],
	writes: false,
});

const assertLocal = (body: Body, name: string): Codec => {
	const codec = body.locals.get(name);
	if (codec === undefined) {
		throw new RangeError(`\`$${name}\` is not a local of this block`);
	}
	return codec;
};

const finish = (body: Body) => ({
	build: (name?: string, options?: { readonly writes?: boolean }) => {
		const label = name ?? 'block';
		const statements = statementsOf(body);
		if (statements.length === 0) {
			throw new RangeError(`block \`${label}\` has no steps`);
		}
		const locals = [...body.own, ...body.inner];
		const shadowed = unique(
			locals.filter((local) => Object.hasOwn(body.params, local)),
		);
		if (shadowed.length > 0) {
			throw new RangeError(
				`block \`${label}\` reads ${shadowed.map((local) => `\`$${local}\``).join(', ')} as a binding and defines a local of the same name, which would shadow it; rename the local`,
			);
		}
		if (options?.writes === false && body.writes) {
			throw new RangeError(
				`block \`${label}\` is declared not to write, but a statement it runs writes`,
			);
		}
		const writes = body.writes ? true : options?.writes;
		return make({
			name: label,
			// One statement: a lone value or IF as it is, anything more as a
			// block, whose locals stay inside it.
			sql:
				body.steps.length === 0
					? `${statements.join('')};`
					: `{\n\t${statements.join(';\n\t')}\n};`,
			params: body.params,
			result: unionOf(resultsOf(body)),
			slots: 1,
			throws: [...body.throws].sort(),
			defines: [],
			...(writes !== undefined && { writes }),
		});
	},
});

const ended = (body: Body) => ({ [BlockTypeId]: body, ...finish(body) });

const branching = (state: Arms): unknown => {
	const scope = () => scopeOf(state.before.locals);
	const arm = (then: (arm: unknown) => unknown): Body =>
		bodyOf(then(open(armStart(state.before))) as Arm);
	const closed = closeArms(state);
	return {
		[BlockTypeId]: closed,
		...finish(closed),
		elseIf: (
			condition: (scope: unknown) => Expr,
			then: (arm: unknown) => unknown,
		) => {
			const test = condition(scope());
			const body = arm(then);
			return branching({
				...state,
				before: {
					...state.before,
					params: mergeParams(state.before.params, paramsOf(test)),
				},
				arms: [...state.arms, { condition: conditionText(test), body }],
			});
		},
		else: (then: (arm: unknown) => unknown) =>
			ended(closeArms({ ...state, otherwise: arm(then) })),
	};
};

const open = (body: Body): unknown => {
	const scope = () => scopeOf(body.locals);
	const step = (
		sql: string,
		changes: Partial<Omit<Body, 'steps' | 'terminal'>> = {},
	): unknown => open({ ...body, ...changes, steps: [...body.steps, sql] });
	const guard = (condition: string, message: string) =>
		`IF ${condition} { THROW ${messageText(message)} }`;
	return {
		[BlockTypeId]: body,
		...finish(body),
		let: (name: string, value: (scope: unknown) => unknown) => {
			assertBindingName(name);
			if (body.locals.has(name)) {
				throw new RangeError(
					`local \`$${name}\` is already defined in this block`,
				);
			}
			const local = valueText(
				value(scope()),
				`the value of \`$${name}\``,
			);
			return step(`LET $${name} = ${local.sql}`, {
				params: mergeParams(body.params, local.params),
				locals: new Map([...body.locals, [name, local.codec]]),
				own: [...body.own, name],
				throws: unique([...body.throws, ...local.throws]),
				writes: body.writes || local.writes,
			});
		},
		throwIf: (condition: (scope: unknown) => Expr, message: string) => {
			const test = condition(scope());
			return step(guard(conditionText(test), message), {
				params: mergeParams(body.params, paramsOf(test)),
				throws: unique([...body.throws, message]),
			});
		},
		throwIfNone: (name: string, message: string) => {
			const codec = assertLocal(body, name);
			return step(guard(`$${name} = NONE`, message), {
				locals: new Map([...body.locals, [name, someOf(codec)]]),
				throws: unique([...body.throws, message]),
			});
		},
		do: (run: (scope: unknown) => unknown) => {
			const statement = run(scope());
			if (!isStatement(statement)) {
				throw new TypeError(
					'a step run for its effect is a built statement',
				);
			}
			const effect = valueText(statement, 'a step run for its effect');
			if (!SchemaAST.isUndefined(effect.codec.ast)) {
				throw new RangeError(
					'a step run for its effect returns NONE (a write by id with `.returning("none")`); bind a result with `let`',
				);
			}
			return step(effect.sql.slice(1, -1), {
				params: mergeParams(body.params, effect.params),
				throws: unique([...body.throws, ...effect.throws]),
				writes: body.writes || effect.writes,
			});
		},
		return: (value: (scope: unknown) => Returned) => {
			const returned: unknown = value(scope());
			if (!isExpr(returned) && !isPlainObject(returned)) {
				throw new TypeError(
					'a block returns an expression or an object of expressions; inline a constant with `literal`',
				);
			}
			const expr = assertInBlock(
				isExpr(returned) ? returned : object(returned as ObjectFields),
			);
			return ended({
				...body,
				terminal: {
					sql: operand(expr),
					results: [expr[ExprTypeId].codec],
				},
				params: mergeParams(body.params, paramsOf(expr)),
			});
		},
		throw: (message: string) =>
			ended({
				...body,
				terminal: { sql: `THROW ${messageText(message)}`, results: [] },
				throws: unique([...body.throws, message]),
			}),
		if: (
			condition: (scope: unknown) => Expr,
			then: (arm: unknown) => unknown,
		) => {
			const test = condition(scope());
			const before = {
				...body,
				params: mergeParams(body.params, paramsOf(test)),
			};
			return branching({
				before,
				arms: [
					{
						condition: conditionText(test),
						body: bodyOf(then(open(armStart(before))) as Arm),
					},
				],
			});
		},
		ifNotNone: (name: string, then: (arm: unknown) => unknown) => {
			const codec = assertLocal(body, name);
			const narrowed = new Map([...body.locals, [name, someOf(codec)]]);
			return branching({
				before: body,
				arms: [
					{
						condition: `$${name} != NONE`,
						body: bodyOf(
							then(open(armStart(body, narrowed))) as Arm,
						),
					},
				],
			});
		},
	};
};

/**
 * A new block: a script of named locals, guards and branches, built into one
 * statement.
 *
 * @example
 * const pay = block()
 *   .let('wallet', () =>
 *     select(WalletTable, param('wallet_id', WalletId)).forUpdate().only().build())
 *   .throwIfNone('wallet', 'Wallet not found')
 *   .throwIf(($) => ne($.wallet.owner, param('payer', AccountId)), 'Not your wallet')
 *   .if(($) => lt($.wallet.balance, param('amount', Int)), (arm) =>
 *     arm.return(($) => ({ kind: 'declined', balance: $.wallet.balance })))
 *   .else((arm) => arm.return(() => ({ kind: 'paid' })))
 *   .build('pay');
 */
export const block = (): Block<NoParams, NoLocals, never> =>
	open({
		steps: [],
		params: {},
		locals: new Map(),
		own: [],
		inner: [],
		throws: [],
		writes: false,
	}) as Block<NoParams, NoLocals, never>;
