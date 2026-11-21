/**
 * Arithmetic on numbers: `+`, `-`, `*` and `/`, with the result codec the
 * engine's answer decodes with.
 *
 * SurrealDB 3.3 and 4.0 agree (`test/conformance.test.ts` pins them): two
 * ints give an int, which fails the statement when it overflows a signed
 * 64-bit int ("Cannot perform addition with …"); an int and a float, or two
 * floats, a float (Infinity past the float range); any decimal operand a
 * decimal. Division of two ints truncates toward zero (`7 / 2` is `3`,
 * `-7 / 2` is `-3`), and division by zero, or by NONE, is the float NaN or
 * ±Infinity whatever the operands, so a quotient decodes as its kind or a
 * float. NONE, NULL, strings, booleans and arrays are refused as operands of
 * `+`, `-` and `*` ("Cannot perform addition with 'int' and 'none'"), so an
 * operand here is a number, never possibly NONE.
 *
 * A float binding is cast (`<float>$f`): the SDK sends a whole JS number as
 * an int whatever its codec, so `$f / 2` with `f = 7` would divide as ints
 * (`3`) on both engines. A stored float field is a float already.
 *
 * An operand's kind (int, float, decimal, or any number) is read from its
 * codec: the generated `Int`, `Int64`, `Float`, `Decimal` and
 * `SurrealNumber`, `Schema.Number` as a float, and a numeric literal by its
 * value. The compiler sees ints and floats alike as `number`, so the
 * result's decoded type is `number` for both; the codec is exact.
 */
import { Schema, SchemaAST } from 'effect';
import type { Decimal as SdkDecimal } from 'surrealdb';

import { Decimal, SurrealNumber } from '../decimal.js';
import { Int, Int64 } from '../int.js';
import { Float } from '../scalars.js';
import {
	codecOf,
	derive,
	type Expr,
	isExpr,
	Level,
	literal,
	type OperandParams,
	operand,
	sqlOf,
} from './expr.js';
import type { Codec, Params } from './statement.js';

/** An operand of arithmetic: a numeric expression, or a number to inline. */
export type NumericOperand = Expr | number | bigint;

type EncodedOf<V> = V extends Expr<infer S, Params> ? S['Encoded'] : V;

/**
 * `V`, when it is a number SurrealDB computes with; otherwise a message
 * saying what SurrealDB refuses.
 */
export type Numeric<V> = [EncodedOf<V>] extends [never]
	? V
	: undefined extends EncodedOf<V>
		? 'this operand may be NONE, and SurrealDB refuses NONE in arithmetic (`1 + NONE` fails the statement): read it where it is set'
		: null extends EncodedOf<V>
			? 'this operand may be NULL, and SurrealDB refuses NULL in arithmetic'
			: [EncodedOf<V>] extends [number | bigint | SdkDecimal]
				? V
				: 'arithmetic takes numbers: SurrealDB refuses a string, a boolean, an array or another value as an operand of + - * /';

/** The kind of number a stored type is, as far as the compiler can tell. */
type KindOf<E> = [E] extends [SdkDecimal]
	? 'decimal'
	: [E] extends [number]
		? 'number'
		: [E] extends [number | bigint]
			? 'int64'
			: 'any';

type Joined<A, B> = 'any' extends A | B
	? 'any'
	: 'decimal' extends A | B
		? 'decimal'
		: 'int64' extends A | B
			? 'number' extends A | B
				? 'mixed'
				: 'int64'
			: 'number';

type ResultOf<K, Quotient extends boolean> = K extends 'any'
	? typeof SurrealNumber
	: K extends 'decimal'
		? Quotient extends true
			? Schema.Codec<
					SdkDecimal | number,
					SdkDecimal | number,
					never,
					never
				>
			: typeof Decimal
		: K extends 'int64'
			? Quotient extends true
				? Schema.Codec<Int64 | number, number | bigint, never, never>
				: typeof Int64
			: K extends 'mixed'
				? Schema.Codec<Int64 | number, number | bigint, never, never>
				: Schema.Codec<number, number, never, never>;

/** The codec of `left op right`: of a quotient, when `Quotient`. */
export type ArithmeticCodec<A, B, Quotient extends boolean> = ResultOf<
	Joined<KindOf<EncodedOf<A>>, KindOf<EncodedOf<B>>>,
	Quotient
>;

// ---- run time

type Kind = 'int' | 'int64' | 'float' | 'decimal' | 'any';

const kinds: Readonly<Record<string, Kind>> = {
	'effect-surrealkit/Int': 'int',
	'effect-surrealkit/Int64': 'int64',
	'effect-surrealkit/Int64Literal': 'int64',
	'effect-surrealkit/Float': 'float',
	'effect-surrealkit/Decimal': 'decimal',
	'effect-surrealkit/DecimalLiteral': 'decimal',
	'effect-surrealkit/SurrealNumber': 'any',
};

const representation = (ast: SchemaAST.AST): string | undefined => {
	const id = (
		ast.annotations as
			| { readonly representation?: { readonly id?: unknown } }
			| undefined
	)?.representation?.id;
	return typeof id === 'string' ? id : undefined;
};

const kindOfAst = (ast: SchemaAST.AST): Kind | 'none' | undefined => {
	if (SchemaAST.isUndefined(ast) || SchemaAST.isNull(ast)) return 'none';
	if (SchemaAST.isDeclaration(ast)) {
		const id = representation(ast);
		return id === undefined ? undefined : kinds[id];
	}
	if (SchemaAST.isNumber(ast)) return 'float';
	if (SchemaAST.isBigInt(ast)) return 'int64';
	if (SchemaAST.isLiteral(ast)) {
		if (typeof ast.literal === 'bigint') return 'int64';
		if (typeof ast.literal === 'number') {
			return Number.isInteger(ast.literal) ? 'int' : 'float';
		}
		return undefined;
	}
	if (SchemaAST.isUnion(ast)) {
		const members = ast.types.map(kindOfAst);
		if (members.includes('none')) return 'none';
		if (members.some((member) => member === undefined)) return undefined;
		const distinct = [...new Set(members)];
		return distinct.length === 1 ? distinct[0] : 'any';
	}
	return undefined;
};

/**
 * Whether a value `ast` describes may be a decimal, or an array holding one
 * at any depth. Only ints, floats, NONE and NULL (and unions and arrays of
 * them) are known not to; any other codec may.
 */
const holdsDecimalAst = (ast: SchemaAST.AST): boolean => {
	if (SchemaAST.isUndefined(ast) || SchemaAST.isNull(ast)) return false;
	if (SchemaAST.isUnion(ast)) return ast.types.some(holdsDecimalAst);
	if (SchemaAST.isArrays(ast)) {
		return [...ast.elements, ...ast.rest].some(holdsDecimalAst);
	}
	const kind = kindOfAst(ast);
	return kind !== 'int' && kind !== 'int64' && kind !== 'float';
};

/** Whether a value of `codec` may be a decimal, or an array holding one. */
export const holdsDecimal = (codec: Codec): boolean =>
	holdsDecimalAst(codec.ast);

/**
 * `true` when a value of stored type `E` may be a decimal, or an array
 * holding one; `false` when it is certainly an int, a float, NONE or NULL
 * (or an array of them). The compiler's answer is never narrower than
 * {@link holdsDecimal}'s.
 */
export type HoldsDecimal<E> = unknown extends E
	? true
	: [E] extends [number | bigint | undefined | null]
		? false
		: [E] extends [ReadonlyArray<infer I> | undefined | null]
			? HoldsDecimal<I>
			: true;

/**
 * A float, or a decimal: what a function SurrealDB computes in the kind of
 * its input returns when that input may hold a decimal (`math::sqrt`,
 * `math::stddev`); some inputs still give a float (the spread of one value
 * is the int 0, the square root of a negative decimal NaN).
 */
export const FloatOrDecimal = /*#__PURE__*/ Schema.Union([Float, Decimal]);

/** {@link FloatOrDecimal} when `E` may hold a decimal, otherwise a float. */
export type FloatOrDecimalOf<E> =
	HoldsDecimal<E> extends false ? typeof Float : typeof FloatOrDecimal;

/** The kind of number `expr` yields, or a refusal saying why it is none. */
const kindOf = (expr: Expr): Kind => {
	const kind = kindOfAst(codecOf(expr).ast);
	if (kind === 'none') {
		throw new TypeError(
			`\`${sqlOf(expr)}\` may be NONE, and SurrealDB refuses NONE in arithmetic`,
		);
	}
	if (kind === undefined) {
		throw new TypeError(
			`\`${sqlOf(expr)}\` is not a number: arithmetic takes an int, a float, a decimal or a number`,
		);
	}
	return kind;
};

const joined = (left: Kind, right: Kind): Kind => {
	const both = new Set([left, right]);
	if (both.has('any')) return 'any';
	if (both.has('decimal')) return 'decimal';
	if (both.has('float')) return 'float';
	if (both.has('int64')) return 'int64';
	return 'int';
};

const product: Readonly<Record<Kind, Codec>> = {
	int: Int,
	int64: Int64,
	float: Float,
	decimal: Decimal,
	any: SurrealNumber,
};

// A quotient by zero (or NONE) is a float, NaN or ±Infinity.
const quotient: Readonly<Record<Kind, Codec>> = {
	int: Schema.Union([Int, Float]) as unknown as Codec,
	int64: Schema.Union([Int64, Float]) as unknown as Codec,
	float: Float,
	decimal: Schema.Union([Decimal, Float]) as unknown as Codec,
	any: SurrealNumber,
};

/** A binding or a block's local: what the SDK sent, of whatever kind. */
const parameter = /^\$[A-Za-z_][A-Za-z0-9_]*$/u;

const lift = (value: NumericOperand): Expr => {
	if (isExpr(value)) return value;
	if (typeof value === 'number' || typeof value === 'bigint') {
		return literal(value);
	}
	throw new TypeError(
		'arithmetic takes numbers: an expression, or a number to inline',
	);
};

const arithmetic =
	<Quotient extends boolean>(
		operator: '+' | '-' | '*' | '/',
		divides: Quotient,
	) =>
	<const A, const B>(
		left: Numeric<A>,
		right: Numeric<B>,
	): Expr<
		ArithmeticCodec<A, B, Quotient>,
		OperandParams<A> & OperandParams<B>
	> => {
		const a = lift(left as NumericOperand);
		const b = lift(right as NumericOperand);
		const kinds = [kindOf(a), kindOf(b)] as const;
		const kind = joined(...kinds);
		const text = (value: Expr, own: Kind): string =>
			own === 'float' && parameter.test(sqlOf(value))
				? `<float>${sqlOf(value)}`
				: operand(value);
		return derive(
			(divides ? quotient : product)[kind] as ArithmeticCodec<
				A,
				B,
				Quotient
			>,
			`${text(a, kinds[0])} ${operator} ${text(b, kinds[1])}`,
			// Looser than a term, so it is parenthesised as an operand.
			Level.comparison,
			[a, b],
		);
	};

/**
 * `left + right`: an int for two ints (an overflow fails the statement), a
 * float with a float, a decimal with a decimal.
 *
 * @example
 * select(PersonTable).value((row) => add(row.score, param('bonus', SurrealSchema.Int)));
 * // SELECT VALUE score + $bonus FROM ⟨person⟩
 */
export const add = arithmetic('+', false);
/** `left - right`, of the kinds {@link add} gives. */
export const sub = arithmetic('-', false);
/** `left * right`, of the kinds {@link add} gives. */
export const mul = arithmetic('*', false);
/**
 * `left / right`: two ints truncate toward zero (`7 / 2` is `3`); a float
 * or a decimal divides exactly; division by zero is a float, NaN or
 * ±Infinity, whatever the operands, so the result decodes as its kind or
 * a float.
 */
export const div = arithmetic('/', true);
