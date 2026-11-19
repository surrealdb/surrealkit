/**
 * Composition of generated texts: `compose(template, fills)` fills every
 * hole of a template with a text, a composite, a choice, a constant or
 * `empty`; `choose(axis, options)` picks a fill by one axis of a key. The
 * names a composition reads are worked out from the literal names the
 * `statements` command writes (each text's free names and each hole's
 * scope). Raw SurrealQL is refused as a fill.
 *
 * Like `part`, this module imports the analyzer.
 */
import type { Text, TextHoleScopes } from '../analyze/text.js';
import { type Expr, ExprTypeId, isExpr } from './expr.js';
import type { Part } from './part.js';
import type { Codec, NoParams } from './statement.js';

export const TextNodeTypeId = '~effect-surrealkit/TextNode';

type AnyText = Text<string, string, string, string, TextHoleScopes>;

/** Fills a hole with nothing: an absent clause is left out of the text. */
export interface Empty {
	readonly [TextNodeTypeId]: 'empty';
}

/** A template with every hole filled. */
export interface Composite<
	T extends AnyText = AnyText,
	F extends CompositeFills = CompositeFills,
> {
	readonly [TextNodeTypeId]: 'compose';
	readonly template: T;
	readonly fills: F;
}

/** One of several texts, by the value of one axis of the key. */
export interface Choice<
	A extends string = string,
	O extends ChoiceOptions = ChoiceOptions,
> {
	readonly [TextNodeTypeId]: 'choose';
	readonly axis: A;
	readonly options: O;
}

/** What fills a hole: a part (statements) fills one that starts a statement. */
export type TextNode =
	| AnyText
	| Part<string, string, string>
	| Composite
	| Choice
	| Empty
	| Expr<Codec, NoParams>;

export type CompositeFills = { readonly [hole: string]: TextNode };
export type ChoiceOptions = { readonly [option: string]: TextNode };

export const empty: Empty = { [TextNodeTypeId]: 'empty' };

// ---------------------------------------------------------------------------
// Compile-time checks.

type IsUnion<T, U = T> = T extends unknown
	? [U] extends [T]
		? false
		: true
	: never;

/**
 * Why `V` cannot fill a hole, or `never`. A fill typed as a union of texts
 * is refused: the compiler could not tell which one a variant holds, so
 * what it reads would not be exact; `choose` between them instead.
 */
type FillProblem<V> =
	true extends IsUnion<V>
		? 'this fill is typed as one of several texts, so the compiler cannot tell which a variant holds: choose between them with `choose(…)`'
		: SingleFillProblem<V>;

type SingleFillProblem<V> =
	V extends Expr<Codec, infer P>
		? [keyof P] extends [never]
			? never
			: 'a constant fill binds nothing: put the binding in a `.surql` fragment'
		: V extends Text<infer Holes, string, string, string, TextHoleScopes>
			? [Holes] extends [never]
				? never
				: `this fill is a template with holes (${Holes}): compose it first`
			: V extends TextNode
				? never
				: 'a hole takes a generated text or part, `compose(…)`, `choose(…)`, `literal(…)` or `empty`, not raw SurrealQL';

/**
 * `F`, when it fills exactly the holes of `T`. Otherwise a fill for a hole
 * the template does not have, or a template given as a fill, is typed as a
 * message naming it, and when nothing is extra, the fills' expected type is
 * a message naming each hole left without one.
 */
export type CheckedFills<T extends AnyText, F> = [F] extends [never]
	? F
	: string extends T['holes'][number]
		? F
		: [
					| Exclude<keyof F, T['holes'][number]>
					| { [H in keyof F]: FillProblem<F[H]> }[keyof F],
				] extends [never]
			? [Exclude<T['holes'][number], keyof F>] extends [never]
				? F
				: `hole \`${Exclude<T['holes'][number], keyof F>}\` has no fill`
			: {
					readonly [H in keyof F]: H extends T['holes'][number]
						? [FillProblem<F[H]>] extends [never]
							? F[H]
							: FillProblem<F[H]>
						: `\`${H & string}\` is not a hole of this template`;
				};

// ---------------------------------------------------------------------------
// Constructors.

/**
 * Fills every hole of `template`. Each fill is a text without holes (a
 * generated fragment or part), another composite, a {@link choose}, a
 * constant (`literal(…)`) or {@link empty}. A hole without a fill, a fill for
 * a hole the template does not have, and a template given as a fill are
 * compile errors, and are refused here.
 */
export const compose = <
	const T extends AnyText,
	const F extends { readonly [hole: string]: unknown },
>(
	template: T,
	fills: CheckedFills<T, F>,
): F extends CompositeFills ? Composite<T, F> : never => {
	const given = fills as unknown as CompositeFills;
	const holes = new Set(template.holes);
	for (const hole of Object.keys(given).sort()) {
		if (!holes.has(hole))
			throw new RangeError(`${label(template)} has no hole ${hole}`);
		checkNode(given[hole], `the fill for ${hole}`);
	}
	for (const hole of [...holes].sort()) {
		if (!Object.hasOwn(given, hole))
			throw new RangeError(
				`hole ${hole} of ${label(template)} has no fill`,
			);
	}
	return {
		[TextNodeTypeId]: 'compose',
		template,
		fills: given,
	} as unknown as F extends CompositeFills ? Composite<T, F> : never;
};

/**
 * `O` with its keys as strings: `{ 1: … }` is keyed by the number 1, and a
 * key's axis value is looked up as text.
 */
export type StringKeyed<O> = [Extract<keyof O, number>] extends [never]
	? O
	: { readonly [X in keyof O as `${X & (string | number)}`]: O[X] };

/** `O`, or each option that cannot be a fill typed as a message saying why. */
export type CheckedOptions<O> = [O] extends [never]
	? O
	: [{ [X in keyof O]: FillProblem<O[X]> }[keyof O]] extends [never]
		? O
		: {
				readonly [X in keyof O]: [FillProblem<O[X]>] extends [never]
					? O[X]
					: FillProblem<O[X]>;
			};

/** One of `options`, by the value of `axis` in the key a family selects with. */
export const choose = <
	const A extends string,
	const O extends { readonly [option: string]: unknown },
>(
	axis: A,
	options: CheckedOptions<O>,
): O extends ChoiceOptions ? Choice<A, StringKeyed<O>> : never => {
	const given = options as unknown as ChoiceOptions;
	const values = Object.keys(given);
	if (values.length === 0)
		throw new RangeError(`choose('${axis}', …) has no options`);
	for (const value of values)
		checkNode(given[value], `the text for ${axis} = ${value}`);
	return {
		[TextNodeTypeId]: 'choose',
		axis,
		options: given,
	} as unknown as O extends ChoiceOptions ? Choice<A, StringKeyed<O>> : never;
};

const label = (item: AnyText): string =>
	item.name === undefined ? 'the template' : `\`${item.name}\``;

const nodeKind = (
	node: unknown,
): 'empty' | 'compose' | 'choose' | 'expr' | 'text' | undefined => {
	if (typeof node !== 'object' || node === null) return undefined;
	if (TextNodeTypeId in node)
		return (
			node as {
				readonly [TextNodeTypeId]: 'empty' | 'compose' | 'choose';
			}
		)[TextNodeTypeId];
	if (isExpr(node)) return 'expr';
	const candidate = node as Partial<AnyText>;
	return typeof candidate.sql === 'string' && Array.isArray(candidate.free)
		? 'text'
		: undefined;
};

const checkNode = (node: unknown, what: string): void => {
	const kind = nodeKind(node);
	if (kind === undefined) {
		throw new RangeError(
			`${what} is not a text, a composite, a choice, a constant or \`empty\`; raw SurrealQL strings are refused`,
		);
	}
	if (kind === 'text' && ((node as AnyText).holes ?? []).length > 0) {
		throw new RangeError(
			`${what} is a template with holes (${((node as AnyText).holes ?? []).join(', ')}): compose it first`,
		);
	}
	if (
		kind === 'expr' &&
		Object.keys((node as Expr)[ExprTypeId].params).length > 0
	) {
		throw new RangeError(
			`${what} is a constant that binds ${Object.keys((node as Expr)[ExprTypeId].params).join(', ')}: a constant fill binds nothing`,
		);
	}
};
