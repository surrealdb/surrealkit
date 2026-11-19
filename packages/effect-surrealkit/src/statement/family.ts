/**
 * Statement families: one authored template, a finite set of variants chosen
 * at run time.
 *
 * `compose(template, fills)` fills every hole of a template with a text, a
 * composite, a choice, a constant or `empty`; `choose(axis, options)` picks a
 * fill by one axis of a key. `family(axes, root).define({ params, result })`
 * is every variant the axes span, each an ordinary {@link Statement} with
 * static text: the text of a variant never depends on input, only on which
 * variant the caller selects.
 *
 * What a variant reads is worked out by the compiler from the literal names
 * the `statements` command writes (each text's free names and each hole's
 * scope), so the bindings of a variant are exact at compile time, and the
 * same rule is checked against the analyzer's reading of every variant's
 * final text before that variant can be sent. A variant is built on its
 * first selection; `variants()` builds every one, for a build step, a test
 * or pinning.
 *
 * Like `part`, this module imports the analyzer.
 */
import { type Analysis, analyzeSync } from '../analyze/analyze.js';
import { DEFAULT_HOLE_DELIMITERS, lex } from '../analyze/lexer.js';
import type { Text, TextHoleScope, TextHoleScopes } from '../analyze/text.js';
import { type Expr, ExprTypeId, isExpr } from './expr.js';
import type { Part } from './part.js';
import {
	type Codec,
	make,
	type NoParams,
	type Params,
	type Statement,
} from './statement.js';

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

/** The value an axis may take. */
export type AxisValue = string | number | boolean;

/** The axes of a family: each a list of the values it may take. */
export type Axes = { readonly [axis: string]: ReadonlyArray<AxisValue> };

/** A key: one value per axis. Union-valued properties select any of them. */
export type KeyOf<A extends Axes> = { readonly [X in keyof A]: A[X][number] };

export const empty: Empty = { [TextNodeTypeId]: 'empty' };

// ---------------------------------------------------------------------------
// What a node reads, defines and throws, at the type level.

type OptionKey<V> = V extends AxisValue ? `${V}` : never;

type ScopeAt<T, H> = T extends { readonly scopes?: infer S }
	? H extends keyof NonNullable<S>
		? NonNullable<S>[H]
		: never
	: never;

type BoundAt<T, H> =
	ScopeAt<T, H> extends TextHoleScope<infer B, string> ? B : never;

type BindsAt<T, H> =
	ScopeAt<T, H> extends TextHoleScope<string, infer B> ? B : never;

type Selected<O, A extends string, K> = A extends keyof K
	? O[OptionKey<K[A]> & keyof O]
	: never;

/**
 * The bindings node `N` reads when the key is `K`. For a key whose axes are
 * unions, every name some selectable variant reads.
 */
export type FreeOf<N, K> = N extends Empty
	? never
	: N extends Choice<infer A, infer O>
		? string extends keyof O
			? string
			: FreeOf<Selected<O, A, K>, K>
		: N extends Composite<infer T, infer F>
			? string extends keyof F
				? string
				:
						| Exclude<
								T['free'][number],
								{
									[H in keyof F]: DefinesOf<F[H], K> &
										BindsAt<T, H>;
								}[keyof F]
						  >
						| {
								[H in keyof F]: Exclude<
									FreeOf<F[H], K>,
									BoundAt<T, H>
								>;
						  }[keyof F]
			: N extends Text<string, infer Free, string, string, TextHoleScopes>
				? Free
				: N extends Part<infer Free, string, string>
					? Free
					: never;

/** Whether `Name` is missing from the defines of some selectable option. */
type MissingFrom<Name, O, Keys, K> = Keys extends keyof O
	? Name extends DefinesOf<O[Keys], K>
		? never
		: Keys
	: never;

/** The names every selectable option of a choice defines. */
type DefinesOfChoice<
	O,
	Keys,
	K,
	All = Keys extends keyof O ? DefinesOf<O[Keys], K> : never,
> = All extends string
	? [MissingFrom<All, O, Keys, K>] extends [never]
		? All
		: never
	: never;

/**
 * The names node `N` `LET`s at its top level when the key is `K`: for a key
 * whose axes are unions, those every selectable variant defines.
 */
export type DefinesOf<N, K> =
	N extends Choice<infer A, infer O>
		? string extends keyof O
			? string
			: A extends keyof K
				? DefinesOfChoice<O, OptionKey<K[A]> & keyof O, K>
				: never
		: N extends Composite<infer T, CompositeFills>
			? T['defines'][number]
			: N extends Text<string, string, infer D, string, TextHoleScopes>
				? D
				: N extends Part<string, infer D, string>
					? D
					: never;

/** The messages node `N` can `THROW` when the key is `K`. */
export type ThrowsOf<N, K> =
	N extends Choice<infer A, infer O>
		? string extends keyof O
			? string
			: ThrowsOf<Selected<O, A, K>, K>
		: N extends Composite<infer T, infer F>
			? string extends keyof F
				? string
				:
						| T['throws'][number]
						| { [H in keyof F]: ThrowsOf<F[H], K> }[keyof F]
			: N extends Text<string, string, string, infer T, TextHoleScopes>
				? T
				: N extends Part<string, string, infer T>
					? T
					: never;

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

type AxesUsed<N> =
	N extends Choice<infer A, infer O>
		? string extends keyof O
			? string
			: A | AxesUsed<O[keyof O]>
		: N extends Composite<AnyText, infer F>
			? string extends keyof F
				? string
				: AxesUsed<F[keyof F]>
			: never;

type ChoiceProblems<A extends Axes, X extends string, O> = X extends keyof A
	?
			| ([Exclude<OptionKey<A[X][number]>, keyof O>] extends [never]
					? never
					: `choose('${X}', …) has no text for \`${Exclude<OptionKey<A[X][number]>, keyof O>}\``)
			| ([Exclude<keyof O & string, OptionKey<A[X][number]>>] extends [
					never,
			  ]
					? never
					: `choose('${X}', …) has a text for \`${Exclude<keyof O & string, OptionKey<A[X][number]>>}\`, which is not a value of the axis`)
	: `choose('${X}', …) names an axis this family does not declare`;

type TreeProblems<A extends Axes, N> =
	N extends Choice<infer X, infer O>
		? string extends keyof O
			? never
			: ChoiceProblems<A, X, O> | TreeProblems<A, O[keyof O]>
		: N extends Composite<AnyText, infer F>
			? string extends keyof F
				? never
				: TreeProblems<A, F[keyof F]>
			: never;

type AxisProblems<A extends Axes, N> =
	| TreeProblems<A, N>
	| ([Exclude<keyof A & string, AxesUsed<N>>] extends [never]
			? never
			: `axis \`${Exclude<keyof A & string, AxesUsed<N>>}\` is never chosen on, so every value of it gives the same text`);

/**
 * `A`, or a message naming each problem between the axes and the texts that
 * choose on them: a choice on an axis `A` does not declare, a value of an
 * axis a choice has no text for, a text for no value, an axis nothing
 * chooses on. Checked on the axes, whose type is short, so the first line of
 * the diagnostic is the message.
 */
export type CheckedAxes<A extends Axes, N> = [A] extends [never]
	? A
	: [AxisProblems<A, N>] extends [never]
		? A
		: AxisProblems<A, N> & string;

/**
 * `P`, the codecs of a family, when they name exactly what its variants
 * read; otherwise as `Exactly` does for a sequence.
 */
export type FamilyContract<Names extends string, P> = string extends Names
	? P
	: [Exclude<keyof P, Names>] extends [never]
		? [Exclude<Names, keyof P>] extends [never]
			? P
			: `binding \`$${Exclude<Names, keyof P> & string}\` is read by a variant of this family and has no codec`
		: {
				readonly [X in keyof P]: X extends Names
					? P[X]
					: `binding \`$${X & string}\` is not read by any variant of this family`;
			};

/** The key `K`, or a message naming a property that is not an axis. */
export type CheckedVariantKey<A extends Axes, K> = [K] extends [never]
	? K
	: [Exclude<keyof K, keyof A>] extends [never]
		? K
		: {
				readonly [X in keyof K]: X extends keyof A
					? K[X]
					: `\`${X & string}\` is not an axis of this family`;
			};

/** The codecs of `P` a variant reading `Names` binds. */
export type ParamsAt<P, Names> = {
	readonly [X in keyof P as X extends Names ? X : never]: P[X];
};

/** A family: every variant its axes span, each a statement. */
export interface Family<A extends Axes, N, P extends Params, R extends Codec> {
	readonly name: string;
	readonly axes: A;
	/**
	 * The statement of the variant `key` selects. A key known only at run
	 * time (an axis typed as a union) is typed as the statement that reads
	 * every name some variant it can select reads: an input must give each
	 * of them that is required, and the selected variant sends only what it
	 * reads.
	 */
	readonly select: <const K extends KeyOf<A>>(
		key: CheckedVariantKey<A, K>,
	) => Statement<ParamsAt<P, FreeOf<N, K>>, R, ThrowsOf<N, K> & string>;
	/** Every variant, built, with its key: for pinning and tests. */
	readonly variants: () => ReadonlyArray<{
		readonly key: KeyOf<A>;
		readonly statement: Statement<
			Params,
			R,
			ThrowsOf<N, KeyOf<A>> & string
		>;
	}>;
}

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

// ---------------------------------------------------------------------------
// Run time: verifying texts, rendering and claiming.

const verified = new WeakMap<
	AnyText,
	{
		readonly analysis: Analysis;
		readonly body: string;
		readonly sites: ReadonlyArray<Site>;
	}
>();

interface Site {
	readonly hole: string;
	readonly start: number;
	readonly end: number;
}

const sameList = (
	a: ReadonlyArray<string>,
	b: ReadonlyArray<string>,
): boolean => {
	const x = [...new Set(a)].sort();
	const y = [...new Set(b)].sort();
	return (
		x.length === y.length && x.every((value, index) => value === y[index])
	);
};

/**
 * Checks a text against its own SurrealQL once: the holes, free names,
 * defines, throws and hole scopes it declares must be what the analyzer
 * derives, since the compiler's reading of a family rests on them.
 */
const verifyText = (item: AnyText) => {
	const known = verified.get(item);
	if (known !== undefined) return known;
	const analysis = analyzeSync(item.sql);
	const differences: Array<string> = [];
	const compare = (
		field: string,
		declared: ReadonlyArray<string>,
		derived: ReadonlyArray<string>,
	) => {
		if (!sameList(declared, derived))
			differences.push(
				`${field}: declared [${[...declared].sort().join(', ')}], the text has [${derived.join(', ')}]`,
			);
	};
	compare('holes', item.holes ?? [], analysis.holes);
	compare('free', item.free, analysis.free);
	compare('defines', item.defines, analysis.defines);
	compare('throws', item.throws, analysis.throws);
	if (analysis.holes.length > 0) {
		if (item.scopes === undefined) {
			differences.push(
				'scopes: a template must declare the scope of each hole (regenerate it with `effect-surrealkit statements`)',
			);
		} else {
			for (const hole of analysis.holes) {
				const declared = item.scopes[hole];
				const derived = analysis.holeScopes[hole];
				if (declared === undefined || derived === undefined) {
					differences.push(
						`scopes: hole ${hole} has no declared scope`,
					);
					continue;
				}
				compare(`scopes.${hole}.bound`, declared.bound, derived.bound);
				compare(`scopes.${hole}.binds`, declared.binds, derived.binds);
			}
		}
	}
	if (differences.length > 0)
		throw new RangeError(
			`${label(item)} does not declare what its text does: ${differences.join('; ')}`,
		);
	const { tokens } = lex(item.sql, DEFAULT_HOLE_DELIMITERS);
	const first = tokens[0];
	const last = tokens[tokens.length - 1];
	const offset = first?.start ?? 0;
	const body =
		first === undefined || last === undefined
			? ''
			: item.sql.slice(first.start, last.end);
	const sites = tokens
		.filter((token) => token.kind === 'hole')
		.map((token) => ({
			hole: token.value,
			start: token.start - offset,
			end: token.end - offset,
		}));
	const entry = { analysis, body, sites };
	verified.set(item, entry);
	return entry;
};

type RuntimeKey = Readonly<Record<string, AxisValue>>;

const selectedOption = (node: Choice, key: RuntimeKey): TextNode => {
	const value = key[node.axis];
	const option =
		value === undefined ? undefined : node.options[String(value)];
	if (option === undefined)
		throw new RangeError(
			`choose('${node.axis}', …) has no text for ${String(value)}`,
		);
	return option;
};

/** The SurrealQL of `node` for `key`, without surrounding whitespace. */
const render = (node: TextNode, key: RuntimeKey): string => {
	switch (nodeKind(node)) {
		case 'empty':
			return '';
		case 'expr':
			return (node as Expr)[ExprTypeId].sql;
		case 'choose':
			return render(selectedOption(node as Choice, key), key);
		case 'compose': {
			const { template, fills } = node as Composite;
			const { body, sites } = verifyText(template);
			let out = '';
			let at = 0;
			for (const site of sites) {
				out +=
					body.slice(at, site.start) +
					render(fills[site.hole] as TextNode, key);
				at = site.end;
			}
			return out + body.slice(at);
		}
		default:
			return verifyText(node as AnyText).body;
	}
};

interface Claim {
	readonly free: ReadonlySet<string>;
	readonly defines: ReadonlySet<string>;
	readonly throws: ReadonlySet<string>;
}

const none: Claim = { free: new Set(), defines: new Set(), throws: new Set() };

/**
 * What `node` reads, defines and throws for `key`, by the rule the
 * compiler applies ({@link FreeOf}), from the texts' declared names alone.
 */
const claim = (node: TextNode, key: RuntimeKey): Claim => {
	switch (nodeKind(node)) {
		case 'empty':
		case 'expr':
			return none;
		case 'choose':
			return claim(selectedOption(node as Choice, key), key);
		case 'compose': {
			const { template, fills } = node as Composite;
			verifyText(template);
			const free = new Set<string>();
			const throws = new Set<string>(template.throws);
			const bindsByFills = new Set<string>();
			for (const [hole, fill] of Object.entries(fills)) {
				const scope = template.scopes?.[hole] ?? {
					bound: [],
					binds: [],
				};
				const inner = claim(fill, key);
				for (const name of inner.free)
					if (!scope.bound.includes(name)) free.add(name);
				for (const name of inner.defines)
					if (scope.binds.includes(name)) bindsByFills.add(name);
				for (const message of inner.throws) throws.add(message);
			}
			for (const name of template.free)
				if (!bindsByFills.has(name)) free.add(name);
			return { free, defines: new Set(template.defines), throws };
		}
		default: {
			const item = node as AnyText;
			verifyText(item);
			return {
				free: new Set(item.free),
				defines: new Set(item.defines),
				throws: new Set(item.throws),
			};
		}
	}
};

const keysOf = (axes: Axes): ReadonlyArray<RuntimeKey> => {
	let keys: Array<Record<string, AxisValue>> = [{}];
	for (const [axis, values] of Object.entries(axes)) {
		keys = keys.flatMap((key) =>
			values.map((value) => ({ ...key, [axis]: value })),
		);
	}
	return keys;
};

const describeKey = (key: RuntimeKey): string =>
	Object.entries(key)
		.map(([axis, value]) => `${axis}=${String(value)}`)
		.join(', ');

const axesOf = (node: TextNode, into: Map<string, Set<string>>): void => {
	const kind = nodeKind(node);
	if (kind === 'choose') {
		const choice = node as Choice;
		const options = into.get(choice.axis) ?? new Set<string>();
		for (const [value, option] of Object.entries(choice.options)) {
			options.add(value);
			axesOf(option, into);
		}
		into.set(choice.axis, options);
	} else if (kind === 'compose') {
		for (const fill of Object.values((node as Composite).fills))
			axesOf(fill, into);
	}
};

/** The axes a tree chooses on must be the family's, each value covered. */
const checkAxes = (name: string, axes: Axes, root: TextNode): void => {
	const used = new Map<string, Set<string>>();
	axesOf(root, used);
	const problems: Array<string> = [];
	for (const [axis, values] of Object.entries(axes)) {
		if (values.length === 0)
			problems.push(`axis \`${axis}\` has no values`);
		if (new Set(values.map(String)).size !== values.length)
			problems.push(`axis \`${axis}\` repeats a value`);
		if (!used.has(axis))
			problems.push(`axis \`${axis}\` is never chosen on`);
	}
	for (const axis of used.keys())
		if (!Object.hasOwn(axes, axis))
			problems.push(
				`choose('${axis}', …) names an axis the family does not declare`,
			);
	if (problems.length > 0)
		throw new RangeError(`family \`${name}\`: ${problems.join('; ')}`);
};

const listNames = (names: Iterable<string>): string =>
	[...names]
		.sort()
		.map((name) => `$${name}`)
		.join(', ');

/**
 * Every variant the axes span of the text `root`, as statements.
 *
 * `define` takes one codec for each name any variant reads, and the result
 * codec every variant decodes with. It checks at once that every text
 * declares what its SQL does, that the axes are the ones the texts choose
 * on, and that the codecs are exactly the names the variants read by the
 * compiler's rule. A variant is built on its first selection (or by
 * `variants()`, or by `define` with `build: 'eager'`): its text is
 * rendered and analysed, and refused unless the analyzer reads exactly the
 * names the rule says (so the types of `select` are what is sent), throws
 * what it says, and the text has no holes and no errors.
 *
 * Every variant's statement is named after the family, so its metrics and
 * span are the family's; its `variant` is the key that selected it, which a
 * run records on its span.
 */
export const family = <const A extends Axes, const N extends TextNode>(
	checkedAxes: CheckedAxes<A, N>,
	root: N,
) => ({
	define: <const P extends Params, R extends Codec>(contract: {
		readonly name?: string;
		readonly params: FamilyContract<FreeOf<N, KeyOf<A>> & string, P>;
		readonly result: R;
		readonly writes?: boolean;
		readonly build?: 'eager' | 'lazy';
	}): Family<A, N, P, R> => {
		const name = contract.name ?? 'family';
		const tree = root;
		const axes = checkedAxes as A;
		const params = contract.params as P;
		checkNode(tree, `the text of family \`${name}\``);
		checkAxes(name, axes, tree);
		const keys = keysOf(axes);
		const read = new Set<string>();
		for (const key of keys)
			for (const free of claim(tree, key).free) read.add(free);
		const declared = Object.keys(params);
		const missing = [...read].filter((free) => !declared.includes(free));
		const extra = declared.filter((free) => !read.has(free));
		if (missing.length > 0 || extra.length > 0) {
			throw new RangeError(
				[
					`the bindings of family \`${name}\` do not match the names its variants read`,
					...(missing.length > 0
						? [`missing: ${listNames(missing)}`]
						: []),
					...(extra.length > 0
						? [`not read by any variant: ${listNames(extra)}`]
						: []),
				].join('; '),
			);
		}
		const built = new Map<string, Statement<Params, R, string>>();
		const indexOf = (key: RuntimeKey): string =>
			JSON.stringify(Object.keys(axes).map((axis) => key[axis]));
		const build = (key: RuntimeKey): Statement<Params, R, string> => {
			const index = indexOf(key);
			const known = built.get(index);
			if (known !== undefined) return known;
			const where = `family \`${name}\`, variant ${describeKey(key)}`;
			const sql = render(tree, key);
			const analysis = analyzeSync(sql);
			const expected = claim(tree, key);
			const problems: Array<string> = [];
			if (analysis.holes.length > 0)
				problems.push(
					`its text still has holes (${analysis.holes.join(', ')})`,
				);
			if (analysis.kind !== 'part')
				problems.push(
					'its text is a fragment, not a statement: end it with `;`',
				);
			if (!sameList([...expected.free], analysis.free))
				problems.push(
					`the composition reads [${listNames(expected.free)}], its text reads [${listNames(analysis.free)}]`,
				);
			if (!sameList([...expected.throws], analysis.throws))
				problems.push(
					`the composition throws [${[...expected.throws].sort().join(', ')}], its text [${analysis.throws.join(', ')}]`,
				);
			const errors = analysis.diagnostics.filter(
				(diagnostic) => diagnostic.severity === 'error',
			);
			for (const error of errors)
				problems.push(
					`${error.message} at ${error.line}:${error.column}`,
				);
			if (problems.length > 0)
				throw new RangeError(`${where}: ${problems.join('; ')}`);
			const writes = analysis.writes ? true : contract.writes;
			const made = make({
				name,
				sql,
				params: Object.fromEntries(
					analysis.free.map((free) => [free, params[free] as Codec]),
				),
				result: contract.result,
				slots: analysis.slots,
				valueSlot: analysis.valueSlot,
				throws: analysis.throws,
				defines: analysis.defines,
				transactional: analysis.transactional,
				tables: analysis.tables,
				functions: analysis.functions,
				openTargets: analysis.openTargets,
				...(Object.keys(analysis.uses).length > 0 && {
					uses: analysis.uses,
				}),
				...(writes !== undefined && { writes }),
			});
			// The key that selected it: its span records it, never a value.
			const variant = describeKey(key);
			const statement: Statement<Params, R, string> =
				variant === '' ? made : { ...made, variant };
			built.set(index, statement);
			return statement;
		};
		if (contract.build === 'eager') for (const key of keys) build(key);
		const select = (key: RuntimeKey) => {
			const extraAxes = Object.keys(key).filter(
				(axis) => !Object.hasOwn(axes, axis),
			);
			if (extraAxes.length > 0)
				throw new RangeError(
					`family \`${name}\` has no axis ${extraAxes.join(', ')}`,
				);
			for (const [axis, values] of Object.entries(axes)) {
				if (!values.includes(key[axis] as AxisValue)) {
					throw new RangeError(
						`family \`${name}\`: ${String(key[axis])} is not a value of the axis \`${axis}\` (${values.map(String).join(', ')})`,
					);
				}
			}
			return build(key);
		};
		return {
			name,
			axes,
			select: select as unknown as Family<A, N, P, R>['select'],
			variants: () =>
				keys.map((key) => ({
					key: key as KeyOf<A>,
					statement: build(key),
				})) as unknown as ReturnType<Family<A, N, P, R>['variants']>,
		};
	},
});
