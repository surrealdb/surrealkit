import {
	Effect,
	Schema,
	SchemaAST,
	SchemaGetter,
	SchemaIssue,
	SchemaParser,
} from 'effect';

import type { CodeOutput } from './code.js';

/** Outcome of checking one input value. */
export type Parsed<T> =
	| { readonly ok: true; readonly value: T }
	| { readonly ok: false; readonly message?: string };

export const ok = <T>(value: T): Parsed<T> => ({ ok: true, value });

/** Wrong runtime type: reported as `Expected <expected>`. */
export const wrongType: Parsed<never> = { ok: false };

/** Right type, unacceptable value: reported with `message`. */
export const invalid = (message: string): Parsed<never> => ({
	ok: false,
	message,
});

export interface LeafOptions<T> {
	/** Persistence identity, `effect-surrealkit/<Name>`. */
	readonly id: string;
	readonly payload?: Schema.Json;
	readonly expected: string;
	readonly code: CodeOutput;
	/**
	 * Checks (and, for declarations whose encoded form differs, normalises)
	 * an input. It runs for both decoding and encoding, so it must accept the
	 * decoded type unchanged.
	 */
	readonly parse: (input: unknown) => Parsed<T>;
	/**
	 * The link arbitrary generation starts from. `constraint` carries the bounds
	 * of the checks on the declaration (`S.Int.check(Schema.isBetween(…))`):
	 * apply it to the target with {@link constrained}, or generation can only
	 * filter, and a narrow check exhausts the generator.
	 */
	readonly arbitrary?: (
		constraint: GenerationConstraint | undefined,
	) => SchemaAST.Link;
	readonly equivalence?: (self: T, that: T) => boolean;
}

export type GenerationConstraint =
	Schema.Annotations.ToArbitrary.GenerationConstraint;

/**
 * `target` carrying `constraint` for arbitrary generation only: the added
 * filter accepts everything, so validation is unchanged.
 */
export const constrained = <S extends Schema.Top>(
	target: S,
	constraint: GenerationConstraint | undefined,
): S =>
	constraint === undefined
		? target
		: (target.check(
				Schema.makeFilter<S['Type']>(() => true, {
					arbitraryConstraint: constraint,
				}),
			) as unknown as S);

/**
 * A non-parametric declaration for one SDK value kind.
 *
 * `E` widens the accepted encoded input where the SDK hands back a different
 * runtime shape than the one the schema exposes (bytes, integers).
 */
export const leaf = <T, E = T>(options: LeafOptions<T>) =>
	Schema.declareConstructor<T, E>()(
		[],
		() => (input, ast, parseOptions) => {
			const parsed = options.parse(input);
			if (parsed.ok) return Effect.succeed(parsed.value);
			return Effect.fail(
				parsed.message === undefined
					? new SchemaIssue.InvalidType(ast, input, parseOptions)
					: new SchemaIssue.InvalidValue(
							{ message: parsed.message },
							input,
							parseOptions,
						),
			);
		},
		{
			expected: options.expected,
			representation: {
				id: options.id,
				payload: options.payload ?? null,
			},
			toCode: () => options.code,
			...(options.arbitrary && {
				toCodecArbitrary: ({ constraint }) =>
					(
						options.arbitrary as NonNullable<
							typeof options.arbitrary
						>
					)(constraint as GenerationConstraint | undefined),
			}),
			...(options.equivalence && {
				toEquivalence: () =>
					options.equivalence as (a: T, b: T) => boolean,
			}),
		},
	);

/**
 * Wraps a composed codec (one built from transformations) in a single
 * declaration node, so it carries its own `toCode` and prints by name.
 *
 * Decoding runs the inner codec forwards. Encoding works because flipping a
 * declaration flips its type parameters: the encoding parser receives the
 * flipped inner codec, whose decoder is the inner encoder. `preprocess` runs
 * on the decoding side only, so encoding is exactly the inner codec's.
 */
export const wrap = <S extends Schema.Top>(
	inner: S,
	annotations: {
		readonly id: string;
		readonly payload?: Schema.Json;
		readonly expected: string;
		readonly code: (
			typeParameters: ReadonlyArray<{
				readonly runtime: string;
				readonly Type: string;
			}>,
		) => CodeOutput;
		readonly equivalence?: (self: S['Type'], that: S['Type']) => boolean;
		/** Applied to the input before the inner codec, when decoding only. */
		readonly preprocess?: (input: unknown) => unknown;
		/** Replaces the arbitrary derived from the wrapped codec's type side. */
		readonly arbitrary?: () => SchemaAST.Link;
	},
) => {
	const preprocess = annotations.preprocess ?? ((input: unknown) => input);
	const parser =
		(before: (input: unknown) => unknown): DeclarationRun =>
		(typeParameters) => {
			// The one type parameter: `inner`, flipped when encoding.
			const codec = Schema.make<Schema.Codec<unknown, unknown>>(
				typeParameters[0] ?? inner.ast,
			);
			return (input, _ast, parseOptions) =>
				SchemaParser.decodeUnknownEffect(codec)(
					before(input),
					parseOptions,
				);
		};
	const declaration: Schema.Annotations.Declaration<S['Type'], readonly [S]> =
		{
			expected: annotations.expected,
			representation: {
				id: annotations.id,
				payload: annotations.payload ?? null,
			},
			toCode: ({ typeParameters }) => annotations.code(typeParameters),
			// Arbitraries and equivalences come from the wrapped codec.
			toCodecArbitrary: ({ typeParameters: [type] }) =>
				annotations.arbitrary?.() ??
				Schema.link<S['Type']>()(type, {
					decode: identityGetter(),
					encode: identityGetter(),
				}),
			toEquivalence: ([innerEquivalence]) =>
				annotations.equivalence ?? innerEquivalence,
		};
	// `Schema.declareConstructor` reuses one parser in both directions; the
	// declaration is built directly to give encoding its own (no preprocess).
	return Schema.make<
		Schema.declareConstructor<S['Type'], S['Encoded'], readonly [S]>
	>(
		new SchemaAST.Declaration(
			[inner.ast],
			parser(preprocess),
			declaration,
			undefined,
			undefined,
			undefined,
			undefined,
			parser((input) => input),
		),
	);
};

type DeclarationRun = ConstructorParameters<typeof SchemaAST.Declaration>[1];

const identityGetter = <T>() => SchemaGetter.transform<T, T>((value) => value);

/** True for objects created by `{}` / `Object.create(null)` (not class instances). */
export const isPlainObject = (
	value: unknown,
): value is Record<string, unknown> => {
	if (typeof value !== 'object' || value === null) return false;
	const proto = Object.getPrototypeOf(value);
	return proto === Object.prototype || proto === null;
};
