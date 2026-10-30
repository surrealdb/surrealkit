/** `set<T>` / `set<T, N>` and `range`. */
import {
	Effect,
	Schema,
	SchemaGetter,
	SchemaIssue,
	SchemaParser,
} from 'effect';
import {
	type Bound,
	BoundExcluded,
	BoundIncluded,
	equals,
	Range as SdkRange,
} from 'surrealdb';
import { Int } from './int.js';
import { runtimeCode } from './internal/code.js';
import { leaf, ok, wrongType } from './internal/declare.js';

/** How set elements are compared for uniqueness. */
export type SetEquality = 'surreal' | 'reference';

export interface SetOptions {
	/** `set<T, N>`: the exact number of distinct elements. */
	readonly length?: number;
	/**
	 * `"surreal"` (default) compares elements with SurrealDB value equality
	 * (the SDK's `equals`), so two equal record ids are duplicates;
	 * `"reference"` uses JS `Set` identity.
	 */
	readonly equality?: SetEquality;
}

const duplicateIndex = (
	values: ReadonlyArray<unknown>,
	equality: SetEquality,
): number => {
	if (equality === 'reference') return -1;
	for (let i = 1; i < values.length; i++) {
		for (let j = 0; j < i; j++) {
			if (equals(values[i], values[j])) return i;
		}
	}
	return -1;
};

/**
 * `set<T>`: a `ReadonlySet` whose elements decode through `item`. The SDK
 * decodes SurrealQL sets as JS `Set`s and encodes `Set`s as SurrealQL sets.
 *
 * Both directions require distinct elements (under `equality`) and, with
 * `length`, exactly that many: SurrealDB 3.3.0 aborts the server on a
 * duplicate-bearing write to an `option<set<T, N>>` field, so sets are only
 * ever sent deduplicated and of the exact length. Distinctness is checked on
 * the input of each direction, so a lossy item codec (`DateTimeEffect`, at
 * millisecond precision) decodes a stored set whose elements differ below a
 * millisecond, and refuses to encode two elements that would be stored as one.
 */
export const SetOf = <S extends Schema.Top>(
	item: S,
	options: SetOptions = {},
) => {
	const equality = options.equality ?? 'surreal';
	const length = options.length;
	const optionsCode = [
		...(length === undefined ? [] : [`length: ${length}`]),
		...(equality === 'surreal' ? [] : [`equality: "${equality}"`]),
	];
	return Schema.declareConstructor<
		ReadonlySet<S['Type']>,
		ReadonlySet<S['Encoded']>
	>()(
		[item],
		([itemCodec]) =>
			(input, ast, parseOptions) => {
				if (!(input instanceof Set)) {
					return Effect.fail(
						new SchemaIssue.InvalidType(ast, input, parseOptions),
					);
				}
				const elements = [...input];
				return Effect.flatMap(
					Effect.forEach(elements, (element, index) =>
						Effect.mapError(
							SchemaParser.decodeUnknownEffect(itemCodec)(
								element,
								parseOptions,
							),
							(issue) =>
								new SchemaIssue.Composite(
									ast,
									[new SchemaIssue.Pointer([index], issue)],
									input,
									parseOptions,
								),
						),
					),
					(decoded) => {
						// Distinct as given, not as decoded: a lossy item codec
						// (datetimes at millisecond precision) may map two distinct
						// stored values to equal ones.
						const duplicate = duplicateIndex(elements, equality);
						if (duplicate !== -1) {
							return Effect.fail(
								new SchemaIssue.InvalidValue(
									{
										message: `set element ${duplicate} duplicates an earlier element; SurrealDB sets hold distinct values`,
									},
									input,
									parseOptions,
								),
							);
						}
						if (length !== undefined && decoded.length !== length) {
							return Effect.fail(
								new SchemaIssue.InvalidValue(
									{
										message: `expected a set of exactly ${length} elements, got ${decoded.length}`,
									},
									input,
									parseOptions,
								),
							);
						}
						return Effect.succeed(
							new Set(decoded) as ReadonlySet<S['Type']>,
						);
					},
				);
			},
		{
			expected:
				length === undefined ? 'a set' : `a set of ${length} elements`,
			representation: {
				id: 'effect-surrealkit/SetOf',
				payload: { length: length ?? null, equality },
			},
			toCode: ({ typeParameters: [itemCode] }) =>
				runtimeCode(
					`SetOf(${itemCode?.runtime ?? ''}${optionsCode.length === 0 ? '' : `, { ${optionsCode.join(', ')} }`})`,
					`ReadonlySet<${itemCode?.Type ?? 'unknown'}>`,
				),
			toEquivalence: () => (self, that) =>
				self.size === that.size &&
				[...self].every((a) => [...that].some((b) => equals(a, b))),
			toCodecArbitrary: ({ typeParameters: [itemType] }) =>
				Schema.link<ReadonlySet<S['Type']>>()(
					Schema.Array(itemType).check(
						length === undefined
							? Schema.isMaxLength(8)
							: Schema.isBetweenLength(length, length),
					),
					{
						decode: SchemaGetter.transform(
							(values) =>
								new Set(values) as ReadonlySet<S['Type']>,
						),
						encode: SchemaGetter.transform((set) => [...set]),
					},
				),
		},
	);
};

/** A range bound as the SDK models it. */
export type RangeBound<T> = Bound<T>;

const boundSource = Schema.Union([
	Schema.Struct({ included: Schema.Boolean, value: Int }),
	Schema.Undefined,
]);

const toBound = (source: typeof boundSource.Type): RangeBound<number> =>
	source === undefined
		? undefined
		: source.included
			? new BoundIncluded(source.value)
			: new BoundExcluded(source.value);

const isBound = (bound: unknown): boolean =>
	bound === undefined ||
	bound instanceof BoundIncluded ||
	bound instanceof BoundExcluded;

/**
 * `range`: an SDK `Range` with any bound values (schema document v2 does not
 * parameterise ranges).
 */
export const Range = leaf<SdkRange<unknown, unknown>>({
	id: 'effect-surrealkit/Range',
	expected: 'a SurrealDB range',
	code: runtimeCode('Range', 'Surreal.Range<unknown, unknown>'),
	parse: (input) =>
		input instanceof SdkRange && isBound(input.begin) && isBound(input.end)
			? ok(input)
			: wrongType,
	equivalence: (self, that) => self.equals(that),
	arbitrary: () =>
		Schema.link<SdkRange<unknown, unknown>>()(
			Schema.Tuple([boundSource, boundSource]),
			{
				decode: SchemaGetter.transform(
					([begin, end]) =>
						new SdkRange(toBound(begin), toBound(end)),
				),
				encode: SchemaGetter.forbidden(
					() => 'range arbitraries only generate values',
				),
			},
		),
});
