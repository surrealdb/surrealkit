import { Arbitrary, Effect, Schema } from 'effect';
import { expect } from 'vitest';

/**
 * Generates decoded values from `schema` with `Arbitrary.schema`, sends each
 * through `transport` as its encoded form and decodes the result, and
 * requires the property check to report `Passed`.
 */
export const expectRoundTrip = async <T, E>(
	schema: Schema.Codec<T, E>,
	transport: (encoded: E) => unknown | Promise<unknown>,
	options: { readonly runs?: number; readonly seed?: number } = {},
): Promise<void> => {
	const equivalent = Schema.toEquivalence(schema);
	const encode = Schema.encodeSync(schema);
	const decode = Schema.decodeUnknownSync(schema);
	const result = await Effect.runPromise(
		Arbitrary.checkEffect(
			Arbitrary.schema(schema),
			(value) =>
				Effect.promise(async () => {
					const back = decode(await transport(encode(value)));
					return equivalent(value, back);
				}),
			{ runs: options.runs ?? 100, seed: options.seed ?? 20261024 },
		),
	);
	expect(Arbitrary.formatCheckFailure(result) ?? 'Passed').toBe('Passed');
	expect(result._tag).toBe('Passed');
};
