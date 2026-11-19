/**
 * Type fixtures for modules `effect-surrealkit statements` generates: the
 * literal names of a generated part make a missing or extra binding a
 * compile error through `sequence(...).define`, and a template's holes
 * type its fills. Compiled by `tsc -p test/types`.
 */
import type { Effect } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { type FillError, fill } from '../../src/analyze/index.js';
import {
	type Input,
	type Result,
	sequence,
	type Thrown,
} from '../../src/statement/index.js';
import { accounts, counter } from '../golden/statements/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// The names survive as literals.
same<(typeof accounts.bindOwner.free)[number], 'boundOwner'>(true);
same<(typeof accounts.bindOwner.defines)[number], 'owner' | 'principal'>(true);
same<(typeof counter.step.throws)[number], 'Too large'>(true);

export const profile = sequence(
	accounts.bindOwner,
	accounts.requireOwner,
	accounts.readProfile,
).define({
	name: 'read own profile',
	params: { boundOwner: SurrealSchema.RecordIdAny },
	result: SurrealSchema.String,
});
same<Thrown<typeof profile>, 'Not the owner'>(true);
same<keyof Input<(typeof profile)['params']>, 'boundOwner'>(true);
same<Result<typeof profile>, string>(true);

export const step = sequence(counter.step).define({
	params: { boundStart: SurrealSchema.Int },
	result: SurrealSchema.Int,
});

export const rejected = () => {
	sequence(accounts.bindOwner, accounts.readProfile).define({
		// @ts-expect-error [module/sequence-missing-binding] `$boundOwner` has no codec
		params: {},
		result: SurrealSchema.String,
	});
	sequence(accounts.bindOwner, accounts.readProfile).define({
		params: {
			boundOwner: SurrealSchema.RecordIdAny,
			// @ts-expect-error [module/sequence-extra-binding] bindOwner defines `$owner`, so nothing reads it from the caller
			owner: SurrealSchema.RecordIdAny,
		},
		result: SurrealSchema.String,
	});
	// Without bindOwner, the guard reads `$owner` and `$principal` from the caller.
	sequence(accounts.requireOwner).define({
		// @ts-expect-error [module/sequence-missing-principal] `$principal` is missing
		params: { owner: SurrealSchema.RecordIdAny },
		result: SurrealSchema.String,
	});
};

// A template takes exactly one fill per hole.
export const filled: Effect.Effect<string, FillError> = fill(
	accounts.listWhere,
	{ __FILTER__: accounts.ownerFilter },
);
export const unfilled = () =>
	// @ts-expect-error [module/template-missing-fill] `__FILTER__` has no fill
	fill(accounts.listWhere, {});
