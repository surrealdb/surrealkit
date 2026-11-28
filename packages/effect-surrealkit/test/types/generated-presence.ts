/**
 * Type fixtures for a module `effect-surrealkit statements --schema`
 * generates from the engine's own document for the field-presence corpus
 * (`compiled/field-presence.v2.json`): a binding assigned to a
 * `TYPE option<string>` field is bound as `option<string>`, so a codec
 * narrower than the field that can hold NONE fits it, and one assigned to
 * a `TYPE string` field stays `string`. Compiled by `tsc -p test/types`.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { sequence } from '../../src/statement/index.js';
import { presence } from '../golden/statements-presence/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

same<(typeof presence.assign.binds)['opt'], 'option<string>'>(true);
same<(typeof presence.assign.binds)['req'], 'string'>(true);

// The engine accepts both `'a'` and NONE for `opt`.
export const assigned = sequence(presence.assign).define({
	params: {
		opt: Schema.UndefinedOr(Schema.Literal('a')),
		req: SurrealSchema.String,
	},
	result: Schema.Unknown,
});

export const misfit = () => {
	sequence(presence.assign).define({
		// @ts-expect-error [module/presence-required-narrow-undefined-or] `req` is `TYPE string`: its kind stays `string`
		params: {
			opt: SurrealSchema.String,
			req: Schema.UndefinedOr(Schema.Literal('a')),
		},
		result: Schema.Unknown,
	});
};
