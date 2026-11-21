/**
 * Type fixtures for a module `effect-surrealkit statements --schema`
 * generates: each part's `binds` kind checks the codec its binding is
 * declared with in `sequence(...).define`. Compiled by `tsc -p test/types`.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { sequence } from '../../src/statement/index.js';
import { gauges } from '../golden/statements-binds/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

same<(typeof gauges.readings.binds)['station'], 'option<string>'>(true);
same<(typeof gauges.record.binds)['station'], 'string'>(true);
same<(typeof gauges.record.binds)['site'], 'record<site>'>(true);
same<
	(typeof gauges.record.rejectsNone)['station'],
	{
		readonly table: 'gauge';
		readonly field: 'station';
		readonly kind: 'string';
	}
>(true);

export const readings = sequence(gauges.readings).define({
	params: {
		floor: SurrealSchema.Float,
		station: SurrealSchema.String.check(Schema.isMinLength(1)),
	},
	result: Schema.Array(SurrealSchema.Float),
});

export const recorded = sequence(gauges.record).define({
	params: {
		raw: SurrealSchema.Float,
		site: SurrealSchema.RecordIdFor('site'),
		station: SurrealSchema.String.pipe(Schema.brand('app/Station')),
		// `tags` is an array field: not a simple kind, so not checked.
		tags: Schema.Unknown,
	},
	result: Schema.Unknown,
});

export const misfit = () => {
	sequence(gauges.site).define({
		// @ts-expect-error [module/binds-wrong-representation] the key of `site` is a string
		params: { siteKey: SurrealSchema.Int },
		result: SurrealSchema.String,
	});
	sequence(gauges.record).define({
		// @ts-expect-error [module/binds-wrong-record] `site` holds a record, not a string
		params: {
			raw: SurrealSchema.Float,
			site: SurrealSchema.String,
			station: SurrealSchema.String,
			tags: Schema.Unknown,
		},
		result: Schema.Unknown,
	});
	sequence(gauges.record).define({
		// @ts-expect-error [module/none-assigned] `station` is created as a string: NONE is rejected
		params: {
			raw: SurrealSchema.Float,
			site: SurrealSchema.RecordIdFor('site'),
			station: Schema.UndefinedOr(SurrealSchema.String),
			tags: Schema.Unknown,
		},
		result: Schema.Unknown,
	});
};
