/**
 * `.surql` files imported as modules: the content mapper gives TypeScript
 * each one's module, and these compile against them. The `@ts-expect-error`
 * lines hold only when the mapper's module carries the schema's `binds`, so
 * they show that the schema document in the options reached the check.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';
import { sequence } from 'effect-surrealkit/statement';

import { bindOwner } from '../queries/accounts/bindOwner.surql';
import { readProfile } from '../queries/accounts/readProfile.surql';
import { record } from '../queries/gauges/record.surql';
import { site } from '../queries/gauges/site.surql';

export const profile = sequence(bindOwner, readProfile).define({
	params: { boundOwner: SurrealSchema.RecordIdAny },
	result: SurrealSchema.String,
});

export const recorded = sequence(record).define({
	params: {
		raw: SurrealSchema.Float,
		site: SurrealSchema.RecordIdFor('site'),
		station: SurrealSchema.String,
		tags: Schema.Unknown,
	},
	result: Schema.Unknown,
});

export const misfit = () => {
	sequence(site).define({
		// @ts-expect-error the key of `site` is a string, by the schema
		params: { siteKey: SurrealSchema.Int },
		result: SurrealSchema.String,
	});
	sequence(record).define({
		// @ts-expect-error `site` holds a record, not a string, by the schema
		params: {
			raw: SurrealSchema.Float,
			site: SurrealSchema.String,
			station: SurrealSchema.String,
			tags: Schema.Unknown,
		},
		result: Schema.Unknown,
	});
	sequence(bindOwner, readProfile).define({
		// @ts-expect-error `$boundOwner` has no codec
		params: {},
		result: SurrealSchema.String,
	});
};
