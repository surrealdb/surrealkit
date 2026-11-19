/**
 * The statement families the type fixtures and tests share: a guarded
 * listing search over the `catalog` templates, with three axes (a price
 * cap, a region filter and a nearest-neighbour branch), and a summary with
 * no axes whose fills read a closure parameter and define names a later
 * statement reads.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import {
	choose,
	compose,
	empty,
	family,
	literal,
} from '../../src/statement/index.js';
import { catalog } from '../golden/statements/index.js';

export const visibility = compose(catalog.visible, {
	__PRICE__: choose('price', { true: catalog.priceCap, false: empty }),
	__REGION__: choose('region', { true: catalog.regionIs, false: empty }),
});

export const body = compose(catalog.search, {
	__WORDS__: compose(catalog.byWords, { __WHERE__: visibility }),
	__READINESS__: choose('vector', {
		true: catalog.readiness,
		false: catalog.notReady,
	}),
	__NEAR__: choose('vector', {
		true: compose(catalog.byVector, {
			__WHERE__: visibility,
			__LIMIT__: literal(5),
		}),
		false: catalog.noRows,
	}),
});

export const codecs = {
	tenant: SurrealSchema.String,
	terms: SurrealSchema.String,
	maxPrice: SurrealSchema.Int,
	region: SurrealSchema.String,
	vector: Schema.Array(SurrealSchema.Float),
};

export const listings = family(
	{ price: [false, true], region: [false, true], vector: [false, true] },
	compose(catalog.guarded, { __BODY__: body }),
).define({ name: 'listings', params: codecs, result: Schema.Unknown });

export const summary = family(
	{},
	compose(catalog.guarded, {
		__BODY__: compose(catalog.summary, {
			__COUNTS__: catalog.counts,
			__TITLE__: catalog.rowTitle,
		}),
	}),
).define({
	name: 'summary',
	params: { tenant: SurrealSchema.String, offset: SurrealSchema.Int },
	result: Schema.Unknown,
});

/** A family on a numeric axis: `{ 1: …, 2: … }` is keyed by numbers. */
export const sized = family(
	{ size: [1, 2] },
	compose(catalog.guarded, {
		__BODY__: choose('size', { 1: catalog.noRows, 2: catalog.notReady }),
	}),
).define({
	name: 'sized',
	params: { tenant: SurrealSchema.String },
	result: Schema.Unknown,
});
