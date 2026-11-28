/**
 * Negative type fixtures for composition and statement families. Each
 * case's diagnostic is pinned by `test/diagnostics.test.ts`; its first line
 * names the mistake.
 */
import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { run } from '../../src/client/index.js';
import {
	choose,
	compose,
	empty,
	family,
	param,
} from '../../src/statement/index.js';
import { catalog } from '../golden/statements/index.js';
import { body, codecs, listings } from './family.js';

const axes = {
	price: [false, true],
	region: [false, true],
	vector: [false, true],
} as const;
const root = compose(catalog.guarded, { __BODY__: body });
declare const capped: boolean;
declare const vectorAvailable: boolean;

export const fills = () => {
	// @ts-expect-error [family/missing-fill] `__REGION__` has no fill
	compose(catalog.visible, { __PRICE__: empty });
	compose(catalog.visible, {
		__PRICE__: empty,
		__REGION__: empty,
		// @ts-expect-error [family/extra-fill] the template has no `__COLOUR__`
		__COLOUR__: empty,
	});
	compose(catalog.visible, {
		// @ts-expect-error [family/raw-string-fill] raw SurrealQL is not a fill
		__PRICE__: 'AND price < 3',
		__REGION__: empty,
	});
	// @ts-expect-error [family/template-fill] a template must be composed first
	compose(catalog.guarded, { __BODY__: catalog.search });
	compose(catalog.byVector, {
		__WHERE__: empty,
		// @ts-expect-error [family/binding-constant-fill] a constant binds nothing
		__LIMIT__: param('limit', SurrealSchema.Int),
	});
};

declare const either: typeof catalog.priceCap | typeof catalog.regionIs;

export const unionFills = () => {
	compose(catalog.visible, {
		// @ts-expect-error [family/union-typed-fill] which of the two texts is not known to the compiler
		__PRICE__: either,
		__REGION__: empty,
	});
	compose(catalog.visible, {
		__PRICE__: choose('price', {
			true: catalog.priceCap,
			// @ts-expect-error [family/raw-string-option] an option of a choice is a fill too
			false: 'AND TRUE',
		}),
		__REGION__: empty,
	});
};

export const axesAndCodecs = () => {
	family(
		// @ts-expect-error [family/undeclared-axis] `vector` is chosen on but not declared
		{ price: [false, true], region: [false, true] },
		root,
	);
	family(
		// @ts-expect-error [family/unused-axis] nothing chooses on `size`
		{ ...axes, size: ['s', 'm'] },
		root,
	);
	family(
		// @ts-expect-error [family/option-without-text] `choose('price', …)` has texts for true and false only
		{ ...axes, price: ['none', 'capped'] },
		root,
	);
	const { maxPrice: _, ...withoutCap } = codecs;
	family(axes, root).define({
		// @ts-expect-error [family/missing-codec] `$maxPrice` is read by a variant
		params: withoutCap,
		result: Schema.Unknown,
	});
	family(axes, root).define({
		params: {
			...codecs,
			// @ts-expect-error [family/extra-codec] no variant reads `$colour`
			colour: SurrealSchema.String,
		},
		result: Schema.Unknown,
	});
};

export const selections = () => {
	const capOnly = listings.select({
		price: true,
		region: false,
		vector: false,
	});
	// @ts-expect-error [family/select-missing-binding] this variant reads `$maxPrice`
	run(capOnly, { tenant: 't', terms: 'lamp' });
	// @ts-expect-error [family/select-extra-binding] this variant does not read `$region`
	run(capOnly, { tenant: 't', terms: 'lamp', maxPrice: 3, region: 'north' });
	// @ts-expect-error [family/select-misspelt-binding] `maxprice` for `maxPrice`
	run(capOnly, { tenant: 't', terms: 'lamp', maxprice: 3 });
	// @ts-expect-error [family/select-wrong-type] `$maxPrice` is an int
	run(capOnly, { tenant: 't', terms: 'lamp', maxPrice: '3' });
	// @ts-expect-error [family/select-unknown-axis] `colour` is not an axis
	listings.select({ price: true, region: false, vector: false, colour: 1 });
	// @ts-expect-error [family/select-missing-axis] every axis needs a value
	listings.select({ price: true, region: false });
	// @ts-expect-error [family/select-bad-value] `yes` is not a value of `price`
	listings.select({ price: 'yes', region: false, vector: false });
	// A key known only at run time may select the vector variant, so its
	// input must carry the vector.
	const maybeNear = listings.select({
		price: capped,
		region: false,
		vector: vectorAvailable,
	});
	// @ts-expect-error [family/runtime-key-missing-binding] some selectable variant reads `$vector`
	run(maybeNear, { tenant: 't', terms: 'lamp', maxPrice: 3 });
};
