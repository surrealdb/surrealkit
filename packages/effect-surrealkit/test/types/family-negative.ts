/**
 * Negative type fixtures for composition and statement families. Each
 * case's diagnostic is pinned by `test/diagnostics.test.ts`; its first line
 * names the mistake.
 */
import * as SurrealSchema from 'effect-surrealkit';

import { choose, compose, empty, param } from '../../src/statement/index.js';
import { catalog } from '../golden/statements/index.js';

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
