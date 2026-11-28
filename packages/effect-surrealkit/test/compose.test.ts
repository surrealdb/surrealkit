/**
 * Composition at run time: `compose` takes exactly one fill per hole and
 * `choose` at least one option, and each fill is a generated text without
 * holes, a composite, a choice, a constant that binds nothing or `empty`.
 * Raw SurrealQL, a template and a binding constant are refused.
 */
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	choose,
	compose,
	empty,
	literal,
	param,
	TextNodeTypeId,
} from '../src/statement/index.js';
import { catalog } from './golden/statements/index.js';

const loose = (value: unknown): never => value as never;

describe('compose and choose', () => {
	it('build the nodes a family renders', () => {
		const price = choose('price', { true: catalog.priceCap, false: empty });
		const visible = compose(catalog.visible, {
			__PRICE__: price,
			__REGION__: empty,
		});
		expect(price[TextNodeTypeId]).toBe('choose');
		expect(price.axis).toBe('price');
		expect(Object.keys(price.options).sort()).toEqual(['false', 'true']);
		expect(visible[TextNodeTypeId]).toBe('compose');
		expect(visible.template).toBe(catalog.visible);
		expect(visible.fills.__PRICE__).toBe(price);
		expect(empty[TextNodeTypeId]).toBe('empty');
		expect(() =>
			compose(catalog.byVector, {
				__WHERE__: visible,
				__LIMIT__: literal(5),
			}),
		).not.toThrow();
	});

	it('refuse a missing fill and a fill for a hole the template does not have', () => {
		expect(() =>
			compose(catalog.visible, loose({ __REGION__: empty })),
		).toThrow('hole __PRICE__ of `catalog.visible` has no fill');
		expect(() =>
			compose(
				catalog.visible,
				loose({
					__PRICE__: empty,
					__REGION__: empty,
					__OTHER__: empty,
				}),
			),
		).toThrow('`catalog.visible` has no hole __OTHER__');
	});

	it('refuse raw SurrealQL, a template and a constant that binds', () => {
		expect(() =>
			compose(
				catalog.visible,
				loose({ __PRICE__: 'AND price < 3', __REGION__: empty }),
			),
		).toThrow(
			'the fill for __PRICE__ is not a text, a composite, a choice, a constant or `empty`; raw SurrealQL strings are refused',
		);
		expect(() =>
			compose(catalog.guarded, loose({ __BODY__: catalog.search })),
		).toThrow(
			'the fill for __BODY__ is a template with holes (__NEAR__, __READINESS__, __WORDS__): compose it first',
		);
		expect(() =>
			compose(
				catalog.byVector,
				loose({
					__WHERE__: empty,
					__LIMIT__: param('limit', SurrealSchema.Int),
				}),
			),
		).toThrow(
			'the fill for __LIMIT__ is a constant that binds limit: a constant fill binds nothing',
		);
	});

	it('refuse a choice without options, or with an option that is not a fill', () => {
		expect(() => choose('price', loose({}))).toThrow(
			"choose('price', …) has no options",
		);
		expect(() =>
			choose(
				'price',
				loose({ true: catalog.priceCap, false: 'AND TRUE' }),
			),
		).toThrow(
			'the text for price = false is not a text, a composite, a choice, a constant or `empty`; raw SurrealQL strings are refused',
		);
	});
});
