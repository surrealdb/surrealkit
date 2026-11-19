/**
 * Statement families at run time: every variant is rendered and analysed
 * on its first selection (or by `variants()`, or eagerly), reads exactly
 * what the compiler says it reads, and `select` returns that variant's
 * statement, named after the family, whose run binds only what it reads.
 * Texts that lie about themselves and codecs that do not match what the
 * variants read are refused when the family is defined.
 */
import { Effect, Schema, Tracer } from 'effect';
import { describe, expect, it } from 'vitest';

import { text } from '../src/analyze/index.js';
import * as SurrealSchema from '../src/index.js';
import { choose, compose, empty, family } from '../src/statement/index.js';
import { fake } from '../src/testing/index.js';
import { catalog } from './golden/statements/index.js';
import {
	body,
	codecs,
	listings,
	sized,
	summary,
	visibility,
} from './types/family.js';

describe('a statement family', () => {
	it('builds one statement per key, each reading exactly what its text reads', () => {
		const variants = listings.variants();
		expect(variants).toHaveLength(8);
		expect(
			new Set(variants.map(({ statement }) => statement.sql)).size,
		).toBe(8);
		for (const { key, statement } of variants) {
			expect(statement.sql).not.toMatch(/__[A-Z]/u);
			const expected = [
				'tenant',
				'terms',
				...(key.price ? ['maxPrice'] : []),
				...(key.region ? ['region'] : []),
				...(key.vector ? ['vector'] : []),
			].sort();
			expect(Object.keys(statement.params).sort()).toEqual(expected);
			expect(statement.throws).toEqual(['No shelf']);
			expect(statement.slots).toBe(1);
		}
	});

	it('leaves an absent filter and an unused branch out of the text', () => {
		const statement = listings.select({
			price: true,
			region: false,
			vector: false,
		});
		expect(statement.sql).toBe(
			[
				'RETURN IF $tenant = NONE {',
				"  { status: 'denied' }",
				'} ELSE {',
				"  LET $shelf = (SELECT id, version FROM ONLY type::record('shelf', $tenant));",
				"  IF $shelf = NONE { THROW 'No shelf' } ELSE {",
				"    { status: 'ready', version: $shelf.version, data: ({",
				'  LET $words = (SELECT id, title FROM listing WHERE shelf = $shelf.id AND price <= $maxPrice  AND title @1@ $terms ORDER BY id LIMIT 10);',
				"  LET $readiness = 'empty';",
				"  LET $near = IF $readiness = 'ready' { ([]) } ELSE { [] };",
				'  { words: $words, near: $near, readiness: $readiness }',
				'}) }',
				'  }',
				'};',
			].join('\n'),
		);
		expect(
			listings.select({ price: false, region: true, vector: true }).sql,
		).toContain('embedding <|5,40|> $vector ORDER BY id LIMIT 5');
	});

	it('selects by a key known only at run time, and refuses one outside the axes', () => {
		const pick = (price: boolean) =>
			listings.select({ price, region: false, vector: false });
		expect(Object.keys(pick(true).params).sort()).toEqual([
			'maxPrice',
			'tenant',
			'terms',
		]);
		expect(Object.keys(pick(false).params).sort()).toEqual([
			'tenant',
			'terms',
		]);
		expect(pick(true)).toBe(pick(true));
		const loose = listings.select as (key: unknown) => unknown;
		expect(() =>
			loose({ price: 'yes', region: false, vector: false }),
		).toThrow('yes is not a value of the axis `price`');
		expect(() =>
			loose({ price: true, region: false, vector: false, colour: 1 }),
		).toThrow('has no axis colour');
	});

	it('binds only what the selected variant reads', async () => {
		const statement = listings.select({
			price: false,
			region: false,
			vector: false,
		});
		const client = fake({ [statement.name]: { status: 'denied' } });
		// A caller holding more values than this variant reads (here as a
		// widened input) sends only the variant's own bindings.
		const input: Record<string, unknown> = {
			tenant: 't',
			terms: 'lamp',
			maxPrice: 3,
		};
		await Effect.runPromise(
			client.run(statement, input as { tenant: string; terms: string }),
		);
		expect(client.calls).toEqual([
			{
				statement: statement.name,
				bindings: { tenant: 't', terms: 'lamp' },
			},
		]);
	});

	it('works out what fills defining names and reading locals leave free', () => {
		const statement = summary.select({});
		expect(Object.keys(statement.params).sort()).toEqual([
			'offset',
			'tenant',
		]);
		expect(statement.sql).toContain('LET $total = array::len($rows);');
		expect(statement.sql).toContain('|$row| string::uppercase($row.title)');
	});

	it('names every variant after the family and records its key on the span, never a value', async () => {
		const statement = listings.select({
			price: true,
			region: false,
			vector: false,
		});
		expect(statement.name).toBe('listings');
		expect(statement.variant).toBe(
			'price=true, region=false, vector=false',
		);
		expect(summary.select({}).variant).toBeUndefined();
		const spans: Array<Tracer.Span> = [];
		const tracer = Tracer.make({
			span: (options) => {
				const span = new Tracer.NativeSpan(options);
				spans.push(span);
				return span;
			},
		});
		await Effect.runPromise(
			fake({ listings: { status: 'denied' } })
				.run(statement, {
					tenant: 'secret-tenant',
					terms: 'lamp',
					maxPrice: 3,
				})
				.pipe(Effect.withTracer(tracer)),
		);
		const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
		expect(spans[0]?.name).toBe('listings');
		expect(attributes).toMatchObject({
			'db.operation.name': 'listings',
			'effect_surrealkit.statement.variant':
				'price=true, region=false, vector=false',
		});
		expect(JSON.stringify(attributes)).not.toMatch(/secret-tenant/);
	});

	it('builds a variant on its first selection, once', () => {
		const counting = family(
			{ size: [1, 2] },
			compose(catalog.guarded, {
				__BODY__: choose('size', {
					1: catalog.noRows,
					2: catalog.notReady,
				}),
			}),
		).define({
			name: 'counting',
			params: { tenant: SurrealSchema.String },
			result: Schema.Unknown,
		});
		const first = counting.select({ size: 1 });
		expect(counting.select({ size: 1 })).toBe(first);
		expect(counting.variants()[0]?.statement).toBe(first);
		expect(counting.variants()[1]?.statement).toBe(
			counting.select({ size: 2 }),
		);
	});

	it('chooses on a numeric axis', () => {
		expect(sized.select({ size: 1 }).sql).toContain('data: ([])');
		expect(sized.select({ size: 2 }).sql).toContain("data: ('empty')");
	});

	it('builds eagerly the same texts it builds on first use', () => {
		const eager = family(
			{
				price: [false, true],
				region: [false, true],
				vector: [false, true],
			},
			compose(catalog.guarded, { __BODY__: body }),
		).define({
			name: 'listings',
			params: codecs,
			result: Schema.Unknown,
			build: 'eager',
		});
		expect(eager.variants().map(({ statement }) => statement.sql)).toEqual(
			listings.variants().map(({ statement }) => statement.sql),
		);
	});
});

describe('what a family refuses', () => {
	const loose = (value: unknown): never => value as never;

	it('a text whose declared names its SQL contradicts', () => {
		const lying = text({
			kind: 'fragment',
			sql: 'AND price <= $ceiling',
			holes: [],
			free: ['maxPrice'],
			defines: [],
			provisional: false,
			slots: 0,
			throws: [],
		});
		expect(() =>
			family(
				{ price: [false, true] },
				compose(catalog.guarded, {
					__BODY__: compose(catalog.byWords, {
						__WHERE__: compose(catalog.visible, {
							__PRICE__: choose('price', {
								true: lying,
								false: empty,
							}),
							__REGION__: empty,
						}),
					}),
				}),
			).define({
				params: loose({
					tenant: SurrealSchema.String,
					terms: SurrealSchema.String,
					maxPrice: SurrealSchema.Int,
				}),
				result: Schema.Unknown,
			}),
		).toThrow('free: declared [maxPrice], the text has [ceiling]');
	});

	it('a template whose declared hole scopes its SQL contradicts', () => {
		const forged = {
			...catalog.guarded,
			scopes: { __BODY__: { bound: [], binds: [] } },
		};
		expect(() =>
			family({}, compose(forged, { __BODY__: catalog.noRows })).define({
				params: loose({ tenant: SurrealSchema.String }),
				result: Schema.Unknown,
			}),
		).toThrow('scopes.__BODY__.bound: declared [], the text has [shelf]');
	});

	it('codecs that do not name exactly what the variants read', () => {
		const root = compose(catalog.guarded, { __BODY__: body });
		const axes = {
			price: [false, true],
			region: [false, true],
			vector: [false, true],
		} as const;
		const { maxPrice: _, ...withoutCap } = codecs;
		expect(() =>
			family(axes, root).define({
				name: 'short',
				params: loose(withoutCap),
				result: Schema.Unknown,
			}),
		).toThrow(
			'the bindings of family `short` do not match the names its variants read; missing: $maxPrice',
		);
		expect(() =>
			family(axes, root).define({
				name: 'long',
				params: loose({ ...codecs, colour: SurrealSchema.String }),
				result: Schema.Unknown,
			}),
		).toThrow('not read by any variant: $colour');
	});

	it('an axis nothing chooses on, and a choice on an axis the family does not declare', () => {
		const root = compose(catalog.guarded, { __BODY__: body });
		expect(() =>
			family(
				loose({
					price: [false, true],
					region: [false, true],
					vector: [false, true],
					size: ['s', 'm'],
				}),
				root,
			).define({ params: loose(codecs), result: Schema.Unknown }),
		).toThrow('axis `size` is never chosen on');
		expect(() =>
			family(
				loose({ price: [false, true], region: [false, true] }),
				root,
			).define({ params: loose(codecs), result: Schema.Unknown }),
		).toThrow(
			"choose('vector', …) names an axis the family does not declare",
		);
	});

	it('a root that renders to a fragment rather than a statement, on first use or eagerly', () => {
		const fragmentFamily = (build: 'eager' | 'lazy') =>
			family(
				{
					price: [false, true],
					region: [false, true],
					vector: [false, true],
				},
				body,
			).define({
				build,
				params: loose({
					terms: SurrealSchema.String,
					maxPrice: SurrealSchema.Int,
					region: SurrealSchema.String,
					vector: codecs.vector,
					shelf: Schema.Unknown,
				}),
				result: Schema.Unknown,
			});
		const lazy = fragmentFamily('lazy');
		expect(() =>
			lazy.select({ price: false, region: false, vector: false }),
		).toThrow(
			'family `family`, variant price=false, region=false, vector=false: its text is a fragment, not a statement',
		);
		expect(() => lazy.variants()).toThrow(
			'its text is a fragment, not a statement',
		);
		expect(() => fragmentFamily('eager')).toThrow(
			'its text is a fragment, not a statement',
		);
	});
});
