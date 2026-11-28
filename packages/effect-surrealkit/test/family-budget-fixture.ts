/**
 * Writes the family budget fixture: `count` copies of a family shaped like
 * `listings` in `test/types/family.ts` (a guard whose local a fill reads,
 * a search of three holes, a visibility clause of two optional filters,
 * three axes and so eight variants), each declared with `text` and names
 * of its own so no copy shares a type with another. Each copy selects one
 * variant by a literal key and one by a key known only at run time, and
 * runs both. The text is a pure function of `count`, so the compiler's
 * counters for it are deterministic.
 */
const header = `import { Schema } from 'effect';
import * as SurrealSchema from 'effect-surrealkit';

import { run } from '../../src/client/index.js';
import { text } from '../../src/analyze/index.js';
import { choose, compose, empty, family, literal } from '../../src/statement/index.js';

export const unused = [Schema, SurrealSchema, run, text, choose, compose, empty, family, literal] as const;
declare const flag: boolean;
`;

const none = '{ bound: [], binds: [] }';

const fragment = (
	sql: string,
	free: ReadonlyArray<string>,
	holes: ReadonlyArray<string> = [],
	scopes = '',
) =>
	`text({ kind: 'fragment', sql: ${JSON.stringify(sql)}, holes: ${JSON.stringify(holes)}, free: ${JSON.stringify(free)}, defines: [], provisional: ${holes.length > 0}, slots: 0, throws: []${scopes === '' ? '' : `, scopes: ${scopes}`} })`;

const listing = (
	i: number,
): string => `const visible${i} = compose(${fragment('shelf = $shelf.id __PRICE__ __REGION__', ['shelf'], ['__PRICE__', '__REGION__'], `{ __PRICE__: ${none}, __REGION__: ${none} }`)}, {
	__PRICE__: choose('price', { true: ${fragment(`AND price <= $maxPrice${i}`, [`maxPrice${i}`])}, false: empty }),
	__REGION__: choose('region', { true: ${fragment(`AND region = $region${i}`, [`region${i}`])}, false: empty }),
});
const body${i} = compose(${fragment(`{ LET $words = (__WORDS__); LET $readiness = __READINESS__; LET $near = IF $readiness = 'ready' { (__NEAR__) } ELSE { [] }; { words: $words, near: $near } }`, [], ['__NEAR__', '__READINESS__', '__WORDS__'], `{ __NEAR__: { bound: ['readiness', 'words'], binds: [] }, __READINESS__: { bound: ['words'], binds: [] }, __WORDS__: ${none} }`)}, {
	__WORDS__: compose(${fragment(`SELECT id FROM listing WHERE __WHERE__ AND title @1@ $terms${i} LIMIT 10`, [`terms${i}`], ['__WHERE__'], `{ __WHERE__: ${none} }`)}, { __WHERE__: visible${i} }),
	__READINESS__: choose('vector', { true: ${fragment("IF $shelf.version > 0 { 'ready' } ELSE { 'empty' }", ['shelf'])}, false: ${fragment("'empty'", [])} }),
	__NEAR__: choose('vector', {
		true: compose(${fragment(`SELECT id FROM listing WHERE __WHERE__ AND embedding <|5,40|> $vector${i} LIMIT __LIMIT__`, [`vector${i}`], ['__LIMIT__', '__WHERE__'], `{ __LIMIT__: ${none}, __WHERE__: ${none} }`)}, { __WHERE__: visible${i}, __LIMIT__: literal(5) }),
		false: ${fragment('[]', [])},
	}),
});
export const listings${i} = family(
	{ price: [false, true], region: [false, true], vector: [false, true] },
	compose(text({ kind: 'part', sql: ${JSON.stringify(`RETURN IF $tenant${i} = NONE { 1 } ELSE { LET $shelf = (SELECT id FROM ONLY type::record('shelf', $tenant${i})); { data: (__BODY__) } };`)}, holes: ['__BODY__'], free: ['tenant${i}'], defines: [], provisional: true, slots: 1, throws: [], scopes: { __BODY__: { bound: ['shelf'], binds: [] } } }), { __BODY__: body${i} }),
).define({
	name: 'listings${i}',
	params: { tenant${i}: SurrealSchema.String, terms${i}: SurrealSchema.String, maxPrice${i}: SurrealSchema.Int, region${i}: SurrealSchema.String, vector${i}: Schema.Array(SurrealSchema.Float) },
	result: Schema.Unknown,
});
export const runs${i} = [
	run(listings${i}.select({ price: true, region: false, vector: true }), { tenant${i}: 't', terms${i}: 'lamp', maxPrice${i}: 3, vector${i}: [0.1] }),
	run(listings${i}.select({ price: flag, region: flag, vector: false }), { tenant${i}: 't', terms${i}: 'lamp', maxPrice${i}: 3, region${i}: 'north' }),
];
`;

/** The fixture's text: the imports, then `count` families. */
export const familyBudgetFixture = (count: number): string =>
	[header, ...Array.from({ length: count }, (_, i) => listing(i))].join('\n');
