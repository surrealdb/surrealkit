/**
 * Type fixtures for statement families: the bindings of each variant are
 * exactly what its text reads, a key known only at run time is typed as
 * the union of what its variants read, and `run` checks an input against
 * the selected statement. Compiled by `tsc -p test/types`.
 */
import type { Effect } from 'effect';

import type { SurrealClient } from '../../src/client/index.js';
import { run } from '../../src/client/index.js';
import type { Input, Thrown } from '../../src/statement/index.js';
import { listings, summary } from './family.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

// A key known at compile time: exactly the names that variant reads.
const plain = listings.select({ price: false, region: false, vector: false });
same<keyof Input<(typeof plain)['params']>, 'tenant' | 'terms'>(true);

const near = listings.select({ price: true, region: false, vector: true });
same<
	keyof Input<(typeof near)['params']>,
	'tenant' | 'terms' | 'maxPrice' | 'vector'
>(true);
same<Thrown<typeof near>, 'No shelf'>(true);

// `$shelf` is a local of the guard, `$readiness` and `$words` locals of the
// search, so none of them is a binding of any variant.
same<keyof (typeof near)['params'], 'tenant' | 'terms' | 'maxPrice' | 'vector'>(
	true,
);

// A key known only at run time: every name some selectable variant reads.
declare const capped: boolean;
const either = listings.select({ price: capped, region: false, vector: false });
same<keyof (typeof either)['params'], 'tenant' | 'terms' | 'maxPrice'>(true);
// `maxPrice` is required: the variant selected may read it.
same<
	Input<(typeof either)['params']>,
	{
		readonly tenant: string;
		readonly terms: string;
		readonly maxPrice: number;
	}
>(true);

// The summary's fills read a closure parameter (`$row`), a local (`$rows`)
// and define `$total`, which its template reads after the fill: only
// `$tenant` and `$offset` are bindings.
const counted = summary.select({});
same<keyof (typeof counted)['params'], 'tenant' | 'offset'>(true);

export const searches: ReadonlyArray<
	Effect.Effect<unknown, unknown, SurrealClient>
> = [
	run(plain, { tenant: 't', terms: 'lamp' }),
	run(near, { tenant: 't', terms: 'lamp', maxPrice: 40, vector: [0.1, 0.2] }),
	run(either, { tenant: 't', terms: 'lamp', maxPrice: 40 }),
	run(counted, { tenant: 't', offset: 1 }),
];
