import { Context, type Duration, Effect } from 'effect';

import {
	type RequestTiming,
	type StatementTiming,
	timed,
} from '../../src/client/index.js';

class Source extends Context.Service<
	Source,
	{
		readonly read: Effect.Effect<7, 'query failed'>;
	}
>()('TimingTypeSource') {}

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Equal<A, B>): void => undefined;

const required = Source.pipe(Effect.flatMap(({ read }) => read));
const measured = timed(required);
same<
	typeof measured,
	Effect.Effect<
		readonly [7, ReadonlyArray<RequestTiming>],
		'query failed',
		Source
	>
>(true);
same<RequestTiming['statements'][number], StatementTiming>(true);
same<StatementTiming['server'], Duration.Duration | undefined>(true);
same<StatementTiming['rows'], number | undefined>(true);
