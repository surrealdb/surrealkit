import type { Client } from '../../src/client/index.js';
import * as S from '../../src/index.js';
import { eq, gte, live, param } from '../../src/statement/index.js';
import { PersonTable } from '../golden/corpus/index.js';

const builder = live(PersonTable);
const statement = builder
	.where((row) => gte(row.age, param('minimum', S.Int)))
	.build('people');

export const invalidLive = (client: Client): void => {
	// @ts-expect-error [live/where-comparison] WHERE rejects an operand incompatible with its field
	builder.where((row) => eq(row.name, 12));
	// @ts-expect-error [live/project-expression] projection fields must be expressions
	builder.project(() => ({ field: 42 }));
	// @ts-expect-error [live/order-by] a live query has no ORDER BY
	builder.orderBy((row) => row.name);
	// @ts-expect-error [live/limit] a live query has no LIMIT
	builder.limit(1);
	// @ts-expect-error [live/group-by] a live query has no GROUP BY
	builder.groupBy((row) => row.name);
	// @ts-expect-error [live/only] a live query has no ONLY
	builder.only();
	// @ts-expect-error [live/required-binding] the named parameter must be supplied
	client.live(statement);
};
