import { Schedule, Schema } from 'effect';

import { bind, type Client, pipeline } from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

const read = make({
	sql: '1;',
	params: {},
	result: Schema.Number,
	writes: false,
});

export const invalidPipelines = (client: Client): void => {
	// @ts-expect-error [pipeline/item-must-be-bound] statements that run together are paired with their input by bind
	client.pipeline([read]);
	// @ts-expect-error [pipeline/retry-is-not-an-option] a pipeline has no retry option
	pipeline([bind(read)], { retry: Schedule.recurs(1) });
};
