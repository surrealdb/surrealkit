/**
 * The workload includes distinct parameter names, result properties and
 * declared throws. This prevents TypeScript from reusing one instantiation
 * for every statement or bulk call.
 */
const imports = `import { Effect, Schema, Stream } from 'effect';

import { bind, type Client, pipeline } from '../../src/client/index.js';
import { make } from '../../src/statement/index.js';

export declare const client: Client;
export const buildingBlocks = [bind, pipeline, make, Effect, Schema, Stream] as const;
`;

const pipeline = (index: number): string => {
	const members = Array.from({ length: 8 }, (_, member) => {
		const suffix = `${index}_${member}`;
		return {
			declaration: `const statement${suffix} = make({
	name: 'lookup ${suffix}',
	sql: 'SELECT VALUE $input${suffix} FROM ONLY item:one;',
	params: { input${suffix}: Schema.Number },
	result: Schema.Struct({ result${suffix}: Schema.String }),
	throws: ['missing ${suffix}'],
	writes: false,
});`,
			bound: `bind(statement${suffix}, { input${suffix}: ${member} })`,
		};
	});
	return `${members.map(({ declaration }) => declaration).join('\n')}
export const pipeline${index} = pipeline([
	${members.map(({ bound }) => bound).join(',\n\t')},
], { each: '${index % 2 === 0 ? 'statement' : 'transaction'}' });`;
};

const bulk = (index: number): string => {
	const row = `{ n: ${index}, label: 'bulk ${index}' } as const`;
	const input = index % 2 === 0 ? `[${row}]` : `Stream.make(${row})`;
	return `const write${index} = make({
	name: 'write ${index}',
	sql: 'FOR $row IN $rows${index} { CREATE item CONTENT $row RETURN NONE; };',
	params: {
		rows${index}: Schema.Array(Schema.Struct({
			n: Schema.Number, label: Schema.Literal('bulk ${index}'),
		})),
		seed${index}: Schema.String,
	},
	result: Schema.Undefined,
	throws: ['write failed ${index}'],
	writes: true,
});
export const batch${index} = client.bulk(write${index}, {
	rows${index}: ${input},
	seed${index}: 'unchanged',
}, {
	over: 'rows${index}',
	chunk: 250,
	beforeChunk: () => Effect.fail('stop ${index}' as const),
});`;
};

export const executionBudgetFixture = (
	kind: 'pipeline' | 'bulk',
	count: number,
): string =>
	[
		imports,
		...Array.from({ length: count }, (_, index) =>
			kind === 'pipeline' ? pipeline(index) : bulk(index),
		),
	].join('\n');
