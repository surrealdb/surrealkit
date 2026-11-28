/**
 * The checker counters reported by TypeScript's --extendedDiagnostics,
 * minus the same imports without a workload. Elapsed time is not a gate.
 */
import { join } from 'node:path';

import ts from 'typescript';
import { beforeAll, describe, expect, it } from 'vitest';

import { compile, packageRoot, typesRoot } from './compiler.js';
import { executionBudgetFixture } from './execution-budget-fixture.js';

const count = 30;
// TypeScript 5.9.3 and Effect 4.1.5 measured 118,430 instantiations
// and 29,028 types for 30 eight-member pipelines, and 23,465
// instantiations and 5,505 types for 30 bulk calls. Each limit leaves
// approximately 23% headroom.
const workloads = [
	{ kind: 'pipeline', limits: { instantiations: 146_000, types: 36_300 } },
	{ kind: 'bulk', limits: { instantiations: 29_200, types: 6_900 } },
] as const;

const measure = (kind: 'pipeline' | 'bulk', quantity: number) => {
	const file = join(typesRoot, '__execution_budget.ts');
	const result = compile(
		[file],
		new Map([[file, executionBudgetFixture(kind, quantity)]]),
	);
	const errors = result.diagnostics.map((diagnostic) =>
		ts
			.flattenDiagnosticMessageText(diagnostic.messageText, '\n')
			.replaceAll(packageRoot, '<package>'),
	);
	return {
		instantiations: result.instantiations,
		types: result.types,
		errors,
	};
};

describe('execution type-check budgets', () => {
	let baseline: ReturnType<typeof measure>;

	beforeAll(() => {
		baseline = measure('pipeline', 0);
		expect(baseline.errors).toEqual([]);
	}, 300_000);

	it.each(workloads)('checks 30 $kind calls within the budget', ({
		kind,
		limits,
	}) => {
		const full = measure(kind, count);
		expect(full.errors).toEqual([]);
		const cost = {
			instantiations: full.instantiations - baseline.instantiations,
			types: full.types - baseline.types,
		};
		console.info(
			`[effect-surrealkit] execution type-check budget: ${count} ${kind} calls cost ${cost.instantiations} instantiations and ${cost.types} types`,
		);
		expect(cost.instantiations).toBeLessThanOrEqual(limits.instantiations);
		expect(cost.types).toBeLessThanOrEqual(limits.types);
	}, 300_000);
});
