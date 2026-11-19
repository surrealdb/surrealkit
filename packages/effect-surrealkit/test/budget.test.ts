/**
 * The type-check budget: what 300 statements cost the compiler, read from
 * its deterministic counters (types created and instantiations, as
 * `--extendedDiagnostics` reports them; never time). The fixture is written
 * by `budgetFixture` into memory beside the type fixtures and compiled with
 * their options; the cost is its counters minus those of the same imports
 * with no statement. A change that makes statements markedly more expensive
 * to check fails here: raise a limit only for a reason you can name.
 */
import { join } from 'node:path';

import ts from 'typescript';
import { describe, expect, it } from 'vitest';

import { blockBudgetFixture } from './block-budget-fixture.js';
import { budgetFixture } from './budget-fixture.js';
import { compile, packageRoot, typesRoot } from './compiler.js';

const statements = 300;

/**
 * Measured with TypeScript 5.9.3 and effect 4.0.6: 300 statements cost
 * 211,180 instantiations (704 each) and 46,921 types (156 each). The limits
 * leave about 30% headroom.
 */
const limits = { instantiations: 275_000, types: 61_000 };

/**
 * Measured the same way: 30 blocks shaped like the `pay` script cost
 * 184,138 instantiations (6,138 each) and 26,090 types (870 each), about
 * what nine statements cost. The limits leave about 30% headroom.
 */
const blocks = 30;
const blockLimits = { instantiations: 240_000, types: 34_000 };

const measure = (count: number, fixture = budgetFixture, name = 'budget') => {
	const file = join(typesRoot, `__${name}_${count}.ts`);
	const compiled = compile([file], new Map([[file, fixture(count)]]));
	const errors = compiled.diagnostics.map((diagnostic) =>
		ts
			.flattenDiagnosticMessageText(diagnostic.messageText, '\n')
			.split(packageRoot)
			.join('<package>'),
	);
	return { ...compiled, errors };
};

describe('type-check budget', () => {
	it(`checks ${statements} statements within the budget`, () => {
		const base = measure(0);
		const full = measure(statements);
		expect(base.errors).toEqual([]);
		expect(full.errors).toEqual([]);
		const cost = {
			instantiations: full.instantiations - base.instantiations,
			types: full.types - base.types,
		};
		console.info(
			`[effect-surrealkit] type-check budget: ${statements} statements cost ${cost.instantiations} instantiations (${Math.round(cost.instantiations / statements)} each) and ${cost.types} types (${Math.round(cost.types / statements)} each)`,
		);
		expect(cost.instantiations).toBeLessThanOrEqual(limits.instantiations);
		expect(cost.types).toBeLessThanOrEqual(limits.types);
	}, 300_000);

	it(`checks ${blocks} blocks within their budget`, () => {
		const base = measure(0, blockBudgetFixture, 'block_budget');
		const full = measure(blocks, blockBudgetFixture, 'block_budget');
		expect(base.errors).toEqual([]);
		expect(full.errors).toEqual([]);
		const cost = {
			instantiations: full.instantiations - base.instantiations,
			types: full.types - base.types,
		};
		console.info(
			`[effect-surrealkit] type-check budget: ${blocks} blocks cost ${cost.instantiations} instantiations (${Math.round(cost.instantiations / blocks)} each) and ${cost.types} types (${Math.round(cost.types / blocks)} each)`,
		);
		expect(cost.instantiations).toBeLessThanOrEqual(
			blockLimits.instantiations,
		);
		expect(cost.types).toBeLessThanOrEqual(blockLimits.types);
	}, 300_000);
});
