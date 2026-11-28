/**
 * The check every block the suites build passes: the package's own analyzer,
 * reading the block's text, must derive exactly what the builder declares.
 * A block is built without the analyzer (builder bundles leave it out), so
 * this is what keeps the two in agreement.
 */
import { expect } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';
import type { AnyStatement } from '../src/statement/index.js';

const sorted = (values: Iterable<string>): ReadonlyArray<string> =>
	[...values].sort();

/**
 * `statement`, after checking that its text reads exactly its bindings,
 * defines exactly its `defines` at the top level, returns `slots` results,
 * can THROW exactly its `throws`, writes no transaction of its own, has no
 * diagnostics, and, when the analyzer finds a write, is declared to write.
 */
export const consistent = <S extends AnyStatement>(statement: S): S => {
	const analysis = analyzeSync(statement.sql);
	expect(
		{
			kind: analysis.kind,
			free: analysis.free,
			defines: analysis.defines,
			slots: analysis.slots,
			throws: analysis.throws,
			transactional: analysis.transactional,
			holes: analysis.holes,
			diagnostics: analysis.diagnostics,
		},
		`what the text of \`${statement.name}\` does`,
	).toStrictEqual({
		kind: 'part',
		free: sorted(Object.keys(statement.params)),
		defines: sorted(statement.defines),
		slots: statement.slots,
		throws: sorted(statement.throws),
		transactional: statement.transactional,
		holes: [],
		diagnostics: [],
	});
	if (analysis.writes) {
		expect(statement.writes, `\`${statement.name}\` writes`).toBe(true);
	}
	return statement;
};
