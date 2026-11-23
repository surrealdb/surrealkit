/**
 * Whether the engine plans a `SELECT` through an index, read from its own
 * plan: the statement runs with `EXPLAIN` appended, through the client in
 * context, and nothing it would read is returned.
 *
 * SurrealDB 3.3 and 4.0 answer `SELECT … EXPLAIN` with the same plan
 * format: one root of a tree of `{ operator, attributes, children }` nodes
 * (`SelectProject`, `ProjectValue`, `UnwrapExactlyOne` for `ONLY`), where a node
 * that reads an index (`IndexScan` for `=` and ranges, `ORDER BY` and
 * `LIMIT` pushed into it) names it as `attributes.index`, and a table scan
 * names none. The prefix form `EXPLAIN SELECT …` returns text instead, so
 * it is not used. The plan of each builder shape (projection, `VALUE`,
 * `ONLY`, a condition with an indexed and an unindexed side) is pinned in
 * `test/testing.test.ts` on both engines.
 */
import { Effect, Schema } from 'effect';

import { run, type SurrealClient } from '../client/client.js';
import type { StatementError } from '../client/errors.js';
import {
	type Codec,
	type Input,
	make,
	type Params,
	type Statement,
} from '../statement/statement.js';
import { IndexExpectationError } from './errors.js';

/**
 * The names of the indexes a plan reads: every `attributes.index` in the
 * tree, and `detail.plan.index` of an older planner's
 * `{ operation, detail }` rows, in the order the plan lists them.
 */
export const indexesOf = (plan: unknown): ReadonlyArray<string> => {
	const found: Array<string> = [];
	const visit = (node: unknown): void => {
		if (Array.isArray(node)) {
			for (const item of node) visit(item);
			return;
		}
		if (typeof node !== 'object' || node === null) return;
		const { attributes, children, detail } = node as {
			readonly attributes?: { readonly index?: unknown };
			readonly children?: unknown;
			readonly detail?: { readonly plan?: { readonly index?: unknown } };
		};
		for (const index of [attributes?.index, detail?.plan?.index]) {
			if (typeof index === 'string' && !found.includes(index)) {
				found.push(index);
			}
		}
		visit(children);
	};
	visit(plan);
	return found;
};

const explainable = (
	statement: Statement<Params, Codec, string, string>,
): string => {
	const text = statement.sql.trim();
	if (statement.slots !== 1 || !/^SELECT\s/iu.test(text)) {
		throw new TypeError(
			`\`${statement.name}\` is not a single SELECT, so the engine has no plan of it to read`,
		);
	}
	return `${text.slice(0, -1).trimEnd()} EXPLAIN;`;
};

/** The plan of `statement` for `input`, and the indexes it reads. */
export const explain = <
	P extends Params,
	R extends Codec,
	T extends string,
	U extends string = never,
>(
	statement: Statement<P, R, T, U>,
	input: Input<P>,
): Effect.Effect<
	{ readonly plan: unknown; readonly indexes: ReadonlyArray<string> },
	StatementError<T, U>,
	SurrealClient
> =>
	Effect.suspend(() => {
		const plan = make({
			name: `explain ${statement.name}`,
			sql: explainable(statement),
			params: statement.params,
			result: Schema.Unknown,
			// EXPLAIN plans the statement without running it.
			writes: false,
		});
		return run(plan, input as never).pipe(
			Effect.map((tree) => ({ plan: tree, indexes: indexesOf(tree) })),
		) as Effect.Effect<
			{ readonly plan: unknown; readonly indexes: ReadonlyArray<string> },
			StatementError<T, U>,
			SurrealClient
		>;
	});

const expecting =
	(expected: 'used' | 'unused') =>
	<
		P extends Params,
		R extends Codec,
		T extends string,
		U extends string = never,
	>(
		statement: Statement<P, R, T, U>,
		input: Input<P>,
		index: string,
	): Effect.Effect<
		void,
		IndexExpectationError | StatementError<T, U>,
		SurrealClient
	> =>
		Effect.flatMap(explain(statement, input), ({ indexes, plan }) =>
			indexes.includes(index) === (expected === 'used')
				? Effect.void
				: Effect.fail(
						new IndexExpectationError({
							statement: statement.name,
							index,
							expected,
							used: indexes,
							plan,
							message:
								expected === 'used'
									? `\`${statement.name}\` does not read the index \`${index}\`; its plan reads ${indexes.length === 0 ? 'no index' : indexes.map((name) => `\`${name}\``).join(', ')}`
									: `\`${statement.name}\` reads the index \`${index}\``,
						}),
					),
		);

/**
 * Succeeds when the engine plans `statement` with `input` through the index
 * `index`; fails with an {@link IndexExpectationError} naming the indexes
 * it does read. The statement must be one `SELECT`.
 */
export const expectIndex = expecting('used');

/** Succeeds when the engine's plan of `statement` does not read `index`. */
export const expectNoIndex = expecting('unused');
