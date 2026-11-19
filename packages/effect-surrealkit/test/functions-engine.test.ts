/**
 * Every entry of the built-in function table (`src/functions/signatures.json`),
 * called through its generated function with the entry's sample arguments
 * on whichever engine the run uses (see `engine.ts`).
 *
 * On an engine major the entry lists, the call must succeed and its result
 * must decode with the declared codec. On a major it does not list, the
 * engine must refuse the call as an unknown function, so a function one
 * version lacks is recorded and verified rather than dropped silently.
 */
import { Cause, Effect, Exit } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
	codecName,
	type Entry,
	exportName,
	familyOf,
	loadTable,
	majors,
	parseKind,
} from '../scripts/generate-functions.mjs';
import { type Client, make as makeClient } from '../src/client/index.js';
import * as codecs from '../src/functions/generated/codecs.js';
import * as functions from '../src/functions/index.js';
import {
	codecOf,
	type Expr,
	paramsOf,
	sqlOf,
	unsafeExpr,
} from '../src/statement/expr.js';
import { type Codec, make } from '../src/statement/statement.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] built-in function suite skipped: ${engineSkipReason}`,
	);
}

const table = loadTable();
const families = [...new Set(table.functions.map((e) => familyOf(e.name)))];

/** What SurrealDB 3.3 and 4.0 say about a function they do not have. */
const unknownFunction = /Invalid function\/constant path/u;

const codecFor = (kind: string): Codec => {
	const codec = (codecs as unknown as Record<string, Codec>)[
		codecName(parseKind(kind, 'sample'))
	];
	if (codec === undefined) throw new Error(`no codec for ${kind}`);
	return codec;
};

/** The generated function's expression over the entry's sample arguments. */
const build = (entry: Entry): Expr => {
	const family = (functions as unknown as Record<string, unknown>)[
		familyOf(entry.name)
	] as Record<string, unknown>;
	const fn = family[exportName(entry.name)];
	if (entry.constant === true) return fn as Expr;
	const args = entry.sample.map((sql, index) => {
		const arg = entry.args[Math.min(index, entry.args.length - 1)];
		if (arg === undefined) throw new Error(`${entry.name}: no argument`);
		return unsafeExpr({ sql, codec: codecFor(arg.kind), atomic: true });
	});
	return (fn as (...values: ReadonlyArray<Expr>) => Expr)(...args);
};

const messageOf = (cause: Cause.Cause<unknown>): string => {
	const error = Cause.squash(cause);
	return error instanceof Error ? error.message : String(error);
};

describe.skipIf(!engineConfigured)('built-in functions on the engine', () => {
	let engine: Engine;
	let client: Client;
	let major: string;

	beforeAll(async () => {
		engine = await startEngine();
		client = makeClient(engine.db);
		major = String(engine.majorVersion);
		console.info(
			`[effect-surrealkit] built-in functions: ${table.functions.length} entries, ${engine.mode} engine ${engine.version}`,
		);
	});
	afterAll(async () => {
		await engine?.stop();
	});

	/** Why the entry does not behave as recorded, or `undefined`. */
	const check = async (entry: Entry): Promise<string | undefined> => {
		const expr = build(entry);
		const expected =
			entry.constant === true
				? entry.name
				: `${entry.name}(${entry.sample.join(', ')})`;
		if (sqlOf(expr) !== expected) {
			return `rendered \`${sqlOf(expr)}\`, not \`${expected}\``;
		}
		if (entry.setup !== undefined) await engine.db.query(entry.setup);
		const statement = make({
			name: entry.name,
			sql: `RETURN ${sqlOf(expr)};`,
			params: paramsOf(expr),
			result: codecOf(expr),
		});
		const exit = await within(
			Effect.runPromiseExit(client.run(statement, {})),
			20_000,
		);
		const listed = entry.engines.includes(major);
		if (listed) {
			return Exit.isSuccess(exit)
				? undefined
				: `failed on SurrealDB ${major}: ${messageOf(exit.cause)}`;
		}
		if (Exit.isSuccess(exit)) {
			return `exists on SurrealDB ${major}, which the table does not list`;
		}
		const message = messageOf(exit.cause);
		return unknownFunction.test(message)
			? undefined
			: `not an unknown function on SurrealDB ${major}: ${message}`;
	};

	it('runs on an engine major the table knows', () => {
		expect(majors).toContain(major);
	});

	it.each(families)('`%s::` behaves as the table records', async (family) => {
		const failures: Array<string> = [];
		for (const entry of table.functions) {
			if (familyOf(entry.name) !== family) continue;
			const failure = await check(entry);
			if (failure !== undefined)
				failures.push(`${entry.name}: ${failure}`);
		}
		expect(failures).toEqual([]);
	});
});
