/**
 * Every entry of the built-in function table (`src/functions/signatures.json`),
 * called through its generated function with the entry's sample arguments
 * and with each of its variants on whichever engine the run uses (see
 * `engine.ts`).
 *
 * On an engine major the entry lists, every call must succeed and its result
 * must decode with the codec the generated function derives. A numeric
 * literal argument (`3`, `2.5f`, `2.5dec`, or an array of them) is given the
 * codec of its own kind, so a result that depends on whether an argument
 * holds a decimal is checked for each kind. On a major the entry does not
 * list, the engine must refuse the sample call as an unknown function, so a
 * function one version lacks is recorded and verified rather than dropped
 * silently.
 */
import { Cause, Effect, Exit, Schema } from 'effect';
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
import { Decimal, Float, Int } from '../src/index.js';
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

/** The elements of a SurrealQL array literal's text, split at the top level. */
const elementsOf = (text: string): ReadonlyArray<string> => {
	const inner = text.slice(1, -1).trim();
	if (inner === '') return [];
	const parts: Array<string> = [];
	let depth = 0;
	let start = 0;
	for (let index = 0; index < inner.length; index++) {
		const char = inner[index];
		if (char === '[' || char === '{' || char === '(') depth++;
		else if (char === ']' || char === '}' || char === ')') depth--;
		else if (char === ',' && depth === 0) {
			parts.push(inner.slice(start, index).trim());
			start = index + 1;
		}
	}
	parts.push(inner.slice(start).trim());
	return parts;
};

/**
 * The codec of a numeric literal of its own kind (`3` an int, `2.5f` or
 * `0.5` a float, `2.5dec` a decimal), or of a non-empty array of them;
 * `undefined` for any other text.
 */
const literalCodec = (
	text: string,
): { readonly key: string; readonly codec: Codec } | undefined => {
	if (/^-?\d+$/u.test(text)) return { key: 'int', codec: Int };
	if (/^-?\d+(\.\d+)?f$/u.test(text) || /^-?\d+\.\d+$/u.test(text)) {
		return { key: 'float', codec: Float };
	}
	if (/^-?\d+(\.\d+)?dec$/u.test(text)) {
		return { key: 'decimal', codec: Decimal };
	}
	if (!text.startsWith('[') || !text.endsWith(']')) return undefined;
	const items = elementsOf(text).map(literalCodec);
	if (items.length === 0) return undefined;
	const known: Array<{ readonly key: string; readonly codec: Codec }> = [];
	for (const item of items) {
		if (item === undefined) return undefined;
		if (!known.some((other) => other.key === item.key)) known.push(item);
	}
	const [only] = known;
	const element =
		known.length === 1 && only !== undefined
			? only.codec
			: (Schema.Union(
					known.map((item) => item.codec),
				) as unknown as Codec);
	return {
		key: `array<${known.map((item) => item.key).join('|')}>`,
		codec: Schema.Array(element) as unknown as Codec,
	};
};

/** The generated function's expression over the given arguments. */
const build = (entry: Entry, sample = entry.sample): Expr => {
	const family = (functions as unknown as Record<string, unknown>)[
		familyOf(entry.name)
	] as Record<string, unknown>;
	const fn = family[exportName(entry.name)];
	if (entry.constant === true) return fn as Expr;
	const args = sample.map((sql, index) => {
		const arg = entry.args[Math.min(index, entry.args.length - 1)];
		if (arg === undefined) throw new Error(`${entry.name}: no argument`);
		return unsafeExpr({
			sql,
			codec: literalCodec(sql)?.codec ?? codecFor(arg.kind),
			atomic: true,
		});
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

	const call = (expr: Expr, name: string) =>
		within(
			Effect.runPromiseExit(
				client.run(
					make({
						name,
						sql: `RETURN ${sqlOf(expr)};`,
						params: paramsOf(expr),
						result: codecOf(expr),
					}),
					{},
				),
			),
			20_000,
		);

	/** Why the entry does not behave as recorded, or `undefined`. */
	const check = async (entry: Entry): Promise<ReadonlyArray<string>> => {
		const expr = build(entry);
		const expected =
			entry.constant === true
				? entry.name
				: `${entry.name}(${entry.sample.join(', ')})`;
		if (sqlOf(expr) !== expected) {
			return [`rendered \`${sqlOf(expr)}\`, not \`${expected}\``];
		}
		if (entry.setup !== undefined) await engine.db.query(entry.setup);
		const exit = await call(expr, entry.name);
		const listed = entry.engines.includes(major);
		if (!listed) {
			if (Exit.isSuccess(exit)) {
				return [
					`exists on SurrealDB ${major}, which the table does not list`,
				];
			}
			const message = messageOf(exit.cause);
			return unknownFunction.test(message)
				? []
				: [`not an unknown function on SurrealDB ${major}: ${message}`];
		}
		const failures = Exit.isSuccess(exit)
			? []
			: [`failed on SurrealDB ${major}: ${messageOf(exit.cause)}`];
		for (const variant of entry.variants ?? []) {
			const varied = await call(build(entry, variant), entry.name);
			if (Exit.isFailure(varied)) {
				failures.push(
					`(${variant.join(', ')}) failed on SurrealDB ${major}: ${messageOf(varied.cause)}`,
				);
			}
		}
		return failures;
	};

	it('runs on an engine major the table knows', () => {
		expect(majors).toContain(major);
	});

	it.each(families)('`%s::` behaves as the table records', async (family) => {
		const failures: Array<string> = [];
		for (const entry of table.functions) {
			if (familyOf(entry.name) !== family) continue;
			for (const failure of await check(entry)) {
				failures.push(`${entry.name}: ${failure}`);
			}
		}
		expect(failures).toEqual([]);
	});
});
