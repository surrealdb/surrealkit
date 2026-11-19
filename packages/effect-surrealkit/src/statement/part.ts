/**
 * Authored SurrealQL as statements: a `part` is one text with what it reads,
 * defines, returns and throws, checked against the text by the package's own
 * analyzer; a `sequence` runs parts in one scope as one statement.
 *
 * This is the only module of `effect-surrealkit/statement` that imports the
 * analyzer, so code that only uses the builders does not carry it.
 */
import { type Analysis, analyzeSync } from '../analyze/analyze.js';
import {
	assertIdentifier,
	type Codec,
	make,
	type Params,
	type Statement,
	type ThrowSite,
} from './statement.js';

/**
 * One authored SurrealQL text and what it does with variables:
 *
 * - `free`: the `$names` it reads and does not define itself;
 * - `defines`: the names it `LET`s at the top level, which later parts of a
 *   sequence can read;
 * - `slots`: how many results it returns (one per top-level statement);
 * - `throws`: the messages it can `THROW`;
 * - optionally `name` and `source` (where the text came from) and `throwsAt`
 *   (the lines each message is thrown on), for messages and tooling;
 * - `writes`: `true` when the text certainly changes data or schema,
 *   `false` when it certainly does not, absent when that is unknown (text
 *   can call a function that writes). {@link part} sets `true` when the
 *   analyzer finds a write; only a declaration makes it `false`, and a
 *   declared value is checked against the text.
 *
 * `effect-surrealkit statements` derives these from `.surql` files
 * (`effect-surrealkit/analyze`). Written by hand they are checked against
 * the text by {@link part}, and again by {@link sequence} for a part that
 * was not declared with `part`.
 */
export interface Part<
	Free extends string = string,
	Defines extends string = string,
	Throws extends string = string,
> {
	readonly name?: string;
	readonly source?: string;
	readonly sql: string;
	readonly free: ReadonlyArray<Free>;
	readonly defines: ReadonlyArray<Defines>;
	readonly slots: number;
	readonly throws: ReadonlyArray<Throws>;
	readonly throwsAt?: Readonly<Record<string, ReadonlyArray<number>>>;
	readonly writes?: boolean;
}

/** What each checked part's text does. */
const checked = new WeakMap<Part, Analysis>();

const names = (values: ReadonlyArray<string>): string =>
	values.map((name) => `$${name}`).join(', ');

const messages = (values: ReadonlyArray<string>): string =>
	values.map((message) => `'${message}'`).join(', ');

const missingFrom = (
	values: ReadonlyArray<string>,
	other: ReadonlyArray<string>,
): ReadonlyArray<string> =>
	[...new Set(values)].filter((value) => !other.includes(value)).sort();

const isWordChar = (c: string | undefined): boolean =>
	c !== undefined &&
	((c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		(c >= '0' && c <= '9') ||
		c === '_');

/** Whether `$name` occurs in `sql` as a whole name: a scan, not a pattern. */
const reads = (sql: string, name: string): boolean => {
	const needle = `$${name}`;
	for (
		let at = sql.indexOf(needle);
		at !== -1;
		at = sql.indexOf(needle, at + 1)
	) {
		if (!isWordChar(sql[at + needle.length])) return true;
	}
	return false;
};

/**
 * Checks a part against its own text and returns what the text does, or
 * throws a `RangeError` naming every difference (an `AnalyzeError` when the
 * text cannot be analysed).
 */
const verify = (item: Part, label: string): Analysis => {
	const known = checked.get(item);
	if (known !== undefined) return known;
	const text = item.sql.trim();
	if (!text.endsWith(';')) {
		throw new RangeError(
			`${label} does not end with \`;\`, so the next part would run into it`,
		);
	}
	if (!Number.isInteger(item.slots) || item.slots < 0) {
		throw new RangeError(`${label} returns ${item.slots} results`);
	}
	for (const name of [...item.free, ...item.defines]) {
		// A part may define, and a later part read, a name a caller could
		// not bind (`LET $event = …`): only what is left unbound is checked
		// as a binding, when the statement is made.
		assertIdentifier(name);
		if (!reads(text, name)) {
			throw new RangeError(
				`${label} lists \`$${name}\`, which its text never mentions`,
			);
		}
	}
	const analysis = analyzeSync(item.sql);
	if (analysis.holes.length > 0) {
		throw new RangeError(
			`${label} has holes (${analysis.holes.join(', ')}): a template is not a part; fill its holes and declare the filled text`,
		);
	}
	const differences: Array<string> = [];
	const undeclaredFree = missingFrom(analysis.free, item.free);
	if (undeclaredFree.length > 0) {
		differences.push(
			`free: the text reads ${names(undeclaredFree)}, which is not declared`,
		);
	}
	const unreadFree = missingFrom(item.free, analysis.free);
	if (unreadFree.length > 0) {
		differences.push(
			`free: declared ${names(unreadFree)}, which the text does not read before defining`,
		);
	}
	const undeclaredDefines = missingFrom(analysis.defines, item.defines);
	if (undeclaredDefines.length > 0) {
		differences.push(
			`defines: the text LETs ${names(undeclaredDefines)} at the top level, which is not declared`,
		);
	}
	const unletDefines = missingFrom(item.defines, analysis.defines);
	if (unletDefines.length > 0) {
		differences.push(
			`defines: declared ${names(unletDefines)}, which the text does not LET at the top level`,
		);
	}
	if (item.slots !== analysis.slots) {
		differences.push(
			`slots: declared ${item.slots}, the text returns ${analysis.slots}`,
		);
	}
	const undeclaredThrows = missingFrom(analysis.throws, item.throws);
	if (undeclaredThrows.length > 0) {
		differences.push(
			`throws: the text can THROW ${messages(undeclaredThrows)}, which is not declared`,
		);
	}
	const unthrown = missingFrom(item.throws, analysis.throws);
	if (unthrown.length > 0) {
		differences.push(
			`throws: declared ${messages(unthrown)}, which the text never THROWs`,
		);
	}
	if (item.writes !== undefined && item.writes !== analysis.writes) {
		differences.push(
			analysis.writes
				? 'writes: declared false, the text writes'
				: 'writes: declared true, the text does not write',
		);
	}
	if (differences.length > 0) {
		throw new RangeError(
			`${label} does not declare what its text does: ${differences.join('; ')}`,
		);
	}
	checked.set(item, analysis);
	return analysis;
};

const nameOf = (name: string | undefined, fallback: string): string =>
	name === undefined ? fallback : `part \`${name}\``;

/**
 * Declares a {@link Part}, keeping its names as literal types, and checks it
 * against its text: a `free`, `defines`, `slots` or `throws` that differs
 * from what the analyzer derives from `sql` is refused, naming the
 * difference, and so is a text with holes (a template).
 *
 * @throws RangeError for a part its text contradicts, or `AnalyzeError`
 *   (from `effect-surrealkit/analyze`) for a text it cannot analyse.
 */
export const part = <
	const Free extends string = never,
	const Defines extends string = never,
	const Throws extends string = never,
>(options: {
	readonly name?: string;
	readonly source?: string;
	readonly sql: string;
	readonly free?: ReadonlyArray<Free>;
	readonly defines?: ReadonlyArray<Defines>;
	readonly slots?: number;
	readonly throws?: ReadonlyArray<Throws>;
	readonly throwsAt?: Readonly<Record<string, ReadonlyArray<number>>>;
	/**
	 * `false` declares that the text certainly does not write, which the
	 * analyzer cannot prove (a function it calls may write); refused when
	 * the analyzer finds a write. `true` is refused when it finds none.
	 * Left out, it is `true` when the analyzer finds a write and unknown
	 * otherwise.
	 */
	readonly writes?: boolean;
}): Part<Free, Defines, Throws> => {
	const declared: Part<Free, Defines, Throws> = {
		...(options.name !== undefined && { name: options.name }),
		...(options.source !== undefined && { source: options.source }),
		sql: options.sql,
		free: options.free ?? [],
		defines: options.defines ?? [],
		slots: options.slots ?? 1,
		throws: options.throws ?? [],
		...(options.throwsAt !== undefined && { throwsAt: options.throwsAt }),
		...(options.writes !== undefined && { writes: options.writes }),
	};
	const analysis = verify(declared, nameOf(options.name, 'part'));
	const result: Part<Free, Defines, Throws> = analysis.writes
		? { ...declared, writes: true }
		: declared;
	checked.set(result, analysis);
	return result;
};

/** The source and lines of the first part declaring each message with both. */
const throwSitesOf = (
	parts: ReadonlyArray<Part>,
): Readonly<Record<string, ThrowSite>> | undefined => {
	const sites: Record<string, ThrowSite> = {};
	for (const item of parts) {
		if (item.source === undefined || item.throwsAt === undefined) continue;
		for (const message of item.throws) {
			const lines = item.throwsAt[message];
			if (lines === undefined || Object.hasOwn(sites, message)) continue;
			sites[message] = { source: item.source, lines };
		}
	}
	return Object.keys(sites).length > 0 ? sites : undefined;
};

/**
 * The bindings a sequence still needs from its caller: each part's free
 * names, minus what earlier parts define.
 */
export type Unbound<
	Parts extends ReadonlyArray<Part>,
	Defined extends string = never,
> = Parts extends readonly [
	infer Head extends Part,
	...infer Tail extends ReadonlyArray<Part>,
]
	?
			| Exclude<Head['free'][number], Defined>
			| Unbound<Tail, Defined | Head['defines'][number]>
	: never;

/** The messages any part of a sequence can `THROW`. */
export type ThrownBy<Parts extends ReadonlyArray<Part>> =
	Parts[number]['throws'][number];

/**
 * `P`, the codecs a contract declares, when they name exactly `Names`.
 * Otherwise a binding nothing reads is typed as a message naming it, and
 * when none is extra, the contract's expected type is a message naming each
 * binding it leaves without a codec.
 */
export type Exactly<Names extends string, P> = [
	Exclude<keyof P, Names>,
] extends [never]
	? [Exclude<Names, keyof P>] extends [never]
		? P
		: `binding \`$${Exclude<Names, keyof P>}\` is read by this sequence and has no codec`
	: {
			readonly [K in keyof P]: K extends Names
				? P[K]
				: `binding \`$${K & string}\` is not read by this sequence`;
		};

/**
 * The contract of a sequence: `P`, checked by {@link Exactly} against the
 * names the parts leave unbound. Parts that are not typed as literals (a
 * widened list, names typed `string`) cannot be checked by the compiler, so
 * they are left to the check `define` makes when it runs.
 */
export type Contract<
	Parts extends ReadonlyArray<Part>,
	P,
> = number extends Parts['length']
	? P
	: string extends Unbound<Parts>
		? P
		: Exactly<Unbound<Parts>, P>;

/**
 * What a sequence writes: `true` when any part certainly writes, `false`
 * when every part certainly does not, otherwise unknown.
 */
const writesOf = (
	parts: ReadonlyArray<Part>,
	analyses: ReadonlyArray<Analysis>,
): boolean | undefined =>
	// `verify` has refused a declared `writes` the text contradicts.
	analyses.some((analysis) => analysis.writes)
		? true
		: parts.every((item) => item.writes === false)
			? false
			: undefined;

const unboundOf = (parts: ReadonlyArray<Part>): ReadonlyArray<string> => {
	const defined = new Set<string>();
	const unbound = new Set<string>();
	for (const item of parts) {
		for (const name of item.free) {
			if (!defined.has(name)) unbound.add(name);
		}
		for (const name of item.defines) defined.add(name);
	}
	return [...unbound];
};

/**
 * Authored SurrealQL texts run one after another in a single request, sharing
 * one variable scope, as a statement.
 *
 * `define` takes a codec for exactly the names the sequence still needs:
 * leaving one out, or declaring one no part reads, is a compile error, and
 * the same check runs again when the statement is defined, in case the parts
 * were not typed as literals. A part that was not declared with {@link part}
 * is checked against its text here.
 *
 * @example
 * const guarded = sequence(bindWrite, requireOwner, readCutover).define({
 *   params: { boundProducer: Producer },
 *   result: Cutover,
 * });
 */
export const sequence = <const Parts extends ReadonlyArray<Part>>(
	...parts: Parts
) => ({
	define: <const P extends Params, R extends Codec>(contract: {
		readonly name?: string;
		/**
		 * What the sequence writes, when its parts leave it unknown: `false`
		 * declares that it certainly does not write, `true` that it does.
		 * Refused when it contradicts what the parts say. Default derived
		 * from the parts.
		 */
		readonly writes?: boolean;
		readonly params: Contract<Parts, P>;
		readonly result: R;
	}): Statement<P, R, ThrownBy<Parts>> => {
		const analyses = parts.map((item, index) =>
			verify(
				item,
				nameOf(item.name, `part ${index + 1} of the sequence`),
			),
		);
		const label = contract.name ?? 'sequence';
		const throwSites = throwSitesOf(parts);
		const expected = unboundOf(parts);
		const declared = Object.keys(contract.params);
		const missing = expected.filter((name) => !declared.includes(name));
		const extra = declared.filter((name) => !expected.includes(name));
		if (missing.length > 0 || extra.length > 0) {
			throw new RangeError(
				[
					`the bindings of sequence \`${label}\` do not match the names it reads`,
					...(missing.length > 0
						? [`missing: ${names(missing)}`]
						: []),
					...(extra.length > 0
						? [`not read by any part: ${names(extra)}`]
						: []),
				].join('; '),
			);
		}
		const derived = writesOf(parts, analyses);
		if (
			contract.writes !== undefined &&
			derived !== undefined &&
			contract.writes !== derived
		) {
			throw new RangeError(
				contract.writes
					? `sequence \`${label}\` is declared to write, but every part is declared not to`
					: `sequence \`${label}\` is declared not to write, but it writes`,
			);
		}
		const writes = contract.writes ?? derived;
		return make({
			name: label,
			sql: parts.map((item) => item.sql.trim()).join('\n'),
			params: contract.params as P,
			result: contract.result,
			slots: parts.reduce((total, item) => total + item.slots, 0),
			throws: [
				...new Set(parts.flatMap((item) => item.throws)),
			] as ReadonlyArray<ThrownBy<Parts>>,
			defines: [...new Set(parts.flatMap((item) => item.defines))].sort(),
			transactional: analyses.some((analysis) => analysis.transactional),
			...(writes !== undefined && { writes }),
			sources: [
				...new Set(
					parts.flatMap((item) =>
						item.source === undefined ? [] : [item.source],
					),
				),
			],
			...(throwSites !== undefined && { throwSites }),
		});
	},
});
