/**
 * A statement: one request to SurrealDB, described as a value.
 *
 * The SurrealQL text is static. Every value enters through a named binding
 * that carries its own codec, so the text never depends on input, and the
 * final result is decoded by a codec the statement names. Nothing here talks
 * to a database: `effect-surrealkit/client` runs statements.
 */
import type { Schema } from 'effect';

import type { BindingUses } from '../analyze/uses.js';
import { assertResolved, withoutRefs } from './refs.js';

/** A codec that needs no services, so a statement needs only a client. */
export type Codec = Schema.Codec<unknown, unknown, never, never>;

/** The codec of each named binding of a statement or expression. */
export type Params = { readonly [name: string]: Codec };

/** No bindings. */
export type NoParams = Record<never, never>;

/** Flattens an intersection so hovers and errors show one object. */
export type Simplify<A> = { readonly [K in keyof A]: A[K] } & {};

type OptionalNames<P extends Params> = {
	[K in keyof P]: undefined extends P[K]['Type'] ? K : never;
}[keyof P];

/**
 * The values a caller supplies: one per binding, as the codec's decoded type,
 * in the order the statement reads them. A binding whose codec accepts
 * `undefined` (NONE) may be omitted.
 */
export type Input<P extends Params> = Simplify<
	{
		readonly [K in keyof P as undefined extends P[K]['Type']
			? never
			: K]: P[K]['Type'];
	} & {
		readonly [K in keyof P as undefined extends P[K]['Type']
			? K
			: never]?: P[K]['Type'];
	}
>;

type RequiredNames<P extends Params> = Exclude<keyof P, OptionalNames<P>>;

/**
 * The expected type of the input of a statement reading `P`, given the
 * input as the caller wrote it, `I`: `Input<P>`, unless `I` leaves a
 * binding without a value and names nothing else, when it is a message
 * naming each such binding. The compiler reports an unknown or misspelt
 * name and a wrong value against `Input<P>` as it always does.
 *
 * The first branch is never taken (no statement reads `never`); it is where
 * the compiler infers `I` from the argument. It tests `P`, not `I`, because
 * the compiler narrows `I` to `never` inside a branch that tests `I`
 * against `never`. The second tests `I` against `Input<P>` itself, so
 * generic code that forwards an `Input<P>` passes.
 */
export type CheckedInput<P extends Params, I> = [P] extends [never]
	? I
	: [I] extends [Input<P>]
		? Input<P>
		: [Exclude<keyof I, keyof P>] extends [never]
			? [Exclude<RequiredNames<P>, keyof I>] extends [never]
				? Input<P>
				: `binding \`${Exclude<RequiredNames<P>, keyof I> & string}\` has no value`
			: Input<P>;

export const StatementTypeId = '~effect-surrealkit/Statement';

/**
 * - `sql`: the SurrealQL text, one or more statements.
 * - `params`: the codec of every binding the text reads. Values are encoded
 *   through them before anything is sent.
 * - `result`: decodes the result at `valueSlot`.
 * - `slots`: how many results the text returns, one per top-level statement.
 *   Every one but the one at `valueSlot` must be NONE (`LET`, guards,
 *   `BEGIN`, `COMMIT`).
 * - `valueSlot`: the 1-based position of the result `result` decodes,
 *   `slots` unless the text ends with transaction control (`sequence`
 *   derives it; `make` takes it as declared).
 * - `throws`: the `THROW` messages the text can raise, which a client
 *   surfaces as a typed error rather than a request failure.
 * - `uniqueIndexes`: the unique indexes a write of the text can duplicate a
 *   value of, by name (absent when none), which a client surfaces as a typed
 *   `StatementIndexConflict` rather than a request failure. Write builders
 *   take them from the table's `indexes`; `make` and `sequence` take them as
 *   declared.
 * - `defines`: the names the text `LET`s at the top level. Statements in one
 *   transaction share one variable scope, so a client refuses to bind a name
 *   another statement of the same transaction defines.
 * - `transactional`: the text writes its own `BEGIN`, `COMMIT` or `CANCEL`,
 *   so it cannot run inside a client's `transaction`.
 * - `writes`: whether the text changes data or schema, in three states.
 *   `true`: it certainly does, so a client that cannot decode its result
 *   says its writes are committed. `false`: it certainly does not, so a
 *   result that does not decode is a plain `StatementDecodeError`. Absent:
 *   unknown (authored text can call a function that writes), so a client
 *   says any writes it made are committed. Write builders set `true`, a
 *   `select` of stored fields `false`; `sequence` derives it from its
 *   parts; `make` takes it as declared.
 * - `nonEmpty`: the last result is a row, or a non-empty list, whenever
 *   the statement applies (`create` and `relate` returning rows), so a
 *   client reports NONE or an empty list as an empty result, which
 *   permissions cause.
 * - `sources`: where the text came from (the `source` of each part), for
 *   messages and tracing.
 * - `variant`: for a variant of a statement family, the key that selected
 *   it (`axis=value, …`). The statement's name is the family's, so its
 *   metrics are the family's; a run records the key on its span.
 * - `throwSites`: for a `THROW` message, the source and lines of the
 *   first part that declares it with both.
 * - `tables`, `functions` and `openTargets`: the static tables the text
 *   reads and writes, the custom functions it calls, and whether some
 *   target is not a static name, so the lists may be incomplete.
 *   `sequence` sets them, the union over its parts; builders leave them
 *   out; `make` takes them as declared.
 */
export interface Statement<
	P extends Params = Params,
	R extends Codec = Codec,
	Throws extends string = never,
	Unique extends string = never,
> {
	readonly [StatementTypeId]: typeof StatementTypeId;
	readonly name: string;
	readonly sql: string;
	readonly params: P;
	readonly result: R;
	readonly slots: number;
	readonly valueSlot: number;
	readonly throws: ReadonlyArray<Throws>;
	readonly uniqueIndexes?: ReadonlyArray<Unique>;
	readonly defines: ReadonlyArray<string>;
	readonly transactional: boolean;
	readonly writes?: boolean;
	readonly nonEmpty?: boolean;
	readonly sources?: ReadonlyArray<string>;
	readonly variant?: string;
	readonly throwSites?: Readonly<Record<string, ThrowSite>>;
	readonly tables?: {
		readonly read: ReadonlyArray<string>;
		readonly written: ReadonlyArray<string>;
	};
	readonly functions?: ReadonlyArray<string>;
	readonly openTargets?: boolean;
	/**
	 * What each binding is used as, where the analyzer could tell: an
	 * argument of a custom function, the key of a `type::record`, or a
	 * field of the one table a query targets. `sequence` sets it, merged
	 * over its parts for the names that remain bindings, when there is any;
	 * `make` takes it as declared.
	 */
	readonly uses?: BindingUses;
}

/** Where a declared `THROW` message comes from. */
export interface ThrowSite {
	readonly source: string;
	readonly lines: ReadonlyArray<number>;
}

/** Any statement, whatever it binds, returns, throws and can duplicate. */
export type AnyStatement = Statement<Params, Codec, string, string>;

/** The decoded result of a statement. */
export type Result<S> =
	S extends Statement<Params, infer R, string, string> ? R['Type'] : never;

/** The messages a statement declares it can `THROW`. */
export type Thrown<S> =
	S extends Statement<Params, Codec, infer T, string> ? T : never;

/** The unique indexes a statement declares it can duplicate a value of. */
export type UniqueIndexes<S> =
	S extends Statement<Params, Codec, string, infer U> ? U : never;

export const isStatement = (value: unknown): value is AnyStatement =>
	typeof value === 'object' && value !== null && StatementTypeId in value;

const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/u;

/**
 * Parameters only the engine sets. SurrealDB 4 refuses a request that binds
 * one ("'auth' is a protected variable and cannot be set").
 */
const sessionNames: ReadonlySet<string> = new Set([
	'auth',
	'access',
	'session',
	'token',
]);

/**
 * Parameters the engine sets inside clauses, subqueries, fields and events.
 * A request may bind one at the top level, but a statement that reads it in
 * one of those places would silently read the engine's value instead, so a
 * binding may not be called this.
 */
const scopedNames: ReadonlySet<string> = new Set([
	'this',
	'self',
	'parent',
	'value',
	'input',
	'before',
	'after',
	'event',
]);

/**
 * Rejects a name SurrealQL cannot read back as a variable, or one only the
 * engine sets.
 *
 * @internal
 */
export const assertIdentifier = (name: string): void => {
	if (!identifier.test(name)) {
		throw new RangeError(
			`\`$${name}\` is not a parameter name: use letters, digits and underscores, not starting with a digit`,
		);
	}
	if (sessionNames.has(name)) {
		throw new RangeError(
			`\`$${name}\` is set by SurrealDB and cannot be bound or defined by a statement`,
		);
	}
};

/** Rejects a binding name SurrealQL would not read back as bound. */
export const assertBindingName = (name: string): void => {
	assertIdentifier(name);
	if (scopedNames.has(name)) {
		throw new RangeError(
			`\`$${name}\` is set by SurrealDB inside clauses, subqueries and field definitions, so a binding of that name can be shadowed; rename the binding`,
		);
	}
};

/**
 * The text with exactly one terminator: trailing whitespace and `;` are
 * dropped and one `;` is added, so statements joined one after another (a
 * transaction) never run into each other.
 */
const terminated = (sql: string): string => {
	let text = sql.trimEnd();
	while (text.endsWith(';')) text = text.slice(0, -1).trimEnd();
	return text.length === 0 ? '' : `${text};`;
};

const assertStatement = (statement: AnyStatement): void => {
	if (statement.sql.trim().length === 0) {
		throw new RangeError(`statement \`${statement.name}\` has no text`);
	}
	if (!Number.isInteger(statement.slots) || statement.slots < 1) {
		throw new RangeError(
			`statement \`${statement.name}\` returns ${statement.slots} results; a statement returns at least one`,
		);
	}
	if (
		!Number.isInteger(statement.valueSlot) ||
		statement.valueSlot < 1 ||
		statement.valueSlot > statement.slots
	) {
		throw new RangeError(
			`statement \`${statement.name}\` decodes result ${statement.valueSlot}, outside its ${statement.slots} results`,
		);
	}
	for (const name of Object.keys(statement.params)) assertBindingName(name);
	for (const name of statement.defines) assertIdentifier(name);
};

/**
 * A unique index is declared by a name, and only by a statement that may
 * write: one that certainly does not write cannot duplicate a value.
 */
const assertUniqueIndexes = (statement: AnyStatement): void => {
	const declared = statement.uniqueIndexes ?? [];
	if (declared.some((index) => index.length === 0)) {
		throw new RangeError(
			`statement \`${statement.name}\` declares a unique index with no name`,
		);
	}
	if (declared.length > 0 && statement.writes === false) {
		throw new RangeError(
			`statement \`${statement.name}\` is declared not to write, so it cannot duplicate a value of a unique index; remove \`uniqueIndexes\` or declare \`writes\``,
		);
	}
};

const wordChar = /[A-Za-z0-9_]/u;

/** The index just past the comment at `at`, or `at` when there is none. */
const skipComment = (sql: string, at: number): number => {
	const two = sql.slice(at, at + 2);
	if (two === '/*') {
		const end = sql.indexOf('*/', at + 2);
		return end === -1 ? sql.length : end + 2;
	}
	if (two === '--' || two === '//' || sql[at] === '#') {
		const end = sql.indexOf('\n', at);
		return end === -1 ? sql.length : end + 1;
	}
	return at;
};

/** The index just past the whitespace and comments at `at`. */
const skipBlank = (sql: string, at: number): number => {
	let index = at;
	for (;;) {
		while (index < sql.length && /\s/u.test(sql[index] ?? '')) index += 1;
		const next = skipComment(sql, index);
		if (next === index) return index;
		index = next;
	}
};

const closers: Readonly<Record<string, string>> = {
	"'": "'",
	'"': '"',
	'`': '`',
	'⟨': '⟩',
};

/**
 * The first word of each top-level statement of `sql` (upper case), and the
 * names it appears to `LET` there: a lexical scan that skips strings,
 * quoted identifiers, comments and anything inside brackets or blocks. It
 * is no parser (the analyzer is, and stays out of builder-only bundles); it
 * lets {@link make} notice an undeclared `LET`, and the scripted client of
 * `effect-surrealkit/testing` refuse a transaction as the SDK does.
 *
 * @internal
 */
export const scanTopLevel = (
	sql: string,
): {
	readonly keywords: ReadonlyArray<string>;
	readonly lets: ReadonlyArray<string>;
} => {
	const names: Array<string> = [];
	const keywords: Array<string> = [];
	let depth = 0;
	let atStart = true;
	let index = 0;
	while (index < sql.length) {
		const blank = skipBlank(sql, index);
		if (blank !== index) {
			index = blank;
			continue;
		}
		const char = sql[index] ?? '';
		const closer = closers[char];
		if (closer !== undefined) {
			index += 1;
			while (index < sql.length && sql[index] !== closer) {
				index += sql[index] === '\\' ? 2 : 1;
			}
			index += 1;
			atStart = false;
		} else if ('([{'.includes(char)) {
			depth += 1;
			index += 1;
			atStart = false;
		} else if (')]}'.includes(char)) {
			depth = Math.max(0, depth - 1);
			index += 1;
		} else if (char === ';') {
			if (depth === 0) atStart = true;
			index += 1;
		} else if (wordChar.test(char)) {
			let end = index;
			while (end < sql.length && wordChar.test(sql[end] ?? '')) end += 1;
			const word = sql.slice(index, end).toUpperCase();
			if (depth === 0 && atStart) keywords.push(word);
			if (depth === 0 && atStart && word === 'LET') {
				let at = skipBlank(sql, end);
				if (sql[at] === '$') at += 1;
				let close = at;
				while (close < sql.length && wordChar.test(sql[close] ?? ''))
					close += 1;
				if (close > at) names.push(sql.slice(at, close));
			}
			index = end;
			atStart = false;
		} else {
			index += 1;
			atStart = false;
		}
	}
	return { keywords, lets: names };
};

/**
 * Declares a statement from its parts. The text is stored with exactly one
 * final `;`. Nothing checks that `params` are the
 * names `sql` reads, nor that `defines` and `transactional` say what the text
 * does, so prefer a builder (`select`, `create`, …) or, for authored
 * SurrealQL, `part` and `sequence`, which derive all of it from the text.
 *
 * One cheap check runs: text that appears to `LET` a name at the top level
 * must declare `defines`, since a transaction relies on it to refuse a
 * binding that the `LET` would shadow.
 */
export const make = <
	const P extends Params,
	R extends Codec,
	const Throws extends string = never,
	const Unique extends string = never,
>(options: {
	readonly name?: string;
	readonly sql: string;
	readonly params: P;
	readonly result: R;
	readonly slots?: number;
	/** The 1-based result `result` decodes, in `1..slots`. Default `slots`. */
	readonly valueSlot?: number;
	readonly throws?: ReadonlyArray<Throws>;
	/**
	 * The unique indexes a write of the text can duplicate a value of, by
	 * name. A duplicate on one of them fails with `StatementIndexConflict`,
	 * on any other with `StatementRequestError`. Default none.
	 */
	readonly uniqueIndexes?: ReadonlyArray<Unique>;
	/** The names the text `LET`s at the top level. Default none. */
	readonly defines?: ReadonlyArray<string>;
	/** The text writes its own `BEGIN`, `COMMIT` or `CANCEL`. Default false. */
	readonly transactional?: boolean;
	/** The text can change data or schema. Default false. */
	readonly writes?: boolean;
	/**
	 * The last result is a row or a non-empty list whenever the statement
	 * applies. Default false.
	 */
	readonly nonEmpty?: boolean;
	/** Where the text came from. */
	readonly sources?: ReadonlyArray<string>;
	/** The source and lines of each declared `THROW` message. */
	readonly throwSites?: Readonly<Record<string, ThrowSite>>;
	/** The static tables the text reads and writes. Default unknown. */
	readonly tables?: {
		readonly read: ReadonlyArray<string>;
		readonly written: ReadonlyArray<string>;
	};
	/** The custom functions the text calls. Default unknown. */
	readonly functions?: ReadonlyArray<string>;
	/** Some target is not a static table name. Default unknown. */
	readonly openTargets?: boolean;
	/** What each binding is used as, where known. Default unknown. */
	readonly uses?: BindingUses;
}): Statement<P, R, Throws, Unique> => {
	const uniqueIndexes = [...new Set(options.uniqueIndexes ?? [])].sort();
	const statement: Statement<P, R, Throws, Unique> = {
		[StatementTypeId]: StatementTypeId,
		name: options.name ?? 'statement',
		sql: terminated(options.sql),
		// A built statement's search references are resolved and its own, so a
		// block or a transaction that takes it in never joins them.
		params: withoutRefs(options.params),
		result: options.result,
		slots: options.slots ?? 1,
		valueSlot: options.valueSlot ?? options.slots ?? 1,
		throws: options.throws ?? [],
		...(uniqueIndexes.length > 0 && { uniqueIndexes }),
		defines: options.defines ?? [],
		transactional: options.transactional ?? false,
		...(options.writes !== undefined && { writes: options.writes }),
		nonEmpty: options.nonEmpty ?? false,
		...(options.sources !== undefined &&
			options.sources.length > 0 && { sources: options.sources }),
		...(options.throwSites !== undefined && {
			throwSites: options.throwSites,
		}),
		...(options.tables !== undefined && { tables: options.tables }),
		...(options.functions !== undefined && {
			functions: options.functions,
		}),
		...(options.openTargets !== undefined && {
			openTargets: options.openTargets,
		}),
		...(options.uses !== undefined && { uses: options.uses }),
	};
	assertStatement(statement);
	assertUniqueIndexes(statement);
	assertResolved(options.params, `statement \`${statement.name}\``);
	if (options.defines === undefined) {
		const { lets } = scanTopLevel(statement.sql);
		if (lets.length > 0) {
			throw new RangeError(
				`statement \`${statement.name}\` LETs ${[...new Set(lets)]
					.map((name) => `\`$${name}\``)
					.join(
						', ',
					)} at the top level but declares no \`defines\`, so a transaction could not refuse a binding it shadows; declare \`defines\`, or build it with \`part\` and \`sequence\``,
			);
		}
	}
	return statement;
};
