/**
 * A statement: one request to SurrealDB, described as a value.
 *
 * The SurrealQL text is static. Every value enters through a named binding
 * that carries its own codec, so the text never depends on input, and the
 * final result is decoded by a codec the statement names. Nothing here talks
 * to a database: `effect-surrealkit/client` runs statements.
 */
import type { Schema } from 'effect';

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
 * The values a caller supplies: one per binding, as the codec's decoded type.
 * A binding whose codec accepts `undefined` (NONE) may be omitted.
 */
export type Input<P extends Params> = Simplify<
	{
		readonly [K in Exclude<keyof P, OptionalNames<P>>]: P[K]['Type'];
	} & {
		readonly [K in OptionalNames<P>]?: P[K]['Type'];
	}
>;

export const StatementTypeId = '~effect-surrealkit/Statement';

/**
 * - `sql`: the SurrealQL text, one or more statements.
 * - `params`: the codec of every binding the text reads. Values are encoded
 *   through them before anything is sent.
 * - `result`: decodes the result of the last statement of the text.
 * - `slots`: how many results the text returns, one per top-level statement.
 *   Every one but the last must be NONE (`LET`, guards).
 * - `throws`: the `THROW` messages the text can raise, which a client
 *   surfaces as a typed error rather than a request failure.
 */
export interface Statement<
	P extends Params = Params,
	R extends Codec = Codec,
	Throws extends string = never,
> {
	readonly [StatementTypeId]: typeof StatementTypeId;
	readonly name: string;
	readonly sql: string;
	readonly params: P;
	readonly result: R;
	readonly slots: number;
	readonly defines?: ReadonlyArray<string>;
	readonly throws: ReadonlyArray<Throws>;
}

/** Any statement, whatever it binds, returns and throws. */
export type AnyStatement = Statement<Params, Codec, string>;

/** The decoded result of a statement. */
export type Result<S> =
	S extends Statement<Params, infer R, string> ? R['Type'] : never;

/** The messages a statement declares it can `THROW`. */
export type Thrown<S> = S extends Statement<Params, Codec, infer T> ? T : never;

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

const assertIdentifier = (name: string): void => {
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

const assertStatement = (statement: AnyStatement): void => {
	if (statement.sql.trim().length === 0) {
		throw new RangeError(`statement \`${statement.name}\` has no text`);
	}
	if (!Number.isInteger(statement.slots) || statement.slots < 1) {
		throw new RangeError(
			`statement \`${statement.name}\` returns ${statement.slots} results; a statement returns at least one`,
		);
	}
	for (const name of Object.keys(statement.params)) assertBindingName(name);
};

/**
 * Declares a statement from its parts. Nothing checks that `params` are the
 * names `sql` reads, so prefer a builder (`select`, `create`, …) or, for
 * authored SurrealQL, {@link sequence}, which checks the bindings against the
 * names each part reads.
 */
export const make = <
	const P extends Params,
	R extends Codec,
	const Throws extends string = never,
>(options: {
	readonly name?: string;
	readonly sql: string;
	readonly params: P;
	readonly result: R;
	readonly slots?: number;
	readonly defines?: ReadonlyArray<string>;
	readonly throws?: ReadonlyArray<Throws>;
}): Statement<P, R, Throws> => {
	const statement: Statement<P, R, Throws> = {
		[StatementTypeId]: StatementTypeId,
		name: options.name ?? 'statement',
		sql: options.sql,
		params: options.params,
		result: options.result,
		defines: options.defines ?? [],
		slots: options.slots ?? 1,
		throws: options.throws ?? [],
	};
	assertStatement(statement);
	return statement;
};

/**
 * One authored SurrealQL text and what it does with variables:
 *
 * - `free`: the `$names` it reads and does not define itself;
 * - `defines`: the names it `LET`s at the top level, which later parts of a
 *   sequence can read;
 * - `slots`: how many results it returns (one per top-level statement);
 * - `throws`: the messages it can `THROW`;
 * - optionally `name` and `source` (where the text came from) and `throwsAt`
 *   (the lines each message is thrown on), for messages and tooling.
 *
 * `effect-surrealkit statements` derives these from `.surql` files
 * (`effect-surrealkit/analyze`); written by hand they are checked only as
 * far as {@link sequence} can check them.
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
}

/** Declares a {@link Part}, keeping its names as literal types. */
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
}): Part<Free, Defines, Throws> => ({
	...(options.name !== undefined && { name: options.name }),
	...(options.source !== undefined && { source: options.source }),
	sql: options.sql,
	free: options.free ?? [],
	defines: options.defines ?? [],
	slots: options.slots ?? 1,
	throws: options.throws ?? [],
	...(options.throwsAt !== undefined && { throwsAt: options.throwsAt }),
});

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
 * `P` must name exactly `Names`: a missing binding is a missing property, and
 * a binding nothing reads is typed as a message naming it.
 */
export type Exactly<Names extends string, P> = {
	readonly [K in Names]: Codec;
} & {
	readonly [K in Exclude<
		keyof P,
		Names
	>]: `$${K & string} is not read by this sequence`;
};

/**
 * The contract of a sequence. Parts that are not typed as literals (a
 * widened list, names typed `string`) cannot be checked by the compiler, so
 * they are left to the check `define` makes when it runs.
 */
export type Contract<
	Parts extends ReadonlyArray<Part>,
	P,
> = number extends Parts['length']
	? unknown
	: string extends Unbound<Parts>
		? unknown
		: Exactly<Unbound<Parts>, P>;

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

const reads = (sql: string, name: string): boolean =>
	new RegExp(`\\$${name}(?![A-Za-z0-9_])`, 'u').test(sql);

const assertPart = (item: Part, index: number): void => {
	const text = item.sql.trim();
	if (!text.endsWith(';')) {
		throw new RangeError(
			`part ${index + 1} of the sequence does not end with \`;\`, so the next part would run into it`,
		);
	}
	if (!Number.isInteger(item.slots) || item.slots < 0) {
		throw new RangeError(
			`part ${index + 1} of the sequence returns ${item.slots} results`,
		);
	}
	for (const name of [...item.free, ...item.defines]) {
		// A part may define, and a later part read, a name a caller could
		// not bind (`LET $event = …`): only what is left unbound is checked
		// as a binding, when the statement is made.
		assertIdentifier(name);
		if (!reads(text, name)) {
			throw new RangeError(
				`part ${index + 1} of the sequence lists \`$${name}\`, which its text never mentions`,
			);
		}
	}
};

/**
 * Authored SurrealQL texts run one after another in a single request, sharing
 * one variable scope, as a statement.
 *
 * `define` takes a codec for exactly the names the sequence still needs:
 * leaving one out, or declaring one no part reads, is a compile error, and
 * the same check runs again when the statement is defined, in case the parts
 * were not typed as literals.
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
		readonly params: P & Contract<Parts, P>;
		readonly result: R;
	}): Statement<P, R, ThrownBy<Parts>> => {
		parts.forEach(assertPart);
		const expected = unboundOf(parts);
		const declared = Object.keys(contract.params);
		const missing = expected.filter((name) => !declared.includes(name));
		const extra = declared.filter((name) => !expected.includes(name));
		if (missing.length > 0 || extra.length > 0) {
			const list = (names: ReadonlyArray<string>) =>
				names.map((name) => `$${name}`).join(', ');
			throw new RangeError(
				[
					`the bindings of sequence \`${contract.name ?? 'sequence'}\` do not match the names it reads`,
					...(missing.length > 0
						? [`missing: ${list(missing)}`]
						: []),
					...(extra.length > 0
						? [`not read by any part: ${list(extra)}`]
						: []),
				].join('; '),
			);
		}
		return make({
			name: contract.name ?? 'sequence',
			defines: parts.flatMap((item) => item.defines),
			sql: parts.map((item) => item.sql.trim()).join('\n'),
			params: contract.params as P,
			result: contract.result,
			slots: parts.reduce((total, item) => total + item.slots, 0),
			throws: [
				...new Set(parts.flatMap((item) => item.throws)),
			] as ReadonlyArray<ThrownBy<Parts>>,
		});
	},
});
