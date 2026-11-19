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
 * - `defines`: the names the text `LET`s at the top level. Statements in one
 *   transaction share one variable scope, so a client refuses to bind a name
 *   another statement of the same transaction defines.
 * - `transactional`: the text writes its own `BEGIN`, `COMMIT` or `CANCEL`,
 *   so it cannot run inside a client's `transaction`.
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
	readonly throws: ReadonlyArray<Throws>;
	readonly defines: ReadonlyArray<string>;
	readonly transactional: boolean;
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
	for (const name of statement.defines) assertIdentifier(name);
};

/**
 * Declares a statement from its parts. Nothing checks that `params` are the
 * names `sql` reads, nor that `defines` and `transactional` say what the text
 * does, so prefer a builder (`select`, `create`, …) or, for authored
 * SurrealQL, `part` and `sequence`, which derive all of it from the text.
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
	readonly throws?: ReadonlyArray<Throws>;
	/** The names the text `LET`s at the top level. Default none. */
	readonly defines?: ReadonlyArray<string>;
	/** The text writes its own `BEGIN`, `COMMIT` or `CANCEL`. Default false. */
	readonly transactional?: boolean;
}): Statement<P, R, Throws> => {
	const statement: Statement<P, R, Throws> = {
		[StatementTypeId]: StatementTypeId,
		name: options.name ?? 'statement',
		sql: options.sql,
		params: options.params,
		result: options.result,
		slots: options.slots ?? 1,
		throws: options.throws ?? [],
		defines: options.defines ?? [],
		transactional: options.transactional ?? false,
	};
	assertStatement(statement);
	return statement;
};
