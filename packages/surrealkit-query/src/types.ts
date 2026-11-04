/**
 * The registry types: how query text resolves to its result and parameter
 * types.
 *
 * `surreal kit compile` writes a module exporting `Queries`, keyed by the
 * exact text of every query the project sends. Register it once, in any
 * `.d.ts` or `.ts` file the compiler sees:
 *
 * ```ts
 * import type { Queries } from './generated/queries';
 *
 * declare module 'surrealkit-query' {
 * 	interface Register {
 * 		queries: Queries;
 * 		// Optional: make query text that is not in the registry a type error.
 * 		strict: true;
 * 	}
 * }
 * ```
 *
 * Every binding (the core, `svelte-surrealkit`, `react-surrealkit`) reads the
 * same `Register`, so one augmentation types them all.
 */

/**
 * The augmentation point. `queries` is the generated `Queries` registry;
 * `strict: true` rejects text that is not in it.
 */
// biome-ignore lint/suspicious/noEmptyInterface: augmented by the application.
export interface Register {}

/** The shape of one generated registry entry. */
export interface RegistryEntry {
	readonly result: readonly unknown[];
	readonly params: object;
}

/** The registered `Queries`, or an empty registry when none is registered. */
export type RegisteredQueries = Register extends {
	queries: infer Q extends Record<string, RegistryEntry>;
}
	? Q
	: Record<never, RegistryEntry>;

/** Whether text outside the registry is rejected. */
export type IsStrict = Register extends { strict: true } ? true : false;

/** Query text the registry knows. */
export type KnownQuery = keyof RegisteredQueries & string;

/**
 * Text accepted where a query is expected: a registry key, or (unless the
 * registry is strict) any other string, which is untyped.
 *
 * `string & {}` keeps literal inference: a `Q extends QueryText` parameter
 * still infers the literal the caller wrote.
 */
export type QueryText = IsStrict extends true
	? KnownQuery
	: KnownQuery | (string & {});

/** The per-statement result tuple of `Q`; `unknown[]` for unregistered text. */
export type ResultOf<Q extends string> = Q extends KnownQuery
	? RegisteredQueries[Q]['result']
	: unknown[];

/**
 * The parameters of `Q`; any bindings for unregistered text. A query with no
 * parameters takes none: `Record<string, never>`, so a stray one is an error
 * (the registry's `{}` would accept anything).
 */
export type ParamsOf<Q extends string> = Q extends KnownQuery
	? keyof RegisteredQueries[Q]['params'] extends never
		? Record<string, never>
		: RegisteredQueries[Q]['params']
	: Record<string, unknown>;

/** Whether `Q` can be called without parameters. */
export type ParamsOptional<Q extends string> =
	Record<never, never> extends ParamsOf<Q> ? true : false;

/**
 * The trailing parameters argument for `Q`: optional when every parameter is
 * optional, required otherwise.
 */
export type ParamsArg<Q extends string> =
	ParamsOptional<Q> extends true
		? [params?: ParamsOf<Q>]
		: [params: ParamsOf<Q>];

/**
 * The data of a one-shot query: a single statement resolves to its own
 * result, several statements to the whole tuple.
 */
export type Rows<R> = R extends readonly [infer Only] ? Only : R;

/** The data a one-shot `Q` resolves to. */
export type DataOf<Q extends string> = Rows<ResultOf<Q>>;

/**
 * The row of a live `Q`: the element of its single array result. A query
 * whose result is not one array (several statements, `FROM ONLY`, `RETURN`)
 * has no live row, and is `never`. Unregistered text has `unknown` rows.
 */
export type LiveRowOf<Q extends string> = Q extends KnownQuery
	? RegisteredQueries[Q]['result'] extends readonly [ReadonlyArray<infer Row>]
		? Row
		: never
	: unknown;

declare const liveError: unique symbol;

/**
 * Intersected with a live query's text to reject one that cannot be live:
 * the text becomes `Q & LiveTextError`, which no literal satisfies, and the
 * compiler names the error.
 */
export interface LiveTextError {
	readonly [liveError]: 'a live query must be one SELECT whose result is an array of rows';
}

/** `Q` itself when it can be live, otherwise an unsatisfiable `Q`. */
export type LiveText<Q extends string> = [LiveRowOf<Q>] extends [never]
	? Q & LiveTextError
	: Q;

/**
 * One live notification for a row type: `CREATE`, `UPDATE` and `DELETE`
 * carry the record id and the row, `KILLED` carries neither.
 *
 * The row type is the SELECT's own row. The engine's live typing (a
 * `LiveQueries` registry keyed by the same text) will refine it per action.
 */
export type LiveNotification<Row> =
	| {
			readonly action: 'CREATE' | 'UPDATE' | 'DELETE';
			readonly recordId: unknown;
			readonly value: Row;
	  }
	| {
			readonly action: 'KILLED';
			readonly recordId?: undefined;
			readonly value?: undefined;
	  };

/**
 * A query result fetched on the server, carried to the client with the query
 * that produced it.
 *
 * `text` keeps the query's literal type, so a component handed a payload is
 * typed from the registry without naming the query again. `params` and
 * `data` are CBOR, base64-encoded, so SDK values (`RecordId`, `DateTime`)
 * survive SvelteKit's `load` and a React Server Component's props unchanged.
 */
export interface Preloaded<Q extends string = string> {
	readonly surrealkit: 1;
	readonly text: Q;
	readonly params: string | undefined;
	/** The whole per-statement result tuple. */
	readonly data: string;
}

/**
 * A component's `params` prop for `Q`: always accepted, and required when
 * `Q` has a required parameter.
 */
export type ParamsProp<Q extends string> = {
	params?: ParamsOf<Q> | undefined;
} & (ParamsOptional<Q> extends true
	? Record<never, never>
	: { params: ParamsOf<Q> });
