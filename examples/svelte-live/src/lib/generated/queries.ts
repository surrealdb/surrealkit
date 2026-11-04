// Written by hand, in the shape `surreal kit compile` generates, so this
// example runs before the engine types queries (SurrealDB 4.1). Once it does,
// `npm run types` regenerates this file from the queries in src.
//
// Type a client's `query()` by exact query text:
//
//   const db = new Surreal() as Typed<Surreal>;
//   const [people] = await db.query("SELECT name FROM person");

import type { BoundQuery, DateTime, QueryLike, RecordId, SurrealQueryable } from "surrealdb";

/** Every query, keyed by its exact text. */
export type Queries = {
  "SELECT id, name, team, joined FROM person ORDER BY name LIMIT $lim START $start": { result: [Array<{ id: RecordId<"person">; joined: DateTime; name: string; team: string }>]; params: { lim: number; start: number } };
  "SELECT id, name FROM person WHERE team = $team": { result: [Array<{ id: RecordId<"person">; name: string }>]; params: { team: string } };
  "SELECT count() AS total FROM person GROUP ALL": { result: [Array<{ total: number }>]; params: {} };
  "CREATE person SET name = $name, team = $team": { result: [Array<{ id: RecordId<"person">; joined: DateTime; name: string; team: string }>]; params: { name: string; team: string } };
  "DELETE $id": { result: [[]]; params: { id: RecordId<"person"> } };
};

declare const queryable: SurrealQueryable;
/** The SDK's `Query<R>`, which it does not export by name. */
export type QueryOf<R extends unknown[]> = ReturnType<typeof queryable.query<R>>;
type ParamsOf<K extends keyof Queries> = Queries[K]["params"];
/** A query that reads no parameter takes no bindings; one whose parameters are all optional may omit them. */
type BindingsArg<K extends keyof Queries> = [keyof ParamsOf<K>] extends [never]
  ? [bindings?: Record<string, never>]
  : {} extends ParamsOf<K>
    ? [bindings?: ParamsOf<K>]
    : [bindings: ParamsOf<K>];
/** Query text that is not a key of `Queries`; a known text is `never`, so it cannot reach the untyped overload. */
type UnknownText<S extends string> = S extends keyof Queries ? never : S;
/**
 * `query()` typed by exact query text: a known text gets the registry's result type and must be
 * given its parameters' types. Any other text, and the SDK's other forms, keep the SDK's own types.
 */
export interface TypedQueryable {
  query<K extends keyof Queries>(query: K, ...bindings: BindingsArg<K>): QueryOf<Queries[K]["result"]>;
  query<R extends unknown[] = unknown[], S extends string = string>(
    query: UnknownText<S>,
    bindings?: Record<string, unknown>,
  ): QueryOf<R>;
  query<R extends unknown[] = unknown[]>(query: BoundQuery<R>): QueryOf<R>;
  query<R extends unknown[] = unknown[]>(queries: readonly QueryLike[]): QueryOf<R>;
}
/** A client whose `query()` returns the registry's result type for known query text. */
export type Typed<T extends SurrealQueryable> = Omit<T, "query"> & TypedQueryable;
