/**
 * A database for tests: an in-memory engine (`@surrealdb/node`, imported only
 * when one is started), or any connection a test already has, with schema
 * text to load and record users to sign up.
 */
import { Context, Effect, Layer, type Scope } from 'effect';
import { Surreal, type SurrealSession } from 'surrealdb';

import { type Client, make, SurrealClient } from '../client/client.js';
import { TestingError } from './errors.js';

/**
 * What the helpers need from an SDK connection: queries, and new sessions
 * for record users.
 */
export type Connection = Pick<Surreal, 'query' | 'transaction' | 'newSession'>;

/** A database a test owns: its connection, scope and client. */
export interface Database {
	readonly connection: Connection;
	readonly namespace: string;
	readonly database: string;
	/** A client with the connection's own (root) authentication. */
	readonly client: Client;
	/** Runs SurrealQL schema text (`DEFINE …`) in the database. */
	readonly load: (schema: string) => Effect.Effect<void, TestingError>;
}

/** The test database in context. */
export class TestDatabase extends Context.Service<TestDatabase, Database>()(
	'effect-surrealkit/testing/TestDatabase',
) {}

const messageOf = (cause: unknown): string =>
	cause instanceof Error ? cause.message : String(cause);

/**
 * A {@link Database} over a connection that is already in `namespace` and
 * `database` with the authentication its statements should run with.
 */
export const fromConnection = (
	connection: Connection,
	scope: { readonly namespace: string; readonly database: string },
): Database => ({
	connection,
	namespace: scope.namespace,
	database: scope.database,
	client: make(connection),
	load: (schema) =>
		Effect.tryPromise({
			try: () => connection.query(schema).then(() => undefined),
			catch: (cause) =>
				new TestingError({
					message: `the schema text failed: ${messageOf(cause)}`,
					cause,
				}),
		}),
});

/** Provides {@link TestDatabase} and its {@link SurrealClient}. */
const provide = (database: Database) =>
	Layer.succeedContext(
		Context.make(TestDatabase, database).pipe(
			Context.add(SurrealClient, database.client),
		),
	);

/** {@link TestDatabase} and {@link SurrealClient} over an existing connection. */
export const layerFrom = (
	connection: Connection,
	scope: { readonly namespace: string; readonly database: string },
): Layer.Layer<TestDatabase | SurrealClient> =>
	provide(fromConnection(connection, scope));

export interface MemoryOptions {
	/** SurrealQL schema text to load once the database is up. */
	readonly schema?: string;
	/** Default `test`. */
	readonly namespace?: string;
	/** Default `test`. */
	readonly database?: string;
}

interface NodeEngines {
	readonly createNodeEngines: () => unknown;
}

/**
 * `@surrealdb/node`, imported when a memory database starts, so nothing else
 * in the package needs it installed or reachable.
 */
const nodeEngines = Effect.tryPromise({
	try: () => import('@surrealdb/node') as Promise<NodeEngines>,
	catch: (cause) =>
		new TestingError({
			message: `a memory database needs \`@surrealdb/node\` and \`@surrealdb/node-native\` installed: ${messageOf(cause)}`,
			cause,
		}),
});

/**
 * Starts an in-memory SurrealDB engine in this process and closes it when the
 * scope closes. Each call is a database of its own.
 */
export const memory = (
	options: MemoryOptions = {},
): Effect.Effect<Database, TestingError, Scope.Scope> =>
	Effect.gen(function* () {
		const { createNodeEngines } = yield* nodeEngines;
		const scope = {
			namespace: options.namespace ?? 'test',
			database: options.database ?? 'test',
		};
		const db = yield* Effect.acquireRelease(
			Effect.tryPromise({
				try: async () => {
					const surreal = new Surreal({
						engines: createNodeEngines() as never,
					});
					try {
						await surreal.connect('mem://', scope);
						return surreal;
					} catch (cause) {
						await surreal.close();
						throw cause;
					}
				},
				catch: (cause) =>
					new TestingError({
						message: `the memory database did not start: ${messageOf(cause)}`,
						cause,
					}),
			}),
			(surreal) => Effect.promise(() => surreal.close()),
		);
		const database = fromConnection(db, scope);
		if (options.schema !== undefined) yield* database.load(options.schema);
		return database;
	});

/**
 * {@link TestDatabase} and its {@link SurrealClient} over a fresh in-memory
 * engine, with `options.schema` loaded.
 *
 * @example
 * it.effect('finds an adult', () =>
 *   run(adults, { minimum: 18 }).pipe(
 *     Effect.provide(memoryLayer({ schema: 'DEFINE TABLE person …;' })),
 *   ),
 * );
 */
export const memoryLayer = (
	options: MemoryOptions = {},
): Layer.Layer<TestDatabase | SurrealClient, TestingError> =>
	Layer.unwrap(Effect.map(memory(options), provide));

/** Loads schema text into the {@link TestDatabase} in context. */
export const loadSchema = (
	schema: string,
): Effect.Effect<void, TestingError, TestDatabase> =>
	Effect.flatMap(Effect.service(TestDatabase), (database) =>
		database.load(schema),
	);

export interface RecordUserOptions {
	/** The `DEFINE ACCESS … TYPE RECORD` to sign up through. */
	readonly access: string;
	/** The variables its `SIGNUP` reads (`$name`, `$pass`, …). */
	readonly variables?: Readonly<Record<string, unknown>>;
}

/** A record user's session and a client that runs statements as it. */
export interface RecordUser {
	readonly session: SurrealSession;
	readonly client: Client;
}

/**
 * Signs a new record user up through an access method of the
 * {@link TestDatabase} and returns a client for that session, so a test can
 * prove what a restricted session sees. The session closes with the scope.
 *
 * @example
 * const ann = yield* recordUser({ access: 'user', variables: { name: 'ann' } });
 * const rows = yield* ann.client.run(visible);
 */
export const recordUser = (
	options: RecordUserOptions,
): Effect.Effect<RecordUser, TestingError, TestDatabase | Scope.Scope> =>
	Effect.gen(function* () {
		const database = yield* Effect.service(TestDatabase);
		const scope = {
			namespace: database.namespace,
			database: database.database,
		};
		const session = yield* Effect.acquireRelease(
			Effect.tryPromise({
				try: () => database.connection.newSession(),
				catch: (cause) =>
					new TestingError({
						message: `no session for a record user: ${messageOf(cause)}`,
						cause,
					}),
			}),
			(opened) => Effect.promise(() => opened.closeSession()),
		);
		yield* Effect.tryPromise({
			try: async () => {
				await session.use(scope);
				await session.signup({
					...scope,
					access: options.access,
					variables: { ...options.variables },
				});
			},
			catch: (cause) =>
				new TestingError({
					message: `the record user did not sign up through \`${options.access}\`: ${messageOf(cause)}`,
					cause,
				}),
		});
		return { session, client: make(session) };
	});
