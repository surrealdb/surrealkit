/**
 * Helpers for testing code built on `effect-surrealkit/statement` and
 * `effect-surrealkit/client`: an in-memory database (from `@surrealdb/node`,
 * an optional peer dependency imported only when one starts), record users,
 * index expectations read from the engine's plan, a check of a store's
 * declarations against a live database, and a scripted client.
 *
 * Nothing else in the package imports this module, so the main entry points
 * stay host-neutral and installable without `@surrealdb/node`.
 */
export {
	type Connection,
	type Database,
	fromConnection,
	layerFrom,
	loadSchema,
	type MemoryOptions,
	memory,
	memoryLayer,
	type RecordUser,
	type RecordUserOptions,
	recordUser,
	TestDatabase,
} from './database.js';
export {
	type DeclaredStore,
	type DeclaredTable,
	type SchemaDifference,
	schemaDrift,
} from './drift.js';
export { IndexExpectationError, TestingError } from './errors.js';
export { expectIndex, expectNoIndex, explain, indexesOf } from './explain.js';
export {
	type Call,
	conflict,
	events,
	type FakeClient,
	failure,
	fake,
	fakeLayer,
	type Reply,
	result,
	type Script,
	thrown,
	unsupported,
} from './fake.js';
export { conflictingSession } from './session.js';
