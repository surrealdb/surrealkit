/** What the testing helpers fail with. */
import { Data } from 'effect';

/**
 * A test database could not be started or prepared: `@surrealdb/node` is
 * not installed or did not start, schema text failed, or a record user could
 * not sign up. `cause` is the SDK's or the engine's error.
 */
export class TestingError extends Data.TaggedError('TestingError')<{
	readonly message: string;
	readonly cause?: unknown;
}> {}

/**
 * The engine's plan for a statement did not read the index as expected:
 * `expected` says whether it should have, `used` lists the indexes the plan
 * does read, and `plan` is the plan as `EXPLAIN` returned it.
 */
export class IndexExpectationError extends Data.TaggedError(
	'IndexExpectationError',
)<{
	readonly statement: string;
	readonly index: string;
	readonly expected: 'used' | 'unused';
	readonly used: ReadonlyArray<string>;
	readonly plan: unknown;
	readonly message: string;
}> {}
