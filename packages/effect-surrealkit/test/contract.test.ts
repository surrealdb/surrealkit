import { Surreal } from 'surrealdb';
import { describe, expect, it } from 'vitest';

import * as client from '../src/client/index.js';
import * as statement from '../src/statement/index.js';

describe('statement execution contract', () => {
	it('exports the execution accessors and errors from the client entry', () => {
		expect(client).toMatchObject({
			BulkWriteError: expect.any(Function),
			LiveQueryClosed: expect.any(Function),
			StatementUnsupported: expect.any(Function),
			TransactionOutcomeUnknown: expect.any(Function),
			pipeline: expect.any(Function),
			stream: expect.any(Function),
			timed: expect.any(Function),
			withTransaction: expect.any(Function),
		});
	});

	it('assembles every execution method on the client', () => {
		expect(client.make(new Surreal())).toMatchObject({
			pipeline: expect.any(Function),
			withTransaction: expect.any(Function),
			bulk: expect.any(Function),
			stream: expect.any(Function),
			liveScoped: expect.any(Function),
			live: expect.any(Function),
		});
	});

	it('exports the new statement builders and identifiers from the statement entry', () => {
		expect(statement).toMatchObject({
			LiveStatementTypeId: '~effect-surrealkit/LiveStatement',
			live: expect.any(Function),
			renameNames: expect.any(Function),
			updateEach: expect.any(Function),
		});
	});
});
