import { Effect, Stream } from 'effect';

import type { Client } from './client.js';
import type {
	LiveQueryClosed,
	StatementDecodeError,
	StatementEncodeError,
	StatementRequestError,
	StatementUnsupported,
} from './errors.js';
import type { Transport } from './internal/transport.js';

export interface LiveEvent<Row, Id> {
	readonly action: 'CREATE' | 'UPDATE' | 'DELETE';
	readonly id: Id;
	/** The record after the change, or the deleted record for DELETE. */
	readonly value: Row;
}

export type LiveError =
	| StatementEncodeError
	| StatementRequestError
	| StatementDecodeError
	| StatementUnsupported
	| LiveQueryClosed;

export const makeLive = (
	_transport: Transport,
): Pick<Client, 'live' | 'liveScoped'> => ({
	live: () =>
		Stream.die(
			new Error(
				'effect-surrealkit: live is not implemented yet (lane L7)',
			),
		),
	liveScoped: () =>
		Effect.die(
			new Error(
				'effect-surrealkit: liveScoped is not implemented yet (lane L7)',
			),
		),
});
