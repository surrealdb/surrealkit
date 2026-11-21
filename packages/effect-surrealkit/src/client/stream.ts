import { Stream } from 'effect';

import type { Client } from './client.js';
import type { Transport } from './internal/transport.js';

export const makeStream =
	(_transport: Transport): Client['stream'] =>
	() =>
		Stream.die(
			new Error(
				'effect-surrealkit: stream is not implemented yet (lane L6)',
			),
		);
