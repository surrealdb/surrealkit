import { Schema, type Stream } from 'effect';

import {
	type Client,
	type StatementError,
	type StatementUnsupported,
	type SurrealClient,
	stream,
} from '../../src/client/index.js';
import { make, select } from '../../src/statement/index.js';
import { type PersonRow, PersonTable } from '../golden/example/index.js';

type Same<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;
const same = <A, B>(_: Same<A, B>): void => undefined;

const people = select(PersonTable).build();
const guarded = make({
	name: 'guarded numbers',
	sql: `IF $enabled = false { THROW 'disabled'; }; ['1', '2'];`,
	params: { enabled: Schema.Boolean },
	result: Schema.Array(Schema.NumberFromString),
	slots: 2,
	throws: ['disabled'],
	writes: false,
});

export const streamTypes = (client: Client): void => {
	const direct = client.stream(people);
	same<
		typeof direct,
		Stream.Stream<
			PersonRow,
			StatementError<never, never> | StatementUnsupported
		>
	>(true);
	const inContext = stream(guarded, { enabled: true });
	same<
		typeof inContext,
		Stream.Stream<
			number,
			StatementError<'disabled', never> | StatementUnsupported,
			SurrealClient
		>
	>(true);
};
