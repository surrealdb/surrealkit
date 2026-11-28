import type { Stream } from 'effect';
import {
	bindLive,
	type Client,
	type SurrealClient,
	type WatchQueryError,
	type WatchQueryState,
	watchQuery,
} from '../../src/client/index.js';
import * as S from '../../src/index.js';
import { gte, live, param, select } from '../../src/statement/index.js';
import { PersonTable } from '../golden/corpus/index.js';

declare const client: Client;

const read = select(PersonTable)
	.where((row) => gte(row.age, param('readAge', S.Int)))
	.project((row) => ({ label: row.name }))
	.build('people.read', { writes: false });
const changes = live(PersonTable)
	.where((row) => gte(row.age, param('sourceAge', S.Int)))
	.build('people.changes');

const options = { sources: [bindLive(changes, { sourceAge: 0 })] } as const;

type State = WatchQueryState<ReadonlyArray<{ readonly label: string }>>;

export const fromClient: Stream.Stream<State, WatchQueryError> =
	client.watchQuery(read, options, { readAge: 18 });
export const fromContext: Stream.Stream<State, WatchQueryError, SurrealClient> =
	watchQuery(read, options, { readAge: 18 });

// @ts-expect-error The source binding is required.
bindLive(changes);
// @ts-expect-error Source and snapshot bindings are checked separately.
bindLive(changes, { readAge: 18 });
// @ts-expect-error A live source requires checked bindings.
client.watchQuery(read, { sources: [changes] }, { readAge: 18 });
// @ts-expect-error At least one live source is required.
watchQuery(read, { sources: [] }, { readAge: 18 });
// @ts-expect-error The snapshot binding is required.
watchQuery(read, options);
// @ts-expect-error The snapshot binding must match its codec.
client.watchQuery(read, options, { readAge: '18' });
