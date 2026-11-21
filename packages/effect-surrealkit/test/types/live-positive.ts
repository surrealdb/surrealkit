import type { Effect, Scope, Stream } from 'effect';
import type { Client, LiveError, LiveEvent } from '../../src/client/index.js';
import * as S from '../../src/index.js';
import { gte, live, param, readAs } from '../../src/statement/index.js';
import { PersonTable } from '../golden/corpus/index.js';

declare const client: Client;
const statement = live(readAs(PersonTable))
	.where((row) => gte(row.age, param('minimum', S.Int)))
	.project((row) => ({ label: row.name }))
	.build('people');
type Event = LiveEvent<{ readonly label: string }, typeof PersonTable.id.Type>;
const liveStream: Stream.Stream<Event, LiveError> = client.live(statement, {
	minimum: 18,
});
const scoped: Effect.Effect<
	Stream.Stream<Event, LiveError>,
	LiveError,
	Scope.Scope
> = client.liveScoped(statement, { minimum: 18 });
export { liveStream, scoped };
