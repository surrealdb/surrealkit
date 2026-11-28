/**
 * Statements built from graph entry points that start at the binding
 * `$who`: a step, a step from a step, `follow` (once and through a link
 * it reads), `destructure` (with a step from the target's own `id`),
 * `edges`, `rows`, `selectFrom` and `collect`, each under `evaluate` and in
 * a projection. Every one reads `$who`, so its statement must ask for it.
 * Imported by the positive and negative graph fixtures.
 */
import {
	collect,
	destructure,
	edges,
	evaluate,
	follow,
	incoming,
	outgoing,
	param,
	rows,
	select,
	selectFrom,
} from '../../src/statement/index.js';
import { PersonId, PersonTable, store } from '../golden/corpus/index.js';

const who = param('who', PersonId);
const knows = () => outgoing(store, who, 'knows');

export const evaluated = {
	outgoing: evaluate(knows()),
	incoming: evaluate(incoming(store, who, 'knows')),
	nested: evaluate(outgoing(store, knows(), 'knows')),
	follow: evaluate(follow(store, knows()).name),
	followTwice: evaluate(follow(store, follow(store, knows()).manager).name),
	destructure: evaluate(
		destructure(store, knows(), (peer) => ({ name: peer.name })),
	),
	destructureStep: evaluate(
		destructure(store, knows(), (peer) => ({
			peers: outgoing(store, peer.id, 'knows'),
		})),
	),
	edges: evaluate(edges(knows())),
	rows: evaluate(rows(store, knows())),
	collect: evaluate(collect(who, (node) => outgoing(store, node, 'knows'))),
};

export const projected = {
	outgoing: select(PersonTable)
		.project(() => ({ x: knows() }))
		.build(),
	incoming: select(PersonTable)
		.project(() => ({ x: incoming(store, who, 'knows') }))
		.build(),
	nested: select(PersonTable)
		.project(() => ({ x: outgoing(store, knows(), 'knows') }))
		.build(),
	follow: select(PersonTable)
		.project(() => ({ x: follow(store, knows()).name }))
		.build(),
	followTwice: select(PersonTable)
		.project(() => ({
			x: follow(store, follow(store, knows()).manager).name,
		}))
		.build(),
	destructure: select(PersonTable)
		.project(() => ({
			x: destructure(store, knows(), (peer) => ({ name: peer.name })),
		}))
		.build(),
	destructureStep: select(PersonTable)
		.project(() => ({
			x: destructure(store, knows(), (peer) => ({
				peers: outgoing(store, peer.id, 'knows'),
			})),
		}))
		.build(),
	selectFrom: selectFrom(store, knows()).build(),
};
