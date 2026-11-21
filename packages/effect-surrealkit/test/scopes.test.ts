/**
 * Which row a field path reads: a path is relative text, so inside a step's
 * `where` it reads the edge and inside a destructuring the record it
 * destructures. A path of any other row is refused where the callback's
 * result is placed, `parent(row.field)` reads the select's row from a step
 * directly under it, and the places where SurrealDB 3.3 and 4.0 bind
 * `$parent` differently refuse it (`test/conformance.test.ts` pins both).
 */
import { describe, expect, it } from 'vitest';

import * as SurrealSchema from '../src/index.js';
import {
	and,
	block,
	collect,
	create,
	destructure,
	eq,
	evaluate,
	follow,
	inside,
	literal,
	ne,
	outgoing,
	param,
	parent,
	rowOf,
	rows,
	select,
	update,
} from '../src/statement/index.js';
import {
	MemberOfTable,
	Person,
	PersonId,
	PersonMerge,
	PersonTable,
	store,
	TeamTable,
} from './golden/corpus/index.js';

/** A builder with its types switched off, to reach its run-time checks. */
const untyped = <F>(f: F) =>
	f as unknown as (...args: ReadonlyArray<unknown>) => unknown;

describe('a field of the outer row', () => {
	it('is refused in a step’s where, which reads the edge', () => {
		expect(() =>
			select(MemberOfTable).project((row) => ({
				x: outgoing(store, row.in, 'member_of', {
					where: (edge) => ne(edge.out, row.out),
				}),
			})),
		).toThrow(
			/`out` is a field of the row of a select of `member_of`, but in a step's `where` a field path reads the edge \(`member_of`\), so it would read that row's `out` instead\. To read the select's row from a step's `where` directly under it, use `parent\(row\.out\)`/,
		);
	});

	it('is refused in a destructuring, which reads the record it destructures', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: destructure(store, row.manager, (boss) => ({
					boss: boss.name,
					mine: row.name,
				})),
			})),
		).toThrow(
			/`name` is a field of the row of a select of `person`, but in a destructuring callback a field path reads the record it destructures \(`person`\).*project the outer field beside the destructured object/,
		);
	});

	it('is refused through a link or a step built from it', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: destructure(store, row.manager, () => ({
					friends: rows(store, outgoing(store, row.id, 'knows')),
				})),
			})),
		).toThrow(/`id` is a field of the row of a select of `person`/);
		expect(() =>
			select(PersonTable).project((row) => ({
				x: destructure(store, row.manager, () => ({
					city: follow(store, row.manager).name,
				})),
			})),
		).toThrow(/`manager` is a field of the row of a select of `person`/);
	});

	it('is refused in a subquery, which reads its own row', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				peers: select(PersonTable)
					.where((peer) => eq(peer.handle, row.handle))
					.value((peer) => peer.name)
					.toExpr(),
			})),
		).toThrow(
			/`handle` is a field of the row of a select of `person`, but in a select of `person` a field path reads the row of `person`/,
		);
	});

	it('is refused in a block and a bare RETURN, which have no row', () => {
		const person = rowOf(Person);
		const scoped = select(PersonTable);
		let field: unknown;
		scoped.where((row) => {
			field = row.name;
			return eq(row.name, 'x');
		});
		expect(() =>
			block()
				.return(() => ({ name: field as typeof person.name }))
				.build(),
		).toThrow(
			/`name` is a field of the row of a select of `person`, and a block has no row to read it from/,
		);
		expect(() => evaluate(field as typeof person.name)).toThrow(
			/and a bare `RETURN` has no row to read it from/,
		);
		// A row object of `rowOf` reads whichever row is current.
		expect(evaluate(person.name).sql).toBe('RETURN name;');
	});

	it('is refused in a write’s callbacks', () => {
		let field: unknown;
		select(PersonTable).where((row) => {
			field = row.name;
			return eq(row.name, 'x');
		});
		expect(() =>
			update(PersonTable)
				.merge(param('changes', PersonMerge))
				.where((row) => eq(row.name, field as never))
				.build(),
		).toThrow(/in an UPDATE a field path reads the row of `person`/);
		expect(() =>
			create(PersonTable)
				.content(param('p', SurrealSchema.SurrealValue) as never)
				.returning((row) => ({ a: row.name, b: field as never }))
				.build(),
		).toThrow(/in a CREATE a field path reads the row of `person`/);
	});

	it('leaves bindings, constants and the callback’s own row legal', () => {
		const statement = select(PersonTable)
			.project((row) => ({
				teams: outgoing(store, row.id, 'member_of', {
					where: (edge) =>
						and(
							eq(edge.role, param('role', SurrealSchema.String)),
							ne(edge.role, literal('member')),
						),
				}),
				boss: destructure(store, row.manager, (boss) => ({
					name: boss.name,
					tag: literal('boss'),
					who: param('who', SurrealSchema.String),
				})),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT id->(member_of WHERE role = $role AND role != s"member")->? AS teams, manager.{ name: name, tag: s"boss", who: $who } AS boss FROM ⟨person⟩;',
		);
	});
});

describe('parent(row.field)', () => {
	it('reads the select’s row from a step’s where directly under it', () => {
		const statement = select(PersonTable)
			.where((row) =>
				inside(
					param('who', PersonId),
					outgoing(store, row.id, 'knows', {
						where: (edge) => ne(edge.out, parent(row.manager)),
					}),
				),
			)
			.project((row) => ({
				peers: outgoing(store, row.id, 'knows', {
					where: (edge) => ne(edge.out, parent(row.id)),
				}),
				chained: outgoing(
					store,
					outgoing(store, row.id, 'knows'),
					'member_of',
					{ where: (edge) => eq(edge.role, parent(row.name)) },
				),
				fromBinding: outgoing(store, param('who', PersonId), 'knows', {
					where: (edge) => ne(edge.out, parent(row.id)),
				}),
				destructured: destructure(
					store,
					outgoing(store, row.id, 'knows', {
						where: (edge) => ne(edge.out, parent(row.id)),
					}),
					(peer) => ({ name: peer.name }),
				),
			}))
			.build();
		expect(statement.sql).toBe(
			'SELECT id->(knows WHERE out != $parent.id)->? AS peers, id->knows->?->(member_of WHERE role = $parent.name)->? AS chained, $who->(knows WHERE out != $parent.id)->? AS fromBinding, id->(knows WHERE out != $parent.id)->?.{ name: name } AS destructured FROM ⟨person⟩ WHERE $who INSIDE id->(knows WHERE out != $parent.manager)->?;',
		);
		expect(Object.keys(statement.params)).toEqual(['who']);
	});

	it('takes a field path of a select’s row and nothing else', () => {
		const person = rowOf(Person);
		expect(() => untyped(parent)(person.name)).toThrow(
			'`parent` takes a field path of a select’s row, like `parent(row.name)`',
		);
		expect(() =>
			select(PersonTable).project((row) => ({
				x: outgoing(store, row.id, 'member_of', {
					where: (edge) => eq(edge.role, parent(edge.role as never)),
				}),
			})),
		).toThrow(
			"`parent(role)` reads a field of the edge `member_of` of a step; `parent` reads only a select's row",
		);
		expect(() => untyped(parent)(param('x', SurrealSchema.String))).toThrow(
			/takes a field path/,
		);
	});

	it('is refused outside a step’s where', () => {
		expect(() =>
			select(PersonTable).where((row) => eq(row.name, parent(row.name))),
		).toThrow(
			/`parent\(row\.name\)` reads the select's row from inside a step's `where`.*in a select of `person`, read `row\.name` itself/,
		);
	});

	it('is refused in a step’s where inside another step’s where', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: outgoing(store, row.id, 'knows', {
					where: (edge) =>
						inside(
							param('who', PersonId),
							outgoing(store, edge.out, 'knows', {
								where: (inner) => ne(inner.out, parent(row.id)),
							}),
						),
				}),
			})),
		).toThrow(
			"`parent(row.id)` is in a step inside a step's `where`; `parent` reads the select's row only from a step directly under it",
		);
	});

	it('is refused inside a destructuring, where SurrealDB 3.3 and 4.0 bind $parent differently', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: destructure(store, row.manager, (boss) => ({
					peers: outgoing(store, boss.id, 'knows', {
						where: (edge) => ne(edge.out, parent(row.id)),
					}),
				})),
			})),
		).toThrow(
			'`parent(row.id)` is refused in a destructuring callback: SurrealDB 3.3 binds `$parent` inside a destructuring to the outer row and 4.0 to the destructured record; project the step beside the destructured object',
		);
	});

	it('is refused under a recursive path, a bare RETURN and a block', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: collect(row.id, (node) =>
					outgoing(store, node, 'knows', {
						where: (edge) => ne(edge.out, parent(row.id)),
					}),
				),
			})),
		).toThrow(
			/`parent\(row\.id\)` is refused in the step a recursive path repeats: SurrealDB 3\.3 and 4\.0 bind `\$parent` differently under a recursive path/,
		);
		let step: unknown;
		select(PersonTable).project((row) => {
			step = outgoing(store, param('who', PersonId), 'knows', {
				where: (edge) => ne(edge.out, parent(row.id)),
			});
			return { x: row.id };
		});
		expect(() => evaluate(step as never)).toThrow(
			/`parent\(row\.id\)` is refused in a bare `RETURN`: SurrealDB 3\.3 and 4\.0 bind `\$parent` differently under a bare `RETURN`/,
		);
		expect(() =>
			block()
				.return(() => step as never)
				.build(),
		).toThrow(
			/`parent\(row\.id\)` is refused in a block: SurrealDB 3\.3 and 4\.0 bind `\$parent` differently in a block/,
		);
	});

	it('is refused in a subquery of another select, and in a write or a grouped select', () => {
		expect(() =>
			select(PersonTable).project((row) => ({
				x: select(TeamTable)
					.value(() =>
						outgoing(store, param('who', PersonId), 'knows', {
							where: (edge) => ne(edge.out, parent(row.id)),
						}),
					)
					.toExpr(),
			})),
		).toThrow(
			'`parent(row.id)` reads the row of a select of `person`, and this step is in a select of `team`; place the step directly under that select',
		);
		expect(() =>
			update(PersonTable, param('id', PersonId))
				.merge(param('changes', PersonMerge))
				.returningValue((row) =>
					outgoing(store, row.id, 'knows', {
						where: (edge) => ne(edge.out, parent(row.id as never)),
					}),
				)
				.build(),
		).toThrow(/reads only a select's row/);
		expect(() =>
			select(PersonTable)
				.groupBy((row) => ({ name: row.name }))
				.aggregate(() => ({
					x: outgoing(
						store,
						param('who', PersonId),
						'knows',
					) as never,
				}))
				.build(),
		).not.toThrow();
	});
});
