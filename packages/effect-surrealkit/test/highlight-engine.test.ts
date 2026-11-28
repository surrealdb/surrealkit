/**
 * `search::highlight` and `search::offsets` against what the engine
 * returns, on whichever engine the run uses (see `engine.ts`): a field with
 * several matches, a list field with several elements, one element and
 * none, a field that is NONE or NULL, a field whose stored type the marked
 * text does not fit, and a row the term does not occur in (admitted through
 * another branch of the condition). Each result is decoded strictly by the
 * codec its statement derived and annotated with the type it declares.
 * SurrealDB 3.3 and 4.0 answer all of it alike.
 */
import { Effect, Schema } from 'effect';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { type Client, make as makeClient } from '../src/client/index.js';
import * as SurrealSchema from '../src/index.js';
import {
	eq,
	highlight,
	match,
	offsets,
	or,
	param,
	select,
} from '../src/statement/index.js';
import {
	type Engine,
	engineConfigured,
	engineSkipReason,
	startEngine,
	within,
} from './engine.js';

if (!engineConfigured) {
	console.warn(
		`[effect-surrealkit] highlight engine suite skipped: ${engineSkipReason}`,
	);
}

const NoteId = SurrealSchema.RecordIdFor('hl_note');
const NoteFields = {
	body: SurrealSchema.String,
	tags: Schema.Array(SurrealSchema.String),
	note: SurrealSchema.String,
	lines: Schema.Array(SurrealSchema.String),
	bio: Schema.NullOr(SurrealSchema.String),
	kind: Schema.Literals(['open fox', 'closed']),
	// As the emitter prints `ASSERT string::len($value) <= 9`.
	code: SurrealSchema.String.check(Schema.isMaxLength(9)),
} as const;
const Note = Schema.Struct({
	id: NoteId,
	body: NoteFields.body,
	tags: NoteFields.tags,
	note: Schema.optionalKey(NoteFields.note),
	lines: Schema.optionalKey(NoteFields.lines),
	bio: NoteFields.bio,
	kind: NoteFields.kind,
	code: NoteFields.code,
});
const NoteWrite = Schema.Struct({
	body: NoteFields.body,
	tags: NoteFields.tags,
	note: Schema.optionalKey(NoteFields.note),
	lines: Schema.optionalKey(NoteFields.lines),
	bio: NoteFields.bio,
	kind: NoteFields.kind,
	code: NoteFields.code,
});
const required = {
	select: 'required',
	create: 'required',
	merge: 'optional',
	content: 'required',
} as const;
const removable = {
	select: 'optional',
	create: 'optional',
	merge: 'removable',
	content: 'optional',
} as const;
const fulltext = <const F extends string>(field: F) =>
	({
		type: 'fulltext',
		columns: [[field]],
		fulltext: { analyzer: 'hl_plain', highlights: true },
	}) as const;
const NoteTable = SurrealSchema.table({
	name: 'hl_note',
	type: 'normal',
	schemafull: true,
	drop: false,
	id: NoteId,
	select: Note,
	visible: Note,
	create: NoteWrite,
	merge: NoteWrite,
	content: NoteWrite,
	fields: NoteFields,
	presence: {
		id: {
			select: 'required',
			create: 'excluded',
			merge: 'excluded',
			content: 'excluded',
		},
		body: required,
		tags: required,
		note: removable,
		lines: removable,
		bio: required,
		kind: required,
		code: required,
	},
	relations: { in: [], out: [] },
	permissions: { select: false, create: false, update: false, delete: false },
	indexes: {
		hl_body: fulltext('body'),
		hl_tags: fulltext('tags'),
		hl_note: fulltext('note'),
		hl_lines: fulltext('lines'),
		hl_bio: fulltext('bio'),
		hl_kind: fulltext('kind'),
		hl_code: fulltext('code'),
	},
});

/** `fox` holds the term in every field; `dog` and `bare` in none of them. */
const schema = `
	DEFINE ANALYZER hl_plain TOKENIZERS blank, class FILTERS lowercase, ascii, snowball(english);
	DEFINE TABLE hl_note SCHEMAFULL;
	DEFINE FIELD body ON hl_note TYPE string;
	DEFINE FIELD tags ON hl_note TYPE array<string>;
	DEFINE FIELD note ON hl_note TYPE option<string>;
	DEFINE FIELD lines ON hl_note TYPE option<array<string>>;
	DEFINE FIELD bio ON hl_note TYPE string | null;
	DEFINE FIELD kind ON hl_note TYPE 'open fox' | 'closed';
	DEFINE FIELD code ON hl_note TYPE string ASSERT string::len($value) <= 9;
	DEFINE INDEX hl_body ON hl_note FIELDS body FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_tags ON hl_note FIELDS tags FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_note ON hl_note FIELDS note FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_lines ON hl_note FIELDS lines FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_bio ON hl_note FIELDS bio FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_kind ON hl_note FIELDS kind FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	DEFINE INDEX hl_code ON hl_note FIELDS code FULLTEXT ANALYZER hl_plain BM25 HIGHLIGHTS;
	CREATE hl_note:fox CONTENT {
		body: 'the quick fox and another fox', tags: ['red fox', 'dog', 'fox fox'],
		note: 'a fox note', lines: ['one fox', 'two'], bio: 'a fox bio',
		kind: 'open fox', code: 'short fox'
	};
	CREATE hl_note:dog CONTENT {
		body: 'a quick dog', tags: ['dog'], lines: ['only'], bio: NULL,
		kind: 'closed', code: 'dog'
	};
	CREATE hl_note:bare CONTENT {
		body: 'héllo wörld', tags: [], note: 'plain', bio: 'plain',
		kind: 'closed', code: 'bare'
	};
	CREATE hl_note:one CONTENT {
		body: 'héllo wörld fox', tags: ['lone fox'], lines: ['lone fox'],
		bio: 'x', kind: 'closed', code: 'one'
	};
	CREATE hl_note:f1 CONTENT { body: 'filler', tags: ['x'], bio: 'x', kind: 'closed', code: 'f1' };
	CREATE hl_note:f2 CONTENT { body: 'filler', tags: ['y'], bio: 'y', kind: 'closed', code: 'f2' };
	CREATE hl_note:f3 CONTENT { body: 'filler', tags: ['z'], bio: 'z', kind: 'closed', code: 'f3' };
`;

type Spans =
	| { readonly [element: string]: ReadonlyArray<{ s: number; e: number }> }
	| undefined;

describe.skipIf(!engineConfigured)(
	'highlight and offsets on the engine',
	() => {
		let engine: Engine;
		let client: Client;

		const ok = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
			within(Effect.runPromise(effect), 20_000);

		beforeAll(async () => {
			engine = await startEngine();
			client = makeClient(engine.db);
			await engine.db.query(schema);
		});
		afterAll(async () => {
			await engine?.stop();
		});

		const terms = param('terms', SurrealSchema.String);
		/** Rows the match finds, and the rows `dog` and `bare` it does not. */
		const others = ['a quick dog', 'héllo wörld'] as const;

		it('a string field: every match of every term is marked and located, by character', async () => {
			const words = match(NoteTable, 'body', terms, { ref: 1 });
			const found = select(NoteTable)
				.where(() => words)
				.orderBy((row) => row.id)
				.project((row) => ({
					key: row.code,
					marked: highlight(words, '[', ']'),
					spans: offsets(words),
				}))
				.build();
			const rows: ReadonlyArray<{
				readonly key: string;
				readonly marked: string;
				readonly spans: Spans;
			}> = await ok(client.run(found, { terms: 'quick fox' }));
			expect(rows).toStrictEqual([
				{
					key: 'short fox',
					marked: 'the [quick] [fox] and another [fox]',
					spans: {
						0: [
							{ s: 4, e: 9 },
							{ s: 10, e: 13 },
							{ s: 26, e: 29 },
						],
					},
				},
			]);
			// Offsets count characters, not bytes: `é` and `ö` are one each.
			expect(await ok(client.run(found, { terms: 'fox' }))).toStrictEqual(
				[
					{
						key: 'short fox',
						marked: 'the quick [fox] and another [fox]',
						spans: {
							0: [
								{ s: 10, e: 13 },
								{ s: 26, e: 29 },
							],
						},
					},
					{
						key: 'one',
						marked: 'héllo wörld [fox]',
						spans: { 0: [{ s: 12, e: 15 }] },
					},
				],
			);
		});

		it('a row the term does not occur in is unmarked and has no offsets', async () => {
			const words = match(NoteTable, 'body', terms, { ref: 1 });
			const rows: ReadonlyArray<{
				readonly key: string;
				readonly marked: string;
				readonly spans: Spans;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where((row) =>
							or(
								words,
								eq(row.body, others[0]),
								eq(row.body, others[1]),
							),
						)
						.orderBy((row) => row.id)
						.project((row) => ({
							key: row.code,
							marked: highlight(words, '[', ']'),
							spans: offsets(words),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(
				rows.map(({ key, marked, spans }) => [key, marked, spans]),
			).toStrictEqual([
				['bare', 'héllo wörld', undefined],
				['dog', 'a quick dog', undefined],
				[
					'short fox',
					'the quick [fox] and another [fox]',
					{
						0: [
							{ s: 10, e: 13 },
							{ s: 26, e: 29 },
						],
					},
				],
				['one', 'héllo wörld [fox]', { 0: [{ s: 12, e: 15 }] }],
			]);
		});

		it('a list field is a list of its elements: several, one alone, and none', async () => {
			const words = match(NoteTable, 'tags', terms, { ref: 1 });
			const rows: ReadonlyArray<{
				readonly key: string;
				readonly marked: ReadonlyArray<string>;
				readonly spans: Spans;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where((row) =>
							or(
								words,
								eq(row.body, others[0]),
								eq(row.body, others[1]),
							),
						)
						.orderBy((row) => row.id)
						.project((row) => ({
							key: row.code,
							marked: highlight(words, '[', ']'),
							spans: offsets(words),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(
				rows.map(({ key, marked, spans }) => [key, marked, spans]),
			).toStrictEqual([
				// No element: SurrealDB answers NONE, read as the empty list.
				['bare', [], undefined],
				// One element the term does not occur in: SurrealDB answers the
				// element alone, read as a list of one.
				['dog', ['dog'], undefined],
				// Offsets are keyed by the index of each element with a match.
				[
					'short fox',
					['red [fox]', 'dog', '[fox] [fox]'],
					{
						0: [{ s: 4, e: 7 }],
						2: [
							{ s: 0, e: 3 },
							{ s: 4, e: 7 },
						],
					},
				],
				['one', ['lone [fox]'], { 0: [{ s: 5, e: 8 }] }],
			]);
		});

		it('an optional list field that is NONE is the empty list', async () => {
			const words = match(NoteTable, 'lines', terms, { ref: 1 });
			const rows: ReadonlyArray<{
				readonly key: string;
				readonly marked: ReadonlyArray<string>;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where((row) => or(words, eq(row.body, others[1])))
						.orderBy((row) => row.id)
						.project((row) => ({
							key: row.code,
							marked: highlight(words, '<', '>'),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(rows).toStrictEqual([
				{ key: 'bare', marked: [] },
				{ key: 'short fox', marked: ['one <fox>', 'two'] },
				{ key: 'one', marked: ['lone <fox>'] },
			]);
		});

		it('an optional field that is NONE, and a field that is NULL, have no text to mark', async () => {
			const noted = match(NoteTable, 'note', terms, { ref: 1 });
			const notes: ReadonlyArray<{
				readonly key: string;
				readonly marked: string | undefined;
				readonly spans: Spans;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where((row) => or(noted, eq(row.body, others[0])))
						.orderBy((row) => row.id)
						.project((row) => ({
							key: row.code,
							marked: highlight(noted, '[', ']'),
							spans: offsets(noted),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(notes).toStrictEqual([
				{ key: 'dog', marked: undefined, spans: undefined },
				{
					key: 'short fox',
					marked: 'a [fox] note',
					spans: { 0: [{ s: 2, e: 5 }] },
				},
			]);
			// A NULL field is answered as NONE, not as NULL.
			const bio = match(NoteTable, 'bio', terms, { ref: 1 });
			const bios: ReadonlyArray<{
				readonly key: string;
				readonly marked: string | undefined;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where((row) => or(bio, eq(row.body, others[0])))
						.orderBy((row) => row.id)
						.project((row) => ({
							key: row.code,
							marked: highlight(bio, '[', ']'),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(bios).toStrictEqual([
				{ key: 'dog', marked: undefined },
				{ key: 'short fox', marked: 'a [fox] bio' },
			]);
		});

		it('marked text is text, whatever the field stores: a literal and a bounded string', async () => {
			const kind = match(NoteTable, 'kind', terms, { ref: 1 });
			const code = match(NoteTable, 'code', terms, { ref: 2 });
			const rows: ReadonlyArray<{
				readonly kind: string;
				readonly code: string;
				readonly spans: Spans;
			}> = await ok(
				client.run(
					select(NoteTable)
						.where(() => or(kind, code))
						.project(() => ({
							// Not one of the field's literals any more.
							kind: highlight(kind, '[', ']'),
							// Longer than the nine characters the field holds.
							code: highlight(code, '<<', '>>'),
							spans: offsets(code),
						}))
						.build(),
					{ terms: 'fox' },
				),
			);
			expect(rows).toStrictEqual([
				{
					kind: 'open [fox]',
					code: 'short <<fox>>',
					spans: { 0: [{ s: 6, e: 9 }] },
				},
			]);
		});
	},
);
