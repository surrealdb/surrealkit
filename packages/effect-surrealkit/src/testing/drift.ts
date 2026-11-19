/**
 * Schema drift: what a store registry declares, compared with what a live
 * database defines.
 *
 * The statements trust the generated descriptors: an `ENFORCED` relation's
 * endpoints are typed as present, a lookup names a unique index, a search a
 * FULLTEXT or vector index. A database whose schema moved on from the one
 * the descriptors were generated from breaks those promises at run time,
 * so a test can ask for the differences first.
 *
 * It reads `INFO FOR DB STRUCTURE` and `INFO FOR TABLE … STRUCTURE`, which
 * SurrealDB 3.3 and 4.0 answer alike for what is compared here
 * (`test/conformance.test.ts` pins both): each table's `kind` (`NORMAL`,
 * `ANY`, which a table defined without `TYPE` is too, or `RELATION` with its
 * `in` and `out` tables, as the SDK's `Table` values and absent for any
 * table, and `enforced`), and each index's `cols` (field paths as SurrealQL spells
 * them, `tags.*`, `` `odd.name` ``) and `index` (empty for a standard index,
 * otherwise its definition, starting with its kind: `UNIQUE`, `COUNT`,
 * `FULLTEXT`, `HNSW`, `DISKANN`).
 */
import { Effect } from 'effect';

import { linkShape, shapeTables } from '../statement/graph.js';
import type { Codec } from '../statement/statement.js';
import { type PureTable, tableName } from '../statement/table.js';
import { TestDatabase } from './database.js';
import { TestingError } from './errors.js';

/** What the drift check reads of a generated table descriptor. */
export interface DeclaredTable {
	readonly name: string;
	/** `normal`, `any` or `relation`, as the emitter writes it. */
	readonly type?: string;
	readonly edge?: {
		readonly in: ReadonlyArray<unknown>;
		readonly out: ReadonlyArray<unknown>;
		readonly enforced: boolean;
	};
	readonly indexes?: {
		readonly [name: string]: {
			readonly type: string;
			readonly columns: ReadonlyArray<ReadonlyArray<string>>;
		};
	};
}

/** What the drift check reads of a generated store registry. */
export interface DeclaredStore {
	readonly tables: { readonly [name: string]: DeclaredTable };
}

/** One way the live database differs from the store's declarations. */
export interface SchemaDifference {
	readonly kind:
		| 'missing-table'
		| 'table-kind'
		| 'relation-enforced'
		| 'relation-endpoints'
		| 'missing-index'
		| 'index-kind'
		| 'index-fields';
	readonly table: string;
	readonly index?: string;
	/** What the descriptors declare, and what the database defines. */
	readonly declared: string;
	readonly live: string;
	readonly message: string;
}

interface LiveKind {
	readonly kind?: string;
	/** Over the SDK, `Table` values; over HTTP, names. */
	readonly in?: ReadonlyArray<unknown>;
	readonly out?: ReadonlyArray<unknown>;
	readonly enforced?: boolean;
}

/** A table name as `INFO` hands it back: a name, or the SDK's `Table`. */
const nameOf = (value: unknown): string =>
	typeof value === 'string'
		? value
		: typeof value === 'object' &&
				value !== null &&
				'name' in value &&
				typeof value.name === 'string'
			? value.name
			: String(value);

interface LiveTable {
	readonly name: string;
	readonly kind?: LiveKind;
}

interface LiveIndex {
	readonly name: string;
	readonly cols?: ReadonlyArray<string>;
	readonly index?: string;
}

const messageOf = (cause: unknown): string =>
	cause instanceof Error ? cause.message : String(cause);

/** The segments of a field path as SurrealQL spells it: `` `a.b`.c `` is `a.b`, `c`. */
const segmentsOf = (path: string): ReadonlyArray<string> => {
	const segments: Array<string> = [];
	let current = '';
	let index = 0;
	while (index < path.length) {
		const char = path[index] ?? '';
		const close = char === '`' ? '`' : char === '⟨' ? '⟩' : undefined;
		if (close !== undefined) {
			index += 1;
			while (index < path.length && path[index] !== close) {
				if (path[index] === '\\') index += 1;
				current += path[index] ?? '';
				index += 1;
			}
			index += 1;
		} else if (char === '.') {
			segments.push(current);
			current = '';
			index += 1;
		} else {
			current += char;
			index += 1;
		}
	}
	segments.push(current);
	return segments;
};

/** An index's kind from its live definition: `UNIQUE …` is `unique`, none is `standard`. */
const indexKind = (definition: string): string => {
	const word = /^\s*([A-Za-z]+)/u.exec(definition)?.[1];
	return word === undefined ? 'standard' : word.toLowerCase();
};

const columnsText = (columns: ReadonlyArray<ReadonlyArray<string>>): string =>
	columns.length === 0
		? '(none)'
		: columns.map((path) => path.join('.')).join(', ');

const endpoints = (codecs: ReadonlyArray<unknown>): string => {
	const tables = new Set<string>();
	for (const codec of codecs) {
		const shape = linkShape(codec as Codec);
		const names = shape === undefined ? 'any' : shapeTables(shape);
		if (names === 'any') return 'any table';
		for (const name of names) tables.add(name);
	}
	return [...tables].sort().join(' | ');
};

const liveEndpoints = (tables: ReadonlyArray<unknown> | undefined): string =>
	tables === undefined || tables.length === 0
		? 'any table'
		: tables.map(nameOf).sort().join(' | ');

/**
 * The query's last result, as `INFO` answers it.
 */
const info = <A>(sql: string): Effect.Effect<A, TestingError, TestDatabase> =>
	Effect.flatMap(Effect.service(TestDatabase), (database) =>
		Effect.tryPromise({
			try: async () => {
				const results = (await database.connection.query(
					sql,
				)) as ReadonlyArray<unknown>;
				return results[results.length - 1] as A;
			},
			catch: (cause) =>
				new TestingError({
					message: `\`${sql}\` failed: ${messageOf(cause)}`,
					cause,
				}),
		}),
	);

const compareTable = (
	declared: DeclaredTable,
	live: LiveTable,
	indexes: ReadonlyArray<LiveIndex>,
): ReadonlyArray<SchemaDifference> => {
	const table = declared.name;
	const differences: Array<SchemaDifference> = [];
	const liveKind = (live.kind?.kind ?? 'ANY').toLowerCase();
	if (declared.type !== undefined && declared.type !== liveKind) {
		differences.push({
			kind: 'table-kind',
			table,
			declared: declared.type,
			live: liveKind,
			message: `\`${table}\` is declared ${declared.type} and defined ${liveKind}`,
		});
	}
	if (declared.edge !== undefined && liveKind === 'relation') {
		const enforced = live.kind?.enforced === true;
		if (declared.edge.enforced !== enforced) {
			differences.push({
				kind: 'relation-enforced',
				table,
				declared: declared.edge.enforced ? 'ENFORCED' : 'not ENFORCED',
				live: enforced ? 'ENFORCED' : 'not ENFORCED',
				message: `\`${table}\` is declared ${declared.edge.enforced ? 'ENFORCED' : 'not ENFORCED'} and defined ${enforced ? 'ENFORCED' : 'not ENFORCED'}`,
			});
		}
		for (const side of ['in', 'out'] as const) {
			const want = endpoints(declared.edge[side]);
			const have = liveEndpoints(live.kind?.[side]);
			if (want !== have) {
				differences.push({
					kind: 'relation-endpoints',
					table,
					declared: `${side.toUpperCase()} ${want}`,
					live: `${side.toUpperCase()} ${have}`,
					message: `\`${table}\` is declared ${side.toUpperCase()} ${want} and defined ${side.toUpperCase()} ${have}`,
				});
			}
		}
	}
	const byName = new Map(indexes.map((index) => [index.name, index]));
	for (const [name, index] of Object.entries(declared.indexes ?? {})) {
		const found = byName.get(name);
		if (found === undefined) {
			differences.push({
				kind: 'missing-index',
				table,
				index: name,
				declared: `${index.type} on ${columnsText(index.columns)}`,
				live: '(none)',
				message: `\`${table}\` declares the ${index.type} index \`${name}\`, which the database does not define`,
			});
			continue;
		}
		const kind = indexKind(found.index ?? '');
		if (kind !== index.type) {
			differences.push({
				kind: 'index-kind',
				table,
				index: name,
				declared: index.type,
				live: kind,
				message: `the index \`${name}\` of \`${table}\` is declared ${index.type} and defined ${kind}`,
			});
		}
		const columns = (found.cols ?? []).map(segmentsOf);
		if (columnsText(columns) !== columnsText(index.columns)) {
			differences.push({
				kind: 'index-fields',
				table,
				index: name,
				declared: columnsText(index.columns),
				live: columnsText(columns),
				message: `the index \`${name}\` of \`${table}\` is declared on ${columnsText(index.columns)} and defined on ${columnsText(columns)}`,
			});
		}
	}
	return differences;
};

/**
 * The differences between what `store` declares and what the
 * {@link TestDatabase} in context defines, in the store's table order: a
 * declared table the database lacks, a table of another kind, a relation
 * whose `ENFORCED` flag or endpoint tables differ, and a declared index
 * that is missing, of another kind or over other fields. Definitions the
 * store does not declare are not differences. An empty list is a match.
 *
 * @example
 * const differences = yield* schemaDrift(store);
 * expect(differences).toEqual([]);
 */
export const schemaDrift = (
	store: DeclaredStore,
): Effect.Effect<ReadonlyArray<SchemaDifference>, TestingError, TestDatabase> =>
	Effect.gen(function* () {
		const db = yield* info<{ readonly tables?: ReadonlyArray<LiveTable> }>(
			'INFO FOR DB STRUCTURE;',
		);
		const tables = new Map(
			(db?.tables ?? []).map((table) => [table.name, table]),
		);
		const differences: Array<SchemaDifference> = [];
		for (const declared of Object.values(store.tables)) {
			const live = tables.get(declared.name);
			if (live === undefined) {
				differences.push({
					kind: 'missing-table',
					table: declared.name,
					declared: declared.type ?? 'table',
					live: '(none)',
					message: `\`${declared.name}\` is declared and not defined`,
				});
				continue;
			}
			const structure = yield* info<{
				readonly indexes?: ReadonlyArray<LiveIndex>;
			}>(
				`INFO FOR TABLE ${tableName(declared as unknown as PureTable)} STRUCTURE;`,
			);
			differences.push(
				...compareTable(declared, live, structure?.indexes ?? []),
			);
		}
		return differences;
	});
