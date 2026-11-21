import type { ParamsOfAll } from './expr.js';
import type { CheckedProjection, ProjectedCodec } from './row.js';
import { type Select, select } from './select.js';
import type { Codec, NoParams, Params, Simplify } from './statement.js';
import type { Condition, PureTable, RowExprs, StoredRow } from './table.js';

export const LiveStatementTypeId = '~effect-surrealkit/LiveStatement';

export interface LiveStatement<
	P extends Params = Params,
	Row extends Codec = Codec,
	Id extends Codec = Codec,
> {
	readonly [LiveStatementTypeId]: typeof LiveStatementTypeId;
	readonly name: string;
	/** Static LIVE SELECT text. */
	readonly sql: string;
	readonly params: P;
	readonly row: Row;
	readonly id: Id;
}

export interface Live<
	T extends PureTable,
	P extends Params,
	Row extends Codec,
> {
	readonly where: <P2 extends Params>(
		predicate: (row: RowExprs<T>) => Condition<P2>,
	) => Live<T, P & P2, Row>;

	readonly project: <const F>(
		fields: (row: RowExprs<T>) => CheckedProjection<F>,
	) => Live<T, P & ParamsOfAll<F[keyof F]>, ProjectedCodec<F>>;

	readonly build: (name: string) => LiveStatement<Simplify<P>, Row, T['id']>;
}

const builder = <T extends PureTable, P extends Params, Row extends Codec>(
	table: T,
	selection: Select<T, P, Row, false>,
): Live<T, P, Row> => ({
	where: (predicate) => builder(table, selection.where(predicate)),
	project: (fields) => builder(table, selection.project(fields)),
	build: (name) => {
		const statement = selection.build(name);
		return {
			[LiveStatementTypeId]: LiveStatementTypeId,
			name: statement.name,
			sql: `LIVE ${statement.sql}`,
			params: statement.params,
			row: statement.result.value,
			id: table.id,
		};
	},
});

/** Starts a live query over the table or its readAs view. */
export const live = <T extends PureTable>(
	table: T,
): Live<T, NoParams, StoredRow<T>> => builder(table, select(table));
