import type { ParamsOfAll } from './expr.js';
import type { CheckedProjection, ProjectedCodec } from './row.js';
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

/** Starts a live query over the table or its readAs view. */
export const live = <T extends PureTable>(
	_table: T,
): Live<T, NoParams, StoredRow<T>> => {
	throw new Error(
		'effect-surrealkit: live builder is not implemented yet (lane L7)',
	);
};
