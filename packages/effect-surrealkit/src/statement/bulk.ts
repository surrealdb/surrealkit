import type { Expr } from './expr.js';
import type { Codec, Params, Simplify, Statement } from './statement.js';
import type { PureTable } from './table.js';
import type { FitsRows, NoResult, OperationOf } from './write.js';

/** One record id and the fields to merge into its row. */
type UpdateRow<T extends PureTable> = {
	readonly id: T['id']['Encoded'];
	readonly set: OperationOf<T, 'merge'>['Encoded'];
};

export type FitsUpdateRows<T extends PureTable, D extends Codec, V> =
	D['Encoded'] extends ReadonlyArray<unknown>
		? FitsRows<UpdateRow<T>, D, V, T['name'], 'UPDATE EACH'>
		: 'UPDATE EACH needs a list of rows';

type Missing = 'error' | 'ignore';

export interface UpdateEach<
	_T extends PureTable,
	P extends Params,
	M extends Missing = 'error',
> {
	readonly build: (
		name?: string,
	) => Statement<
		Simplify<P>,
		NoResult<'many'>,
		M extends 'ignore' ? never : 'Record not found'
	>;
}

/** Merges each row's fields into its record, throwing on a missing record by default. */
export function updateEach<
	T extends PureTable,
	D extends Codec,
	P extends Params,
>(
	table: T,
	rows: FitsUpdateRows<T, D, Expr<D, P>>,
	options: { readonly missing: 'ignore' },
): UpdateEach<T, P, 'ignore'>;
export function updateEach<
	T extends PureTable,
	D extends Codec,
	P extends Params,
>(
	table: T,
	rows: FitsUpdateRows<T, D, Expr<D, P>>,
	options?: { readonly missing?: Missing },
): UpdateEach<T, P>;
export function updateEach(
	_table: PureTable,
	_rows: unknown,
	_options?: { readonly missing?: Missing },
): never {
	throw new Error(
		'effect-surrealkit: updateEach is not implemented yet (lane L5)',
	);
}
