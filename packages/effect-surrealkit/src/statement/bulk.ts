import { Schema } from 'effect';

import {
	admit,
	assertReader,
	type Expr,
	operand,
	paramsOf,
	settle,
} from './expr.js';
import {
	type Codec,
	make,
	type Params,
	type Simplify,
	type Statement,
} from './statement.js';
import { type PureTable, readerOf } from './table.js';
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
		M extends 'ignore' ? never : 'Record not found',
		never
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
	table: PureTable,
	rows: unknown,
	options?: { readonly missing?: Missing },
): UpdateEach<PureTable, Params, Missing> {
	const data = rows as Expr;
	const missing = options?.missing ?? 'error';
	return {
		build: (name = `update each ${table.name}`) => {
			const params = paramsOf(data);
			if (Object.hasOwn(params, 'row')) {
				throw new RangeError(
					'UPDATE EACH reserves the binding name `row` for its loop variable',
				);
			}
			const what = `statement \`${name}\``;
			const context = admit({ own: undefined, what, parents: 'refuse' }, [
				data,
			]);
			assertReader(readerOf(table), settle(context, what), what);
			const guard =
				missing === 'error'
					? "IF !record::exists($row.id) { THROW 'Record not found'; }; "
					: '';
			return make({
				name,
				// FOR itself returns NONE. The block provides the empty list of a many-row write.
				sql: `{ FOR $row IN ${operand(data)} { ${guard}UPDATE $row.id MERGE $row.set RETURN NONE; }; [] };`,
				params,
				result: Schema.Tuple([]),
				throws: missing === 'error' ? ['Record not found'] : [],
				writes: true,
			});
		},
	};
}
