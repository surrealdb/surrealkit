/**
 * Applying live notifications to rows.
 *
 * Rows are matched by record id, compared by its SurrealQL text, so a
 * `RecordId` row id and a notification's `RecordId` match whichever instance
 * each one is. Every function returns a new array and never mutates its
 * input, and a row that did not change keeps its object identity, so keyed
 * `{#each}` blocks and React keys re-render only what changed.
 */

import { sameValue } from './values.js';

/** The notification shape the reconciler reads. */
export interface LiveMessageLike {
	readonly action: 'CREATE' | 'UPDATE' | 'DELETE' | 'KILLED';
	readonly recordId?: unknown;
	readonly value?: unknown;
}

/** A row the reconciler can key: an object with an `id`. */
type Keyed = { readonly id: unknown };

/** Whether `value` is an object carrying an `id`. */
export function hasId(value: unknown): value is Keyed {
	return (
		typeof value === 'object' &&
		value !== null &&
		'id' in value &&
		value.id != null
	);
}

function idText(id: unknown): string {
	return String(id);
}

/**
 * Applies one notification: `CREATE` and `UPDATE` replace the row with that
 * id or append it, `DELETE` removes it, `KILLED` changes nothing.
 */
export function reconcile<Row>(
	rows: readonly Row[],
	message: LiveMessageLike,
): Row[] {
	if (message.action === 'KILLED') return rows.slice();
	const id = idText(message.recordId);
	const next = rows.slice();
	const at = next.findIndex((row) => hasId(row) && idText(row.id) === id);
	if (message.action === 'DELETE') {
		if (at >= 0) next.splice(at, 1);
		return next;
	}
	const row = message.value as Row;
	if (at < 0) next.push(row);
	else if (!sameValue(next[at], row)) next[at] = row;
	return next;
}

/**
 * Returns `next`, with each row equal to a row of `previous` (by id when
 * rows have one, by position otherwise) replaced by that earlier object.
 * Returns `previous` itself when nothing changed.
 */
export function share<Row>(
	previous: readonly Row[],
	next: readonly Row[],
): Row[] {
	const byId = new Map<string, Row>();
	for (const row of previous) if (hasId(row)) byId.set(idText(row.id), row);
	let changed = previous.length !== next.length;
	const out = next.map((row, i) => {
		const before = hasId(row) ? byId.get(idText(row.id)) : previous[i];
		if (before !== undefined && sameValue(before, row)) {
			if (before !== previous[i]) changed = true;
			return before;
		}
		changed = true;
		return row;
	});
	return changed ? out : (previous as Row[]);
}
