/**
 * Search references: what the search expressions of one `SELECT` define and
 * read, checked when the statement (or subquery) is built.
 *
 * A full-text match defines a reference number (`body @1@ $terms`) that
 * `search::score(1)`, `search::highlight(…, 1)` and `search::offsets(1)`
 * read; `vector::distance::knn()` reads the one nearest-neighbour search of
 * the statement. SurrealDB answers a reference no match defines with some
 * other match's score (4.0) or 0 (3.3), and a distance with no search with
 * an error, so both are refused before anything is sent.
 *
 * Every combinator already merges the bindings of its operands, so the
 * references travel with the bindings: a non-enumerable property of the
 * bindings object, which `Object.keys`, `Object.entries` and spreading never
 * see, so it is never mistaken for a binding.
 *
 * @internal
 */
const key: unique symbol = Symbol.for('effect-surrealkit/SearchRefs');

/** @internal */
export interface SearchRefs {
	/** Each reference number a match defines, with the field it matches. */
	readonly matches: ReadonlyMap<number, string>;
	/** The reference numbers score, highlight and offsets read. */
	readonly reads: ReadonlySet<number>;
	/** The text of each nearest-neighbour search. */
	readonly knn: ReadonlySet<string>;
	/** Whether `vector::distance::knn()` is read. */
	readonly distance: boolean;
}

const empty: SearchRefs = {
	matches: new Map(),
	reads: new Set(),
	knn: new Set(),
	distance: false,
};

/** The search references carried by a bindings object, if any. @internal */
export const refsOf = (params: object): SearchRefs | undefined =>
	(params as { readonly [key]?: SearchRefs })[key];

const define = (params: object, refs: SearchRefs): void => {
	Object.defineProperty(params, key, {
		value: refs,
		enumerable: false,
		configurable: true,
	});
};

/**
 * `params` (a fresh object, which this changes) carrying `refs` as well as
 * the references it already carries.
 *
 * @internal
 */
export const withRefs = <P extends object>(
	params: P,
	refs: Partial<SearchRefs> | undefined,
): P => {
	if (refs === undefined) return params;
	const extra = {};
	define(extra, { ...empty, ...refs });
	const joined = joinRefs([params, extra]);
	if (joined !== undefined) define(params, joined);
	return params;
};

/**
 * The references of several operands together. A reference number defined
 * by matches on two different fields, or two different nearest-neighbour
 * searches, cannot be one `SELECT`'s: refused here, when the expression
 * combining them is built.
 *
 * @internal
 */
export const joinRefs = (
	sets: ReadonlyArray<object>,
): SearchRefs | undefined => {
	let joined: SearchRefs | undefined;
	for (const set of sets) {
		const refs = refsOf(set);
		if (refs === undefined) continue;
		if (joined === undefined) {
			joined = refs;
			continue;
		}
		const matches = new Map(joined.matches);
		for (const [ref, field] of refs.matches) {
			const existing = matches.get(ref);
			if (existing !== undefined && existing !== field) {
				throw new RangeError(
					`match reference ${ref} is defined on both \`${existing}\` and \`${field}\`; give each match its own reference number`,
				);
			}
			matches.set(ref, field);
		}
		const knn = new Set([...joined.knn, ...refs.knn]);
		if (knn.size > 1) {
			throw new RangeError(
				`a SELECT takes one nearest-neighbour search, and this one has ${knn.size}: ${[...knn].map((text) => `\`${text}\``).join(', ')} (SurrealDB 4 refuses a second <|k, …|> in a WHERE clause)`,
			);
		}
		joined = {
			matches,
			reads: new Set([...joined.reads, ...refs.reads]),
			knn,
			distance: joined.distance || refs.distance,
		};
	}
	return joined;
};

/**
 * Refuses a nearest-neighbour search under `OR` or `NOT`: SurrealDB 3.3 and
 * 4.0 accept one only at the top level of a `WHERE`, joined with `AND`.
 *
 * @internal
 */
export const assertNotNested = (
	sets: ReadonlyArray<object>,
	operator: 'OR' | 'NOT',
): void => {
	for (const set of sets) {
		const text = [...(refsOf(set)?.knn ?? [])][0];
		if (text !== undefined) {
			throw new RangeError(
				`the nearest-neighbour search \`${text}\` is under ${operator}; SurrealDB takes one only at the top level of WHERE, joined with AND`,
			);
		}
	}
};

/**
 * Refuses a statement or subquery that reads a reference no match of its
 * own defines, or the distance of a nearest-neighbour search it does not
 * make.
 *
 * @internal
 */
export const assertResolved = (params: object, what: string): void => {
	const refs = refsOf(params);
	if (refs === undefined) return;
	for (const ref of [...refs.reads].sort((a, b) => a - b)) {
		if (!refs.matches.has(ref)) {
			throw new RangeError(
				`${what} reads the score of match reference ${ref}, which no match in it defines${
					refs.matches.size === 0
						? ''
						: ` (it defines ${[...refs.matches.keys()].join(', ')})`
				}`,
			);
		}
	}
	if (refs.distance && refs.knn.size === 0) {
		throw new RangeError(
			`${what} reads the nearest-neighbour distance (\`vector::distance::knn()\`) but makes no nearest-neighbour search`,
		);
	}
};

/** `params` without search references: a subquery's stay inside it. @internal */
export const withoutRefs = <P extends object>(params: P): P =>
	refsOf(params) === undefined ? params : ({ ...params } as P);
