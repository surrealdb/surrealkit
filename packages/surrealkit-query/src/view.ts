/**
 * What every binding does between its `q` and the cache: resolve `q` (query
 * text or a `preload` payload) and its parameters to one request, and seed
 * the first render from a payload. Framework packages add only their own
 * reactivity on top.
 */

import {
	isPreloaded,
	preloadedParams,
	type QueryCache,
	type QueryState,
} from './cache.js';
import type { Preloaded } from './types.js';
import { decodeValue, queryKey } from './values.js';

/** A one-shot query, or a live one. */
export type QueryKind = 'query' | 'live';

/** A component's or hook's `q` and `params`, resolved. */
export interface ViewRequest {
	readonly text: string;
	readonly params: object | undefined;
	/** The payload `q` was, when it was one. */
	readonly payload: Preloaded | undefined;
	/** The request's identity: its text and parameters, by value. */
	readonly key: string;
}

/**
 * Resolves `q` and `params`. A payload gives the text, and its own
 * parameters unless `params` overrides them.
 */
export function resolveRequest(
	q: string | Preloaded,
	params: object | undefined,
): ViewRequest {
	if (isPreloaded(q)) {
		const bound = params ?? preloadedParams(q);
		return {
			text: q.text,
			params: bound,
			payload: q,
			key: queryKey(q.text, bound),
		};
	}
	return { text: q, params, payload: undefined, key: queryKey(q, params) };
}

/** Each payload's decoded result, so a payload is decoded once. */
const decoded = new WeakMap<Preloaded, unknown[]>();

function resultOf(payload: Preloaded): unknown[] {
	let result = decoded.get(payload);
	if (!result) {
		result = decodeValue<unknown[]>(payload.data);
		decoded.set(payload, result);
	}
	return result;
}

const browser = typeof document !== 'undefined';

/**
 * The state to show before the cache has a result: the payload's result,
 * when the request is still the payload's own query and parameters.
 *
 * In the browser this also seeds the cache, so a one-shot query does not run
 * again. On the server the shared cache is left alone: seeding it would show
 * one request's rows to another.
 */
export function seedState(
	kind: QueryKind,
	request: ViewRequest,
	cache: QueryCache,
): QueryState<unknown> | undefined {
	const { payload } = request;
	if (
		!payload ||
		queryKey(payload.text, preloadedParams(payload)) !== request.key
	) {
		return undefined;
	}
	const result = resultOf(payload);
	const data =
		kind === 'live' ? result[0] : result.length === 1 ? result[0] : result;
	if (browser) cache.hydrate(payload);
	return { status: 'success', data, error: undefined };
}

/** The state of a query that is not enabled: pending, with nothing subscribed. */
export function idleState(kind: QueryKind): QueryState<unknown> {
	return {
		status: 'pending',
		data: kind === 'live' ? [] : undefined,
		error: undefined,
	};
}

/** What a binding shows: the query's state, and whether its data is stale. */
export type ViewState = QueryState<unknown> & { readonly stale: boolean };

/**
 * Remembers the last result a view showed, so that when its request changes
 * the previous result stays on screen (marked `stale`) until the new one
 * lands. A request's own pending state (after a cache `reset()`) is never
 * filled from another, so one identity's rows never show to the next.
 */
export class ViewMemory {
	#last: { key: string; data: unknown } | undefined;
	#shown:
		| {
				key: string;
				state: QueryState<unknown>;
				seed: unknown;
				view: ViewState;
		  }
		| undefined;

	/**
	 * The state to show for `key`, given the cache's `state` and the `seed`
	 * from `seedState`. The same inputs give the same object, so a framework
	 * can compare snapshots by identity.
	 */
	show(
		key: string,
		state: QueryState<unknown>,
		seed: QueryState<unknown> | undefined,
	): ViewState {
		const shown = this.#shown;
		if (
			shown &&
			shown.key === key &&
			shown.state === state &&
			shown.seed === seed
		) {
			return shown.view;
		}
		const settled = state.status === 'pending' && seed ? seed : state;
		let view: ViewState;
		if (settled.status === 'success') {
			this.#last = { key, data: settled.data };
			view = { ...settled, stale: false };
		} else if (
			settled.status === 'pending' &&
			this.#last &&
			this.#last.key !== key
		) {
			view = {
				status: 'success',
				data: this.#last.data,
				error: undefined,
				stale: true,
			};
		} else {
			view = { ...settled, stale: false };
		}
		this.#shown = { key, state, seed, view };
		return view;
	}
}
