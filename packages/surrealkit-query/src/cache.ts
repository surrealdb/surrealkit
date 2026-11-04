/**
 * The reactive core: a cache of query results shared by every component on
 * one connection.
 *
 * An entry is one query text bound to one set of parameters, either one-shot
 * or live. Observers reference-count it:
 *
 * - The first observer starts it. A one-shot entry runs its query; a live
 *   entry sends `LIVE SELECT …; SELECT …` as one request, so the live query
 *   is registered before the snapshot is read and no change falls between
 *   them, and then applies every notification on top of the snapshot.
 * - Observers of the same text and parameters share the entry, so N
 *   components on one live query hold one `LIVE SELECT`.
 * - When the last observer leaves, the entry stops at the end of the current
 *   microtask (so an unsubscribe immediately followed by a subscribe, as in
 *   React's strict mode, keeps the subscription) and its live query is
 *   killed. Its data stays cached for `gcTime`, then is dropped.
 * - New parameters are a new entry, so changing them stops the old live
 *   query and starts the new one.
 * - When the connection is re-established (the SDK's `connected` event),
 *   every started entry runs again: live entries re-register and re-read
 *   their snapshot, since the engine forgot their live queries.
 *
 * A live query that resolves after its entry stopped or restarted is killed
 * as soon as it resolves. A `KILL` the engine refuses (the live query is
 * already gone) is ignored.
 */

import type { Uuid } from 'surrealdb';

import { SurrealQueryError } from './error.js';
import { type LivePlan, planLive } from './plan.js';
import { hasId, type LiveMessageLike, reconcile, share } from './reconcile.js';
import type { Preloaded } from './types.js';
import { cleanParams, decodeValue, encodeValue, queryKey } from './values.js';

/** A live subscription, as the SDK's `LiveSubscription` provides it. */
export interface LiveSubscriptionLike {
	subscribe(handler: (message: LiveMessageLike) => void): () => void;
	kill(): Promise<void>;
}

/**
 * The client the cache runs queries on. A connected `Surreal` from the
 * `surrealdb` SDK satisfies it.
 */
export interface SurrealLike {
	query(
		text: string,
		bindings?: Record<string, unknown>,
	): PromiseLike<unknown[]>;
	liveOf(id: Uuid): PromiseLike<LiveSubscriptionLike>;
	subscribe?(event: 'connected', listener: () => void): () => void;
}

export type QueryStatus = 'pending' | 'success' | 'error';

/** An entry's state. `status` narrows `data`. */
export type QueryState<T> =
	| { status: 'pending'; data: T | undefined; error: undefined }
	| { status: 'success'; data: T; error: undefined }
	| { status: 'error'; data: T | undefined; error: SurrealQueryError };

/** One entry, as an observer sees it. */
export interface Observable<T> {
	/** The entry's identity: its text and parameters. */
	readonly key: string;
	get(): QueryState<T>;
	/** Observes the entry, starting it if it is idle. Returns the unsubscribe. */
	subscribe(listener: () => void): () => void;
	/** Runs the query (or a live query's snapshot) again. */
	refetch(): Promise<void>;
}

export interface QueryCacheOptions {
	/**
	 * How long an entry's data is kept after its last observer leaves, in ms.
	 * Default 5 minutes. `Infinity` keeps it until {@link QueryCache.clear}.
	 */
	gcTime?: number;
}

/** What the cache holds, for tests and debugging. */
export interface QueryCacheStats {
	/** Entries in the cache, observed or not. */
	readonly entries: number;
	/** Entries with at least one observer. */
	readonly observed: number;
	/** Live queries registered on the engine and not yet killed. */
	readonly live: number;
}

type Kind = 'query' | 'live';

interface Entry {
	readonly key: string;
	readonly kind: Kind;
	readonly text: string;
	readonly params: Record<string, unknown> | undefined;
	readonly plan: LivePlan | undefined;
	state: QueryState<unknown>;
	readonly listeners: Set<() => void>;
	refs: number;
	/** Whether the entry is started: it has observers, or is stopping. */
	running: boolean;
	/** Bumped on every start and stop; work from an older run is discarded. */
	run: number;
	subscription: LiveSubscriptionLike | undefined;
	unsubscribe: (() => void) | undefined;
	/** The rows carry ids, so notifications can be applied by id. */
	keyed: boolean;
	/** A one-shot entry holds a server payload, so its first start skips the fetch. */
	seeded: boolean;
	refreshing: Promise<void> | undefined;
	dirty: boolean;
	stopQueued: boolean;
	gcTimer: ReturnType<typeof setTimeout> | undefined;
}

const DEFAULT_GC_TIME = 5 * 60_000;

const PENDING_QUERY: QueryState<unknown> = {
	status: 'pending',
	data: undefined,
	error: undefined,
};

export class QueryCache {
	readonly #db: SurrealLike;
	readonly #gcTime: number;
	readonly #entries = new Map<string, Entry>();
	readonly #offConnected: (() => void) | undefined;
	#live = 0;

	constructor(db: SurrealLike, options: QueryCacheOptions = {}) {
		this.#db = db;
		this.#gcTime = options.gcTime ?? DEFAULT_GC_TIME;
		this.#offConnected = db.subscribe?.('connected', () =>
			this.#reconnected(),
		);
	}

	/** The client this cache runs queries on. */
	get db(): SurrealLike {
		return this.#db;
	}

	/** Observes a one-shot query. Its data is the query's result. */
	observe<T>(text: string, params?: object): Observable<T> {
		return this.#observable(
			this.#ensure('query', text, params),
		) as Observable<T>;
	}

	/**
	 * Observes a live query: `text` is a plain SELECT, made live as
	 * {@link planLive} describes. Its data is the current rows, `[]` while
	 * pending.
	 */
	observeLive<Row>(text: string, params?: object): Observable<Row[]> {
		return this.#observable(
			this.#ensure('live', text, params),
		) as Observable<Row[]>;
	}

	/** Runs a one-shot query and caches its result. */
	async fetch<T>(text: string, params?: object): Promise<T> {
		const entry = this.#ensure('query', text, params);
		await this.#load(entry, entry.run);
		if (entry.state.status === 'error') throw entry.state.error;
		return entry.state.data as T;
	}

	/**
	 * Seeds the cache from a server payload. Both a one-shot and a live
	 * observer of the payload's text and parameters render its data at once;
	 * a live observer still opens its live query and re-reads the snapshot
	 * when it starts, so nothing that changed in between is missed.
	 *
	 * Hydrate in the browser only. A server's cache is shared by every
	 * request, so seeding it would show one request's rows to another.
	 */
	hydrate(payload: Preloaded): void {
		const params = payload.params
			? decodeValue<object>(payload.params)
			: undefined;
		const result = decodeValue<unknown[]>(payload.data);
		for (const kind of ['query', 'live'] as const) {
			const entry = this.#ensure(kind, payload.text, params);
			if (entry.state.status === 'success') continue;
			const data = kind === 'live' ? result[0] : unwrap(result);
			if (kind === 'live' && !Array.isArray(data)) continue;
			// Observers hear of the seed on a microtask, so hydrating from inside
			// a framework's derivation never writes state during it.
			entry.state = { status: 'success', data, error: undefined };
			queueMicrotask(() => this.#set(entry, entry.state));
			entry.seeded = kind === 'query' && !entry.running;
			if (entry.refs === 0) this.#scheduleGc(entry);
		}
	}

	/**
	 * Marks queries stale: observed entries run again, unobserved ones are
	 * dropped. With no `params`, every binding of `text` matches; with no
	 * `text`, everything does.
	 */
	async invalidate(text?: string, params?: object): Promise<void> {
		const key = text === undefined ? undefined : queryKey(text, params);
		const pending: Promise<void>[] = [];
		for (const entry of [...this.#entries.values()]) {
			const matches =
				text === undefined ||
				(params === undefined
					? entry.text === text
					: entry.key === `${entry.kind}:${key}`);
			if (!matches) continue;
			if (entry.refs > 0) pending.push(this.#refresh(entry));
			else this.#drop(entry);
		}
		await Promise.all(pending);
	}

	/**
	 * Forgets everything the current identity saw, and restarts what is still
	 * observed. Call it after `signin`, `signup`, `authenticate` or
	 * `invalidate` on the client: a cache key holds no `$auth`, so without
	 * this one identity's rows would render for the next.
	 */
	async reset(): Promise<void> {
		const restarted: Promise<void>[] = [];
		for (const entry of [...this.#entries.values()]) {
			if (entry.refs === 0) {
				this.#drop(entry);
				continue;
			}
			this.#kill(entry);
			this.#set(entry, pendingState(entry.kind));
			restarted.push(this.#start(entry));
		}
		await Promise.all(restarted);
	}

	/** Kills every live query and empties the cache. */
	clear(): void {
		for (const entry of [...this.#entries.values()]) this.#drop(entry);
	}

	/** Clears the cache and stops listening for reconnects. */
	dispose(): void {
		this.clear();
		this.#offConnected?.();
	}

	stats(): QueryCacheStats {
		let observed = 0;
		for (const entry of this.#entries.values())
			if (entry.refs > 0) observed += 1;
		return { entries: this.#entries.size, observed, live: this.#live };
	}

	// --- entries ------------------------------------------------------------

	#ensure(kind: Kind, text: string, params: object | undefined): Entry {
		const clean = cleanParams(params);
		const key = `${kind}:${queryKey(text, clean)}`;
		let entry = this.#entries.get(key);
		if (!entry) {
			entry = {
				key,
				kind,
				text,
				params: clean,
				plan: kind === 'live' ? planLive(text) : undefined,
				state: pendingState(kind),
				listeners: new Set(),
				refs: 0,
				running: false,
				run: 0,
				subscription: undefined,
				unsubscribe: undefined,
				keyed: false,
				seeded: false,
				refreshing: undefined,
				dirty: false,
				stopQueued: false,
				gcTimer: undefined,
			};
			this.#entries.set(key, entry);
		}
		return entry;
	}

	#observable(entry: Entry): Observable<unknown> {
		return {
			key: entry.key,
			get: () => entry.state,
			refetch: () => this.#refresh(entry),
			subscribe: (listener) => {
				entry.listeners.add(listener);
				entry.refs += 1;
				this.#cancelGc(entry);
				if (!entry.running) void this.#start(entry);
				let active = true;
				return () => {
					if (!active) return;
					active = false;
					entry.listeners.delete(listener);
					entry.refs -= 1;
					if (entry.refs === 0) this.#queueStop(entry);
				};
			},
		};
	}

	#start(entry: Entry): Promise<void> {
		entry.running = true;
		entry.run += 1;
		// An entry evicted while stopped is observed again: put it back.
		this.#entries.set(entry.key, entry);
		if (entry.kind === 'live') return this.#open(entry, entry.run);
		if (entry.seeded) {
			entry.seeded = false;
			return Promise.resolve();
		}
		return this.#load(entry, entry.run);
	}

	#queueStop(entry: Entry): void {
		if (entry.stopQueued) return;
		entry.stopQueued = true;
		queueMicrotask(() => {
			entry.stopQueued = false;
			if (entry.refs === 0 && entry.running) this.#stop(entry);
		});
	}

	#stop(entry: Entry): void {
		entry.running = false;
		entry.run += 1;
		this.#kill(entry);
		this.#scheduleGc(entry);
	}

	#drop(entry: Entry): void {
		entry.running = false;
		entry.run += 1;
		this.#kill(entry);
		this.#cancelGc(entry);
		this.#entries.delete(entry.key);
	}

	#scheduleGc(entry: Entry): void {
		this.#cancelGc(entry);
		if (this.#gcTime === Number.POSITIVE_INFINITY) return;
		const timer = setTimeout(() => {
			entry.gcTimer = undefined;
			if (entry.refs === 0 && !entry.running)
				this.#entries.delete(entry.key);
		}, this.#gcTime);
		// A cache eviction never holds a Node process open.
		(timer as { unref?: () => void }).unref?.();
		entry.gcTimer = timer;
	}

	#cancelGc(entry: Entry): void {
		if (entry.gcTimer !== undefined) clearTimeout(entry.gcTimer);
		entry.gcTimer = undefined;
	}

	#reconnected(): void {
		for (const entry of this.#entries.values()) {
			if (!entry.running) continue;
			// The engine forgot this connection's live queries; drop the handle
			// without a KILL, which would only be refused.
			this.#release(entry);
			void this.#start(entry);
		}
	}

	// --- one-shot -----------------------------------------------------------

	async #load(entry: Entry, run: number): Promise<void> {
		try {
			const result = await this.#db.query(entry.text, entry.params);
			if (run !== entry.run) return;
			this.#set(entry, {
				status: 'success',
				data: unwrap(result),
				error: undefined,
			});
		} catch (cause) {
			if (run === entry.run) this.#fail(entry, cause, entry.text);
		}
	}

	// --- live ---------------------------------------------------------------

	async #open(entry: Entry, run: number): Promise<void> {
		const plan = entry.plan as LivePlan;
		if (plan.mode === 'unsupported') {
			this.#set(entry, {
				status: 'error',
				data: entry.state.data,
				error: new SurrealQueryError(
					'unsupported',
					plan.reason,
					entry.text,
					entry.params,
				),
			});
			return;
		}
		// The registered live query's id, until a subscription owns it.
		let id: unknown;
		try {
			const result = await this.#db.query(
				`${plan.liveText};\n${plan.snapshot};`,
				entry.params,
			);
			id = result[0];
			this.#live += 1;
			const rows = result[1];
			if (run !== entry.run) return;
			if (!Array.isArray(rows))
				throw new Error('a live query must select rows');
			const subscription = await this.#db.liveOf(id as Uuid);
			id = undefined;
			if (run !== entry.run) return this.#killHandle(subscription);
			entry.subscription = subscription;
			entry.keyed = plan.mode === 'keyed' && rows.every(hasId);
			this.#setRows(entry, rows);
			// Notifications that arrived since the LIVE statement ran are
			// replayed by the SDK, in order, as soon as this subscribes.
			entry.unsubscribe = subscription.subscribe((message) =>
				this.#notify(entry, run, message),
			);
		} catch (cause) {
			if (run === entry.run) this.#fail(entry, cause, plan.liveText);
		} finally {
			if (id !== undefined) this.#killId(id);
		}
	}

	#notify(entry: Entry, run: number, message: LiveMessageLike): void {
		if (run !== entry.run) return;
		if (message.action === 'KILLED') {
			this.#release(entry);
			this.#set(entry, {
				status: 'error',
				data: entry.state.data,
				error: new SurrealQueryError(
					'killed',
					'the engine killed this live query',
					entry.text,
					entry.params,
				),
			});
			return;
		}
		const applies =
			entry.keyed &&
			(message.action === 'DELETE' || hasId(message.value));
		if (!applies) {
			entry.keyed = false;
			void this.#refresh(entry);
			return;
		}
		const rows = (entry.state.data ?? []) as unknown[];
		this.#set(entry, {
			status: 'success',
			data: reconcile(rows, message),
			error: undefined,
		});
	}

	/**
	 * Re-reads an entry: a one-shot entry runs its query, a live entry its
	 * snapshot. A live entry re-reads at most once at a time, and once more
	 * after that if a notification arrived meanwhile.
	 */
	#refresh(entry: Entry): Promise<void> {
		if (entry.kind === 'query') return this.#load(entry, entry.run);
		if (!entry.subscription) {
			// Opening already reads the snapshot; only a failed entry restarts.
			const failed = entry.running && entry.state.status === 'error';
			return failed ? this.#start(entry) : Promise.resolve();
		}
		if (entry.refreshing) {
			entry.dirty = true;
			return entry.refreshing;
		}
		const run = entry.run;
		const plan = entry.plan as Extract<LivePlan, { snapshot: string }>;
		entry.refreshing = (async () => {
			try {
				do {
					entry.dirty = false;
					try {
						const [rows] = await this.#db.query(
							plan.snapshot,
							entry.params,
						);
						if (run !== entry.run) return;
						this.#setRows(entry, Array.isArray(rows) ? rows : []);
					} catch (cause) {
						if (run !== entry.run) return;
						this.#fail(entry, cause, plan.snapshot);
					}
				} while (entry.dirty && run === entry.run);
			} finally {
				entry.refreshing = undefined;
			}
		})();
		return entry.refreshing;
	}

	#setRows(entry: Entry, rows: unknown[]): void {
		const previous = entry.state.data;
		this.#set(entry, {
			status: 'success',
			data: Array.isArray(previous) ? share(previous, rows) : rows,
			error: undefined,
		});
	}

	/** Detaches from the entry's live query without killing it. */
	#release(entry: Entry): void {
		entry.unsubscribe?.();
		entry.unsubscribe = undefined;
		if (entry.subscription) this.#live -= 1;
		entry.subscription = undefined;
		entry.refreshing = undefined;
		entry.dirty = false;
	}

	#kill(entry: Entry): void {
		const subscription = entry.subscription;
		this.#release(entry);
		if (subscription) this.#killHandle(subscription, false);
	}

	/** Kills a live query no subscription owns. */
	#killId(id: unknown): void {
		this.#live -= 1;
		Promise.resolve(this.#db.liveOf(id as Uuid)).then(
			(subscription) => subscription.kill().catch(ignore),
			ignore,
		);
	}

	#killHandle(subscription: LiveSubscriptionLike, counted = true): void {
		if (counted) this.#live -= 1;
		subscription.kill().catch(ignore);
	}

	#fail(entry: Entry, cause: unknown, text: string): void {
		this.#set(entry, {
			status: 'error',
			data: entry.state.data,
			error: SurrealQueryError.from(cause, text, entry.params),
		});
	}

	#set(entry: Entry, state: QueryState<unknown>): void {
		entry.state = state;
		for (const listener of [...entry.listeners]) listener();
	}
}

function pendingState(kind: Kind): QueryState<unknown> {
	// A live entry is a row stream, so it is `[]` until its snapshot arrives.
	return kind === 'live'
		? { status: 'pending', data: [], error: undefined }
		: PENDING_QUERY;
}

/** One statement resolves to its own result, several to the whole tuple. */
function unwrap(result: readonly unknown[]): unknown {
	return result.length === 1 ? result[0] : result;
}

function ignore(): void {}

const caches = new WeakMap<object, QueryCache>();

/**
 * The cache for a client. There is one per client, which is what lets every
 * component on a connection share entries and live queries.
 */
export function cacheFor(db: SurrealLike): QueryCache {
	let cache = caches.get(db);
	if (!cache) {
		cache = new QueryCache(db);
		caches.set(db, cache);
	}
	return cache;
}

/**
 * Runs a query on the server and returns a payload that seeds the client's
 * cache (see {@link QueryCache.hydrate}). Use the same text with a
 * one-shot or a live observer: the payload carries the whole result.
 */
export async function preloadQuery<Q extends string>(
	db: Pick<SurrealLike, 'query'>,
	text: Q,
	params?: object,
): Promise<Preloaded<Q>> {
	const clean = cleanParams(params);
	const result = await db.query(text, clean);
	return {
		surrealkit: 1,
		text,
		params: clean ? encodeValue(clean) : undefined,
		data: encodeValue(result),
	};
}

/** Whether `value` is a {@link Preloaded} payload. */
export function isPreloaded(value: unknown): value is Preloaded {
	return (
		typeof value === 'object' &&
		value !== null &&
		(value as { surrealkit?: unknown }).surrealkit === 1 &&
		typeof (value as { text?: unknown }).text === 'string'
	);
}

/** The parameters a payload was fetched with. */
export function preloadedParams(
	payload: Preloaded,
): Record<string, unknown> | undefined {
	return payload.params ? decodeValue(payload.params) : undefined;
}
