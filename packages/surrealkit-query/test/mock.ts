/**
 * A stand-in for a connected `Surreal`, shared by the core and binding tests.
 *
 * It does not run SurrealQL. A SELECT resolves through `select(text, params)`,
 * which a test sets. `LIVE …` registers a live query and resolves to its id;
 * a request that holds several statements resolves each one. Notifications
 * for a live query are buffered until it is subscribed to, as the SDK does,
 * and `reconnect()` forgets every live query, as the engine does.
 */

import type {
	LiveMessageLike,
	LiveSubscriptionLike,
	SurrealLike,
} from '../src/index.js';

export interface Live {
	readonly id: string;
	readonly text: string;
	readonly params: Record<string, unknown> | undefined;
}

export class MockDb implements SurrealLike {
	/** Resolves a SELECT. */
	select: (
		text: string,
		params: Record<string, unknown> | undefined,
	) => unknown = () => [];
	/** Every statement run, in order. */
	readonly statements: string[] = [];
	/** Live queries registered and not killed. */
	readonly open = new Map<string, Live>();
	/** Ids killed, in order. */
	readonly killed: string[] = [];
	#next = 0;
	#handlers = new Map<string, (message: LiveMessageLike) => void>();
	#buffered = new Map<string, LiveMessageLike[]>();
	#connected = new Set<() => void>();
	#held: (() => void)[] | undefined;

	/** Holds every query until {@link release}. */
	hold(): void {
		this.#held ??= [];
	}

	/** Lets held queries resolve. */
	release(): void {
		const held = this.#held ?? [];
		this.#held = undefined;
		for (const go of held) go();
	}

	async query(
		text: string,
		params?: Record<string, unknown>,
	): Promise<unknown[]> {
		if (this.#held) await new Promise<void>((go) => this.#held?.push(go));
		const results: unknown[] = [];
		for (const statement of split(text)) {
			this.statements.push(statement);
			if (/^LIVE\s/i.test(statement)) {
				const id = `live-${++this.#next}`;
				this.open.set(id, { id, text: statement, params });
				results.push(id);
			} else {
				results.push(this.select(statement, params));
			}
		}
		return results;
	}

	async liveOf(id: unknown): Promise<LiveSubscriptionLike> {
		const key = String(id);
		return {
			subscribe: (handler) => {
				this.#handlers.set(key, handler);
				const buffered = this.#buffered.get(key) ?? [];
				this.#buffered.delete(key);
				for (const message of buffered) handler(message);
				return () => this.#handlers.delete(key);
			},
			kill: async () => {
				this.#handlers.delete(key);
				if (!this.open.delete(key)) {
					throw new Error(
						`Cannot execute KILL statement using id: ${key}`,
					);
				}
				this.killed.push(key);
			},
		};
	}

	subscribe(_event: 'connected', listener: () => void): () => void {
		this.#connected.add(listener);
		return () => this.#connected.delete(listener);
	}

	/** Sends a notification to every open live query whose text matches. */
	emit(match: RegExp | string, message: LiveMessageLike): void {
		for (const live of this.open.values()) {
			const hit =
				typeof match === 'string'
					? live.text.includes(match)
					: match.test(live.text);
			if (!hit) continue;
			const handler = this.#handlers.get(live.id);
			if (handler) handler(message);
			else
				this.#buffered.set(live.id, [
					...(this.#buffered.get(live.id) ?? []),
					message,
				]);
		}
	}

	/** Drops the connection's live queries and announces a new connection. */
	reconnect(): void {
		this.open.clear();
		this.#handlers.clear();
		this.#buffered.clear();
		for (const listener of this.#connected) listener();
	}

	/** The statements run that match. */
	ran(match: RegExp): string[] {
		return this.statements.filter((statement) => match.test(statement));
	}
}

function split(text: string): string[] {
	return text
		.split(/\n;\n?|;\s*\n/)
		.map((statement) => statement.trim().replace(/;$/, ''))
		.filter((statement) => statement !== '');
}

/** Lets pending promises and microtasks run. */
export async function settle(): Promise<void> {
	for (let i = 0; i < 5; i += 1) await new Promise((go) => setTimeout(go, 0));
}

export type Row = { id: string; name: string };

/** A `RecordId`-like value: compares by text, like the SDK's. */
export function rid(text: string): { toString(): string } {
	return { toString: () => text };
}
