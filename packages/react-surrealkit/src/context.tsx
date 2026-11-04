/**
 * The client every hook below a provider uses.
 *
 * ```tsx
 * 'use client';
 * export function Providers({ children }: { children: React.ReactNode }) {
 * 	return <SurrealProvider client={db}>{children}</SurrealProvider>;
 * }
 * ```
 *
 * This is the browser's client. A Server Component runs queries with a
 * client scoped to the request (see `react-surrealkit/server`), never a
 * module-level one, which every request and every user would share.
 */

import { createContext, type ReactNode, useContext } from 'react';
import type { SurrealLike } from 'surrealkit-query';

const ClientContext = createContext<SurrealLike | null>(null);

export interface SurrealProviderProps {
	client: SurrealLike;
	children?: ReactNode;
}

/** Makes `client` the client of every hook below it. */
export function SurrealProvider({ client, children }: SurrealProviderProps) {
	return (
		<ClientContext.Provider value={client}>
			{children}
		</ClientContext.Provider>
	);
}

/**
 * The client a hook uses: `override` when given, else the provider's.
 * Throws when there is neither.
 */
export function useSurreal(override?: SurrealLike): SurrealLike {
	const provided = useContext(ClientContext);
	const client = override ?? provided;
	if (!client) {
		throw new Error(
			'react-surrealkit: no client. Wrap the tree in <SurrealProvider client={db}>, or pass { client }.',
		);
	}
	return client;
}
