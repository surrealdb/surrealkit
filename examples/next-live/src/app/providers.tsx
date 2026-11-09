'use client';

import { type ReactNode, useState } from 'react';
import { SurrealProvider } from 'react-surrealkit';
import { Surreal } from 'surrealdb';

import { liveQueries } from '@/generated/queries';
import { SCOPE, SURREAL_URL } from '@/lib/config';

/**
 * The browser's one connection, as a guest: it can read every table the
 * page shows and write none of them. Every hook and component below uses
 * it, with the engine's live facts from the generated module.
 */
export function Providers({ children }: { children: ReactNode }) {
	const [db] = useState(() => {
		const client = new Surreal();
		// Retry for as long as the page is open: the SDK's default gives up after
		// five attempts, and the live lists would then stay in their error state.
		if (typeof window !== 'undefined') {
			void client.connect(SURREAL_URL, {
				...SCOPE,
				reconnect: { attempts: -1, retryDelayMax: 5000 },
			});
		}
		return client;
	});
	return (
		<SurrealProvider client={db} live={liveQueries}>
			{children}
		</SurrealProvider>
	);
}
