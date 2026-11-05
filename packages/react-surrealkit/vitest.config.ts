import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
	resolve: {
		// Test against the core's sources, with one copy of the SDK.
		alias: {
			'surrealkit-query': fileURLToPath(
				new URL('../surrealkit-query/src/index.ts', import.meta.url),
			),
		},
		dedupe: ['surrealdb'],
	},
	test: {
		environment: 'jsdom',
		include: ['test/**/*.test.tsx'],
	},
});
