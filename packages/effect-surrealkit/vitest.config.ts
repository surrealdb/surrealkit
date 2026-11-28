import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
	resolve: {
		// Generated modules import the package by name; point that at the
		// sources so tests run without a build.
		// The subpaths come first: an alias also matches what starts with it.
		alias: {
			'effect-surrealkit/statement': fileURLToPath(
				new URL('./src/statement/index.ts', import.meta.url),
			),
			'effect-surrealkit/analyze': fileURLToPath(
				new URL('./src/analyze/index.ts', import.meta.url),
			),
			'effect-surrealkit': fileURLToPath(
				new URL('./src/index.ts', import.meta.url),
			),
		},
	},
	test: {
		include: ['test/**/*.test.ts'],
		// The engine suite boots a disposable SurrealDB process and the type
		// suite runs tsc; give them room.
		testTimeout: 120_000,
		hookTimeout: 60_000,
	},
});
