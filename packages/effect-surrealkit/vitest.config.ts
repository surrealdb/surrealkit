import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

export default defineConfig({
	resolve: {
		// Generated modules import the package by name; point that at the
		// sources so tests run without a build.
		alias: {
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
