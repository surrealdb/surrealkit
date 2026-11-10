import { fileURLToPath } from 'node:url';

import { svelte } from '@sveltejs/vite-plugin-svelte';
import { svelteTesting } from '@testing-library/svelte/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [svelte(), svelteTesting()],
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
		include: ['test/**/*.test.ts'],
	},
});
