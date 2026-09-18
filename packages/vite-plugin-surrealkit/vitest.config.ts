import { defineConfig } from 'vitest/config';

export default defineConfig({
	test: {
		include: ['test/**/*.test.ts'],
		// Each test boots a real Vite server and a filesystem watcher, so give
		// them room and keep the suites off each other's temp directories.
		testTimeout: 30_000,
		hookTimeout: 30_000,
		fileParallelism: false,
	},
});
