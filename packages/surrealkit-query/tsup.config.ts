import { defineConfig } from 'tsup';

export default defineConfig({
	// The cache runs in browsers, Workers and Node alike.
	entry: { index: 'src/index.ts' },
	format: ['esm'],
	dts: true,
	sourcemap: true,
	target: 'es2022',
	platform: 'neutral',
	external: ['surrealdb'],
});
