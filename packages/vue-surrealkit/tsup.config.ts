import { defineConfig } from 'tsup';

export default defineConfig({
	entry: { index: 'src/index.ts' },
	format: ['esm'],
	dts: true,
	sourcemap: true,
	target: 'es2022',
	platform: 'neutral',
	external: ['vue', 'surrealdb', 'surrealkit-query'],
});
