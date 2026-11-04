import { defineConfig, type Options } from 'tsup';

const shared: Options = {
	format: ['esm'],
	dts: true,
	sourcemap: true,
	target: 'es2022',
	platform: 'neutral',
	external: ['react', 'surrealdb', 'surrealkit-query'],
};

export default defineConfig([
	// The hooks and the provider are client modules.
	{
		...shared,
		entry: { index: 'src/index.ts' },
		banner: { js: "'use client';" },
	},
	{ ...shared, entry: { server: 'src/server.ts' } },
]);
