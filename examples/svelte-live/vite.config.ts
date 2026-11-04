import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [sveltekit()],
	// The bindings and the app share one copy of the SDK and of the cache.
	resolve: { dedupe: ['surrealdb', 'surrealkit-query', 'svelte'] },
	ssr: { noExternal: ['svelte-surrealkit'] },
});
