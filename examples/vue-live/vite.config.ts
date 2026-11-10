import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

import { api } from './server/api';

export default defineConfig({
	plugins: [vue(), api()],
	// The bindings and the app share one copy of the SDK and of Vue.
	resolve: { dedupe: ['surrealdb', 'surrealkit-query', 'vue'] },
});
