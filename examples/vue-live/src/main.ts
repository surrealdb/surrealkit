import { Surreal } from 'surrealdb';
import { createApp } from 'vue';
import { surrealkit } from 'vue-surrealkit';

import App from './App.vue';
import { liveQueries } from './generated/queries';
import './style.css';

/** The database `npm run db` starts; set VITE_SURREAL_URL to use another. */
const url = import.meta.env.VITE_SURREAL_URL ?? 'ws://127.0.0.1:8000';

// The browser's one connection, as a guest: it can read every table the page
// shows and write none of them. It retries for as long as the page is open;
// the SDK's default gives up after five attempts.
const db = new Surreal();
void db.connect(url, {
	namespace: 'demo',
	database: 'demo',
	reconnect: { attempts: -1, retryDelayMax: 5000 },
});

createApp(App).use(surrealkit, { client: db, live: liveQueries }).mount('#app');
