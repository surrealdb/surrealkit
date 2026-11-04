<script lang="ts">
	import { setSurreal } from 'svelte-surrealkit';
	import { Surreal } from 'surrealdb';

	import { browser } from '$app/environment';
	import { SCOPE, SURREAL_URL } from '$lib/db';

	let { children } = $props();

	// One browser connection for every query on the page. The server render
	// never subscribes, so it gets a client that is never connected.
	const db = new Surreal();
	if (browser) void db.connect(SURREAL_URL, SCOPE);
	setSurreal(db);
</script>

{@render children()}
