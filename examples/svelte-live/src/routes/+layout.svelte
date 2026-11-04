<script lang="ts">
	import { Surreal } from 'surrealdb';
	import { cacheFor, setSurreal } from 'svelte-surrealkit';

	import { browser } from '$app/environment';
	import { SCOPE, SURREAL_URL } from '$lib/db';

	let { data, children } = $props();

	// One browser connection for every query on the page, signed in as the
	// user. The server render never subscribes, so it gets a client that is
	// never connected.
	const db = new Surreal();
	setSurreal(db);

	// The current token, read again whenever the SDK (re)connects.
	let token = $derived(data.token);
	if (browser) void db.connect(SURREAL_URL, { ...SCOPE, authentication: () => token });

	// Signing in or out changes the identity under every cached query: switch
	// the session, then reset the cache so no rows from the previous identity
	// stay on screen.
	// The identity the connection started with; the effect follows changes.
	// svelte-ignore state_referenced_locally
	let signedIn = data.token;
	$effect(() => {
		const next = data.token;
		if (next === signedIn) return;
		signedIn = next;
		void (next ? db.authenticate(next) : db.invalidate()).then(() => cacheFor(db).reset());
	});
</script>

{@render children()}
