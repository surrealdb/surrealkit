<script context="module" lang="ts">
	export const all = (db: Db) => db.query("SELECT * FROM post");
</script>

<script lang="ts">
	let { db } = $props();
	const one = db.query('SELECT * FROM post:1');
</script>

<h1>Posts</h1>
