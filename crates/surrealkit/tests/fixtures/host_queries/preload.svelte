<script lang="ts">
	import * as kit from '$lib/kit';

	let { db, min } = $props();
	const adults = preload(db, `SELECT * FROM person WHERE age > ${min}`);
	const posts = kit.preload(db, "SELECT * FROM post");
	const swapped = preload("not a query", db);
	const bare = preload(db);
</script>

<h1>{adults.length} adults, {posts.length} posts</h1>
