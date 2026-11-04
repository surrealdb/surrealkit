<script lang="ts">
	import { createQuery } from "@surrealdb/svelte";

	let { min } = $props();
	const people = createQuery("SELECT * FROM person", {});
</script>

<Query q="SELECT * FROM person WHERE age > {min}" params={{ min }} />
<LiveQuery q={`SELECT * FROM post`} />
<Query
	q='SELECT * FROM tag'
	params={{}}
/>
<Other q="SELECT * FROM other" />
<Query q={people} />
<Query q="SELECT * FROM note WHERE body = 'a > b'" />
<!-- <Query q="SELECT * FROM commented" /> -->
