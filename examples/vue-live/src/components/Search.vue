<script setup lang="ts">
import { computed, ref } from 'vue';
import { Query, useQuery } from 'vue-surrealkit';

type Filter = 'all' | 'todo' | 'doing' | 'done';

// A one-shot search. The text is a query parameter, so typing runs the query
// again (the previous results stay, dimmed, until the new ones land); the
// status filter is applied by `select`, without a query. Nothing runs until
// there is text to search for. <Query> renders the composable's selection,
// and the heading counts it: one query, typed once.
const text = ref('');
const filter = ref<Filter>('all');
const search = useQuery(
	'SELECT id, title, status, ->blocks->task.title AS blocks FROM task WHERE string::contains(string::lowercase(title), $text) ORDER BY title',
	computed(() => ({ text: text.value.trim().toLowerCase() })),
	{
		enabled: () => text.value.trim().length > 0,
		select: (rows) => rows.filter((row) => filter.value === 'all' || row.status === filter.value),
	},
);
const { selected: shown } = search;
</script>

<template>
	<section class="search">
		<h2>Search <span v-if="shown?.length" class="muted">({{ shown.length }} shown)</span></h2>
		<input v-model="text" placeholder="Search titles" aria-label="Search" />
		<select v-model="filter" aria-label="Filter by status">
			<option value="all">any status</option>
			<option value="todo">todo</option>
			<option value="doing">doing</option>
			<option value="done">done</option>
		</select>
		<Query :q="search">
			<template #default="{ data, stale }">
				<ul class="results" :class="{ dim: stale }" data-testid="results">
					<li v-for="row in data" :key="String(row.id)">
						{{ row.title }} <span class="muted">({{ row.status }})</span>
						<span v-if="row.blocks.length" class="muted"> · blocks {{ row.blocks.join(', ') }}</span>
					</li>
				</ul>
			</template>
			<template #loading><p class="muted">Searching…</p></template>
			<template #empty><p class="muted">No match.</p></template>
		</Query>
	</section>
</template>
