<script setup lang="ts">
import { computed } from 'vue';
import { LiveQuery, useLive } from 'vue-surrealkit';

import { deleteTask, moveTask } from './api';
import NewTask from './components/NewTask.vue';
import Search from './components/Search.vue';

const STATUSES = ['todo', 'doing', 'done'] as const;

// Every task, live. The summary reads the composable; <LiveQuery> renders the
// list from the same subscription.
const all = useLive(
	'SELECT id, title, status, owner.name AS owner, created FROM task ORDER BY created DESC',
);
const { data: tasks } = all;
const done = computed(() => tasks.value.filter((task) => task.status === 'done').length);
</script>

<template>
	<main>
		<h1>Tasks</h1>
		<p class="muted" data-testid="summary">{{ tasks.length }} tasks · {{ done }} done</p>
		<NewTask />
		<LiveQuery :q="all">
			<template #default="{ data, stale }">
				<ul class="tasks" :class="{ dim: stale }">
					<li v-for="task in data" :key="String(task.id)" :data-status="task.status">
						<span class="title">{{ task.title }}</span>
						<span class="muted">{{ task.owner ?? 'nobody' }}</span>
						<select
							:aria-label="`Status of ${task.title}`"
							:value="task.status"
							@change="moveTask(String(task.id.id), ($event.target as HTMLSelectElement).value as (typeof STATUSES)[number])"
						>
							<option v-for="status in STATUSES" :key="status">{{ status }}</option>
						</select>
						<button type="button" @click="deleteTask(String(task.id.id))">Delete</button>
					</li>
				</ul>
			</template>
			<template #loading><p class="muted">Loading…</p></template>
			<template #empty><p class="muted">No tasks yet. Add one above.</p></template>
			<template #error="{ error }">
				<p role="alert" class="error">Can't reach the database: {{ error.message }}</p>
			</template>
		</LiveQuery>
		<Search />
	</main>
</template>
