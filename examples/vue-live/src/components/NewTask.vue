<script setup lang="ts">
import { ref } from 'vue';
import { LiveQuery } from 'vue-surrealkit';

import { createTask } from '../api';

const title = ref('');
const owner = ref('');

// The new task arrives through the live list, in this tab and every other.
async function add() {
	if (!title.value.trim()) return;
	await createTask(title.value.trim(), owner.value);
	title.value = '';
}
</script>

<template>
	<form class="new" @submit.prevent="add">
		<input v-model="title" placeholder="A new task" aria-label="Title" required />
		<select v-model="owner" aria-label="Owner">
			<option value="">nobody</option>
			<LiveQuery q="SELECT id, name FROM person ORDER BY name">
				<template #default="{ data: people }">
					<option v-for="person in people" :key="String(person.id)" :value="String(person.id.id)">
						{{ person.name }}
					</option>
				</template>
			</LiveQuery>
		</select>
		<button type="submit">Add</button>
	</form>
</template>
