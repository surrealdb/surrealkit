<!--
	Type goldens for the components in templates. A marker comment names an
	error vue-tsc must report in the next element; golden.mjs fails on a
	missing or an unmarked error.
-->
<script setup lang="ts">
import { ref } from 'vue';

import { LiveQuery, Query, useLive } from '../../src/index.js';
import './register.js';

const BY_NAME = 'SELECT name, email FROM user WHERE name = $name LIMIT $limit';
const name = ref('ada');
const limit = ref(10);
const byName = useLive(BY_NAME, () => ({ name: name.value, limit: limit.value }));
const kinds = ref<string[]>([]);
const kindNames = useLive('SELECT kind FROM event', undefined, {
	select: (rows) => rows.map((row) => row.kind),
});
</script>

<template>
	<Query :q="BY_NAME" :params="{ name, limit }">
		<template #default="{ data }">
			<p v-for="row in data" :key="row.email">{{ row.name satisfies string }}</p>
		</template>
	</Query>
	<LiveQuery q="SELECT kind FROM event">
		<template #default="{ data, stale }">
			{{ data.map((row) => row.kind satisfies string) }}{{ stale satisfies boolean }}
		</template>
		<template #empty>none</template>
		<template #error="{ error }">{{ error.code }}</template>
	</LiveQuery>
	<!-- A composable's result carries its own params. -->
	<LiveQuery :q="byName">
		<template #default="{ data }">{{ data.map((row) => row.email satisfies string) }}</template>
	</LiveQuery>
	<!-- A composable's result shows its own selection. -->
	<LiveQuery :q="kindNames">
		<template #default="{ data }">{{ data satisfies string[] }}</template>
	</LiveQuery>
	<LiveQuery
		q="SELECT kind FROM event"
		:select="(rows) => rows.map((row) => row.kind)"
		v-model:selected="kinds"
	>
		<template #default="{ data }">{{ data satisfies string[] }}</template>
	</LiveQuery>

	<!-- expect-error: Property 'params' is missing -->
	<Query :q="BY_NAME" />
	<!-- expect-error: Type 'number' is not assignable to type 'string' -->
	<Query :q="BY_NAME" :params="{ name: 1 }" />
	<!-- expect-error: is not assignable to type 'never' -->
	<LiveQuery :q="byName" :params="{ name, limit }" />
	<!-- expect-error: LiveTextError -->
	<LiveQuery q="SELECT * FROM ONLY user:alice" />
	<LiveQuery q="SELECT kind FROM event">
		<template #default="{ data }">
			<!-- expect-error: Property 'nope' does not exist -->
			{{ data.map((row) => row.nope) }}
		</template>
	</LiveQuery>
</template>
