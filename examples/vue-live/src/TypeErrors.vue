<!--
	Not used by the app: `npm run typecheck` (vue-tsc) compiles it, and each
	`@vue-expect-error` must meet a type error, or the check fails. Together
	they show the generated types reach the composables and the components.
-->
<script setup lang="ts">
import { LiveQuery, Query, useLive, useQuery } from 'vue-surrealkit';

const all = useLive(
	'SELECT id, title, status, owner.name AS owner, created FROM task ORDER BY created DESC',
);
// A row's status is the schema's literal union.
all.data.value.map((task) => task.status satisfies 'todo' | 'doing' | 'done');
// @ts-expect-error `title` is a string, not a number.
all.data.value.map((task) => task.title satisfies number);
// @ts-expect-error a required parameter (`$id`) is missing.
useQuery('UPDATE $id SET status = $status');
</script>

<template>
	<!-- @vue-expect-error a composable's result carries its own params; none may be passed beside it. -->
	<LiveQuery :q="all" :params="{ status: 'todo' }" />
	<!-- @vue-expect-error a required parameter (`$id`) is missing. -->
	<Query q="DELETE $id" />
	<LiveQuery :q="all">
		<template #default="{ data }">
			<!-- @vue-expect-error not a field of the row. -->
			{{ data.map((task) => task.titel) }}
		</template>
	</LiveQuery>
</template>
