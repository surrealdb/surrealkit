# vue-surrealkit

Use `vue-surrealkit` to show typed SurrealQL queries and live queries in Vue 3. The query text types its rows and parameters, from the module that `surreal kit compile` generates. The API matches [svelte-surrealkit](../svelte-surrealkit) and [react-surrealkit](../react-surrealkit), so moving between them is mechanical.

```vue
<LiveQuery q="SELECT * FROM user ORDER BY name LIMIT $lim START $start" :params="{ lim, start }">
	<template #default="{ data: users, stale }">
		<ul :class="{ dim: stale }"><li v-for="u in users" :key="String(u.id)">{{ u.name }}</li></ul>
	</template>
	<template #empty>Nobody yet.</template>
</LiveQuery>
```

When `lim` or `start` changes, the page runs again. When the component unmounts, its live query is killed.

## Install

The package is not published. Link it from this repository, with `file:` dependencies on `packages/vue-surrealkit` and `packages/surrealkit-query`, as [examples/vue-live](../../examples/vue-live) does.

Peer dependencies: `vue` 3.5 or later, and `surrealdb` 2.1.1 or later.

## Set up

1. Register the generated queries. Set `register = "surrealkit-query"` under `[typegen.queries]` in `surrealkit.toml`, and the generated module does it for you. Without it, add the augmentation yourself (see [the core README](../surrealkit-query/README.md#register-the-generated-queries)).

2. Give the browser's client to the app:

   ```ts
   import { Surreal } from 'surrealdb';
   import { surrealkit } from 'vue-surrealkit';
   import { liveQueries } from './generated/queries';

   const db = new Surreal();
   void db.connect('wss://<your-host>', {
   	namespace: 'app',
   	database: 'app',
   	reconnect: { attempts: -1 },
   });
   createApp(App).use(surrealkit, { client: db, live: liveQueries }).mount('#app');
   ```

   For one subtree, call `provideSurreal(db, { live: liveQueries })` in a component's setup instead. `reconnect: { attempts: -1 }` keeps the client retrying: the SDK otherwise stops after five attempts, and live views stay in their error state (`disconnected`) until a reload.

## Composables

| Composable | Result |
| --- | --- |
| `useQuery(q, params?, options?)` | Runs the query once. `data` is the query's result. |
| `useLive(q, params?, options?)` | Keeps the rows current. `data` is always an array. |

Both return refs: `data`, `selected`, `error`, `status`, `loading` and `stale`, and also `refetch()` and the query `text`. Destructure them in `setup` to use them in a template.

- **`q`** is a value, a ref or a getter.
- **`params`** is a value, a ref, a getter or a `reactive()` object. It is compared by value: different values stop the old query (a live one is killed) and start the new one, and equal values run nothing. It is required when the query has a required parameter.
- **`options.select`** maps the result into `selected`. **`options.enabled`** (a value, ref or getter) keeps the query from subscribing while it is `false`. **`options.client`** overrides the provided client.
- When the parameters change, `data` keeps the previous result, with `stale` set, until the new one lands. A result is never kept across `cacheFor(db).reset()`.

The query starts with the component, or the `effectScope`, that calls the composable, and stops when it is unmounted. Call it during `setup`.

## Components

`<Query>` runs a query once. `<LiveQuery>` keeps the rows current. They take the same props and slots, so switching one for the other is a one-word change.

| Prop | Purpose |
| --- | --- |
| `q` | The query text (a key of `Queries`), a payload from `preload`, or a `useQuery` / `useLive` result. |
| `params` | The query's parameters. Required when the query has a required parameter; a type error beside a composable's result, which has its own. |
| `select` | Maps the result before the default slot and the `empty` check. The slot gets its typed return value. |
| `enabled` | While `false`, nothing subscribes and nothing renders. |
| `client` | A client to use instead of the provided one. |
| `v-model:data`, `v-model:selected` | The result, and what `select` made of it, outside the component. |

| Slot | Renders |
| --- | --- |
| `default` | `{ data, loading, stale, error }`: the result (or what `select` made of it). `stale` is set while the previous parameters' result is shown. |
| `empty` | Instead of `default`, when the result is `undefined`, `null` or an empty array. |
| `loading` | Until the first result. Without it, nothing renders while loading. |
| `error` | `{ error }`, on failure. Without it, a failure with a result to show renders `default` with `error` set, and a failure with none is thrown to the nearest `onErrorCaptured`. |

To use the same rows inside and outside a component, make the query with a composable and pass its result as `q`. Both then share one subscription, and the component renders the composable's own `select`ion:

```vue
<script setup lang="ts">
const research = useLive("SELECT * FROM card WHERE kind = 'research'", undefined, {
	select: (cards) => cards.filter((c) => c.open),
});
const { selected: open } = research;
</script>

<template>
	<button>Research {{ open.length }}</button>
	<LiveQuery :q="research"><template #default="{ data }">…</template></LiveQuery>
</template>
```

`v-model:selected` also works, but the bound variable takes part in typing the slot. Declare it as `select`'s return type, `ref<ReturnType<typeof pick>>([])` with `:select="pick"`, or the slot's rows take the variable's type.

The differences between the two are only the ones live and one-shot need:
- `<LiveQuery>` takes a single `SELECT` whose result is an array of rows; anything else is a type error.
- Its `data` is `[]` before the first result, where `<Query>`'s is `undefined`.
- It follows every change to the records.

## Server rendering

A `preload(db, text, params)` payload (exported here and from `surrealkit-query`) works as `q` for the composables and components. It renders its result at once, also in a server render, which runs no query. That is enough for a hand-rolled server render. Nuxt integration (fetching in `useAsyncData` and passing the payload through its state) is not covered yet.

## Development

```bash
bun install
bun run test         # composables and components against a mocked engine
SURREAL_BIN=$(which surreal) bun run test   # also against a real engine
bun run typecheck    # tsc, then vue-tsc goldens for templates (test/types)
bun run build
```
