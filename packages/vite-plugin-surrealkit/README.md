# vite-plugin-surrealkit

Run `surrealkit sync` from Vite so you do not need a separate `surrealkit sync --watch` process.

## Install

```sh
npm i -D vite-plugin-surrealkit
```

## Usage

```ts
// vite.config.ts
import { defineConfig } from 'vite';
import { surrealkitPlugin } from 'vite-plugin-surrealkit';

export default defineConfig({
  plugins: [
    surrealkitPlugin({
      // Optional: pass flags to `surrealkit sync`
      syncArgs: ['--allow-shared-prune'],
    }),
  ],
});
```

Default behavior:

- Runs `surrealkit sync` on dev server startup
- Watches `database/schema/**/*.surql` and
  `database/modules/*/schema/**/*.surql`
- Runs `surrealkit sync` again whenever those files are added/changed/removed
- Debounces and queues runs to avoid overlapping processes
- Dev only. Set `include: ['serve', 'build']` to also sync during `vite build`

## Options

```ts
type RunMode = 'serve' | 'build';
type LogLevel = 'silent' | 'error' | 'info' | 'debug';

interface SurrealkitPluginOptions {
  /** SurrealKit binary (or executable path). Default: `surrealkit`. */
  binary?: string;
  /** Working directory for the SurrealKit process. Default: Vite root. */
  cwd?: string;
  /** Extra env vars merged with `process.env`. */
  env?: Record<string, string>;
  /** Additional args appended after `surrealkit sync`. */
  syncArgs?: string[];
  /** Schema modules to sync. Default: whatever `surrealkit sync` selects. */
  schemas?: string[];
  /** Database targets to sync to. Default: the primary/only target. */
  targets?: string[];
  /** Sync every declared module against every declared target. */
  all?: boolean;
  /** Globs (relative to Vite root) that trigger sync in dev. */
  schemaGlobs?: string[];
  /** Where the plugin runs. Default: `['serve']`. */
  include?: RunMode[];
  /** Sync when the dev server starts or the build begins. Default: true. */
  runOnStartup?: boolean;
  /** Full-reload after a schema-triggered sync in dev. Default: false. */
  reloadOnSync?: boolean;
  /** Debounce window for dev file changes, in ms. Default: 150. */
  debounceMs?: number;
  /** Logging verbosity. Default: `'info'`. */
  logLevel?: LogLevel;
  /** Fail the Vite build if sync exits non-zero. Default: true. */
  failBuildOnError?: boolean;
}
```

## Vite Compatibility

Requires Vite `^8.0.0` and Node `^20.19.0 || >=22.12.0`, matching Vite's own
`engines`. Both the dev-server and build paths are exercised against Vite 8 in
CI.

For Vite 7, stay on `vite-plugin-surrealkit@0.1.x`. Do not use 0.1.x with
Vite 8: it hands raw globs to Vite's watcher, which silently stops schema
changes from triggering a sync.

### Multi-environment builds

Vite resolves the config once per build environment, so a build that produces
more than one environment (a client + SSR app, for instance) instantiates the
plugin once per environment and runs `surrealkit sync` once per environment.
`surrealkit sync` is idempotent, so the extra run is redundant rather than
harmful. There is no shared state between those instances to deduplicate
through; set `include: ['serve']` (the default) if you would rather not sync
during builds at all.
