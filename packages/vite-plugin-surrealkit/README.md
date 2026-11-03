# vite-plugin-surrealkit

Run SurrealKit from Vite so you do not need a separate watch process: either
`surrealkit sync` (apply schema changes to a development database) or, in
`mode: 'generate'`, `surrealkit compile` (regenerate typed code from the
schema without touching any database).

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

## Generate mode

```ts
surrealkitPlugin({
  mode: 'generate',
  // Where the configured [typegen] emitters write, relative to the Vite root.
  generatedDirs: ['src/generated/surreal'],
});
```

In `mode: 'generate'` the plugin runs `surrealkit compile` instead of
`surrealkit sync`. Compile builds the schema document in a throwaway embedded
engine and runs the emitters configured under `[typegen]` (for example the
Effect emitter, `effect-surrealkit`); it never connects to or changes a
database, so the database options (`targets`, `all`, `syncArgs`,
`reloadOnSync`) are rejected in this mode.

- The dev server is not ready until the first generation has finished, so no
  module importing generated code is served from a stale or missing set. If
  that first run fails, the error is logged and the server still starts.
- Closing the dev server terminates a running generation (with the processes
  it started); so do Ctrl-C (SIGINT), SIGHUP and the process exiting, which
  would otherwise miss a generation running in its own process group. On
  Windows the whole process tree is ended (`taskkill /T /F`). Emitters write
  atomically, and the next run regenerates.
- Vite's per-file updates for `generatedDirs` are always suppressed, and while
  a generation runs no per-file update goes out at all, so a half-published
  module set never reaches the browser whether or not `generatedDirs` is set.
  After a successful generation that changed those files, or during which
  updates were held back, one full reload is sent; a failed generation never
  reloads (held-back updates are logged; reload to apply them). Without
  `generatedDirs`, every successful generation reloads. Set
  `reloadOnGenerate: false` to reload only for held-back updates.
- With `include: ['build']`, `vite build` generates before bundling and fails
  if generation fails (`failBuildOnError`).

## Options

```ts
type RunMode = 'serve' | 'build';
type LogLevel = 'silent' | 'error' | 'info' | 'debug';

interface SurrealkitPluginOptions {
  /** `'sync'` (default) runs `surrealkit sync`; `'generate'` runs `surrealkit compile`. */
  mode?: 'sync' | 'generate';
  /** SurrealKit binary (or executable path). Default: `surrealkit`. */
  binary?: string;
  /** Working directory for the SurrealKit process. Default: Vite root. */
  cwd?: string;
  /** Extra env vars merged with `process.env`. */
  env?: Record<string, string>;
  /** Additional args appended after `surrealkit sync` (sync mode). */
  syncArgs?: string[];
  /** Additional args appended after `surrealkit compile` (generate mode). */
  generateArgs?: string[];
  /** Schema modules to sync or compile. Default: SurrealKit's selection. */
  schemas?: string[];
  /** Database targets to sync to (sync mode). Default: the primary/only target. */
  targets?: string[];
  /** Sync every declared module against every declared target (sync mode). */
  all?: boolean;
  /** Globs (relative to Vite root) that trigger a run in dev. */
  schemaGlobs?: string[];
  /** Directories the generation writes (generate mode); see above. */
  generatedDirs?: string[];
  /** Where the plugin runs. Default: `['serve']`. */
  include?: RunMode[];
  /** Sync when the dev server starts or the build begins. Default: true. */
  runOnStartup?: boolean;
  /** Full-reload after a schema-triggered sync in dev (sync mode). Default: false. */
  reloadOnSync?: boolean;
  /** Full-reload after a successful generation in dev (generate mode). Default: true. */
  reloadOnGenerate?: boolean;
  /** Debounce window for dev file changes, in ms. Default: 150. */
  debounceMs?: number;
  /** Logging verbosity. Default: `'info'`. */
  logLevel?: LogLevel;
  /** Fail the Vite build if the run exits non-zero. Default: true. */
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
