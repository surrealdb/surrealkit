import { defineConfig } from "tsup";

export default defineConfig([
	{
		// The runtime codecs and the emitter run in browsers and Workers as well
		// as Node, so they target the language level rather than a Node release.
		entry: {
			index: "src/index.ts",
			emitter: "src/emitter/index.ts",
			statement: "src/statement/index.ts",
			functions: "src/functions/index.ts",
			client: "src/client/index.ts",
			analyze: "src/analyze/index.ts",
			// Imports `@surrealdb/node` (an optional peer) only when a memory
			// database starts; nothing else imports this entry.
			testing: "src/testing/index.ts",
		},
		format: ["esm"],
		dts: true,
		sourcemap: true,
		target: "es2022",
		platform: "neutral",
		external: ["effect", "surrealdb", "@surrealdb/node"],
	},
	{
		entry: { cli: "src/cli/main.ts" },
		format: ["esm"],
		sourcemap: true,
		target: "node20",
		platform: "node",
		external: ["effect", "surrealdb"],
		banner: { js: "#!/usr/bin/env node" },
	},
	{
		// `.surql` imports: the content mapper's process (`mapper-server`,
		// which TypeScript starts) and the Node, Bun and Vite loaders. They read
		// files, so they target Node; none of them imports an engine.
		entry: {
			mapper: "src/mapper/index.ts",
			register: "src/mapper/register.ts",
			"mapper-server": "src/mapper/bin.ts",
		},
		format: ["esm"],
		dts: { entry: { mapper: "src/mapper/index.ts", register: "src/mapper/register.ts" } },
		sourcemap: true,
		target: "node20",
		platform: "node",
		external: ["effect", "surrealdb"],
	},
]);
