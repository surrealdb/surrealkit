import { defineConfig } from "tsup";

export default defineConfig([
	{
		// The runtime codecs and the emitter run in browsers and Workers as well
		// as Node, so they target the language level rather than a Node release.
		entry: { index: "src/index.ts", emitter: "src/emitter/index.ts" },
		format: ["esm"],
		dts: true,
		sourcemap: true,
		target: "es2022",
		platform: "neutral",
		external: ["effect", "surrealdb"],
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
]);
