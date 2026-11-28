/**
 * Loads `.surql` imports at run time, as the modules the content mapper
 * describes: `node --import effect-surrealkit/register` (a synchronous
 * `module.registerHooks` load hook, Node 22.15 or later) or `bun --preload
 * effect-surrealkit/register` (a Bun plugin). Each file is rendered in the
 * process, with no engine, cache or worker.
 */
import * as nodeModule from 'node:module';
import { fileURLToPath } from 'node:url';

import { surqlBunPlugin } from './bun.js';
import { loadSurqlModule } from './load.js';

declare const Bun:
	| undefined
	| { readonly plugin: (plugin: ReturnType<typeof surqlBunPlugin>) => void };

interface NodeHooks {
	readonly registerHooks?: (hooks: {
		readonly load: (
			url: string,
			context: unknown,
			nextLoad: (url: string, context: unknown) => unknown,
		) => unknown;
	}) => unknown;
	readonly stripTypeScriptTypes?: (code: string) => string;
}

if (typeof Bun !== 'undefined') {
	Bun.plugin(surqlBunPlugin());
} else {
	const { registerHooks, stripTypeScriptTypes } =
		nodeModule as unknown as NodeHooks;
	if (
		typeof registerHooks !== 'function' ||
		typeof stripTypeScriptTypes !== 'function'
	)
		throw new Error(
			`effect-surrealkit/register needs module.registerHooks and module.stripTypeScriptTypes, from Node 22.15 or 23.5; this is Node ${process.versions.node}`,
		);
	registerHooks({
		load: (url, context, nextLoad) => {
			if (
				!url.startsWith('file:') ||
				!new URL(url).pathname.endsWith('.surql')
			)
				return nextLoad(url, context);
			return {
				format: 'module',
				source: stripTypeScriptTypes(
					loadSurqlModule(fileURLToPath(url)),
				),
				shortCircuit: true,
			};
		},
	});
}
