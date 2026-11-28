/**
 * A Bun plugin that loads `.surql` imports as the modules the content mapper
 * describes, for `Bun.plugin` at run time (`bun --preload
 * effect-surrealkit/register`) and for `Bun.build`.
 */
import { type LoadSurqlOptions, loadSurqlModule } from './load.js';

/** The part of Bun's plugin interface this plugin uses. */
export interface SurqlBunPlugin {
	readonly name: string;
	readonly setup: (build: {
		readonly onLoad: (
			options: { readonly filter: RegExp },
			load: (args: { readonly path: string }) => {
				readonly contents: string;
				readonly loader: 'ts';
			},
		) => unknown;
	}) => void;
}

/** `.surql` files as TypeScript modules, read from their project's options. */
export const surqlBunPlugin = (
	options: LoadSurqlOptions = {},
): SurqlBunPlugin => ({
	name: 'effect-surrealkit-surql',
	setup: (build) => {
		build.onLoad({ filter: /\.surql$/u }, ({ path }) => ({
			contents: loadSurqlModule(path, options),
			loader: 'ts',
		}));
	},
});
