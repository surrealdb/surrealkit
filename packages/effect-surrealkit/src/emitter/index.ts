/**
 * The Effect emitter: SurrealKit schema document v2 in, Effect modules out.
 * Host-neutral (no filesystem access); the `effect-surrealkit` bin wraps it.
 */
export {
	canonicalJson,
	semanticDigest,
	semanticProjection,
} from './canonical.js';
export {
	DocumentError,
	decodeDocument,
	type Field,
	type Kind,
	type SchemaDocument,
	type Table,
} from './document.js';
export {
	emit,
	type GeneratedFile,
	GenerateError,
	type GenerateHooks,
	type GenerateResult,
	generate,
} from './generate.js';
export { decodeOptions, type EmitterOptions, OptionsError } from './options.js';
export { EMITTER_VERSION, PEER_VERSIONS } from './version.js';
