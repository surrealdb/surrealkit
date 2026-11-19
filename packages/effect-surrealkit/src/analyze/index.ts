/**
 * A SurrealQL statement analyzer: derives what an authored text reads,
 * defines, returns and throws, and fills template holes. Pure and
 * host-neutral; the `effect-surrealkit statements` command runs it over
 * `.surql` files.
 */
export {
	type Analysis,
	type AnalyzeOptions,
	analyze,
	analyzeSync,
	type Diagnostic,
} from './analyze.js';
export { AnalyzeError, DEFAULT_HOLE_PATTERN } from './lexer.js';
export {
	FillError,
	type FillOptions,
	type Fills,
	fill,
	type Text,
	text,
} from './text.js';
