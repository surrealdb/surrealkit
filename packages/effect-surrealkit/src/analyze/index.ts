/**
 * A SurrealQL statement analyzer: derives what an authored text reads,
 * defines, returns, writes and throws, the tables and functions it names,
 * what its bindings are used as and the fields it projects and compares,
 * and fills template holes. A lexer, not a parser: where it is not certain
 * it records nothing. Pure and host-neutral.
 */
export {
	type Analysis,
	type AnalyzeOptions,
	analyze,
	analyzeSync,
	type Diagnostic,
	type HoleScope,
} from './analyze.js';
export {
	AnalyzeError,
	DEFAULT_HOLE_DELIMITERS,
	type HoleDelimiters,
} from './lexer.js';
export type { ProjectedField } from './projections.js';
export {
	FillError,
	type FillOptions,
	type Fills,
	fill,
	type Text,
	type TextHoleScope,
	type TextHoleScopes,
	text,
} from './text.js';
export type {
	BindingUse,
	BindingUses,
	FieldReference,
} from './uses.js';
