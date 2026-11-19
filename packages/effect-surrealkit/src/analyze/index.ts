/**
 * A SurrealQL statement analyzer: derives what an authored text reads,
 * defines, returns and throws, fills template holes, and renders typed
 * statement modules. Pure and host-neutral; the `effect-surrealkit
 * statements` command is a thin filesystem wrapper over
 * {@link renderStatementModules}.
 */
export {
	type Analysis,
	type AnalyzeOptions,
	analyze,
	analyzeSync,
	type Diagnostic,
} from './analyze.js';
export {
	AnalyzeError,
	DEFAULT_HOLE_DELIMITERS,
	type HoleDelimiters,
} from './lexer.js';
export {
	type RenderStatementModulesInput,
	renderStatementModules,
	renderStatementModulesSync,
	STATEMENTS_MARK,
	type StatementModule,
	type StatementModules,
	StatementModulesError,
	type StatementProblem,
	type StatementScope,
	type StatementSourceFile,
	type StatementWarning,
} from './render.js';
export {
	FillError,
	type FillOptions,
	type Fills,
	fill,
	type Text,
	text,
} from './text.js';
