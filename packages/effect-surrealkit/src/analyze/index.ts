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
	type HoleScope,
} from './analyze.js';
export {
	AnalyzeError,
	DEFAULT_HOLE_DELIMITERS,
	type HoleDelimiters,
} from './lexer.js';
export {
	type PinnableStatement,
	type PinnedStatementDrift,
	type PinnedStatementProblem,
	PinnedStatementsError,
	pinnedStatementProblems,
	renderPinnedStatements,
} from './pin.js';
export {
	type DeclaredDetail,
	type DeclaredNames,
	type DeclaredTable,
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
export type { BindingUse, BindingUses } from './uses.js';
