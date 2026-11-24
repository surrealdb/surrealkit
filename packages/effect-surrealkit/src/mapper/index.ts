/**
 * `.surql` imports: the module one file yields, as the content mapper gives
 * it to TypeScript 7.1 and the run-time loaders give it to Node, Bun and
 * Vite. Each renders through `renderStatementModules`.
 */
export { type SurqlBunPlugin, surqlBunPlugin } from './bun.js';
export {
	type LoadSurqlOptions,
	loadSurqlModule,
	SurqlModuleError,
	surqlFileAt,
	surqlModuleIn,
} from './load.js';
export {
	EMPTY_SURQL_MODULE,
	renderSurqlModule,
	type SurqlDiagnostic,
	type SurqlFile,
	type SurqlModule,
	type SurqlModuleOptions,
	surqlScript,
} from './module.js';
export {
	decodeSurqlOptions,
	MAPPER_PACKAGE,
	openSurqlProject,
	type SurqlMapperOptions,
	type SurqlOptionProblem,
	type SurqlProject,
	type SurqlSettings,
	surqlProjectFor,
} from './project.js';
