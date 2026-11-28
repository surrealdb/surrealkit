/**
 * The checks that compare an authored text with a schema's declared names:
 * a table or custom function the schema does not declare, a projected,
 * compared or assigned field a `SCHEMAFULL` table does not declare, and a
 * binding use that does not resolve. {@link checkText} is the one
 * implementation: `renderStatementModules` calls it for each file of a scope
 * with `declared`, and {@link schemaProblems} calls it for named texts
 * without rendering anything. Pure and host-neutral.
 */
import { type Analysis, analyzeSync } from './analyze.js';
import {
	AnalyzeError,
	type HoleDelimiters,
	holeDelimiters,
	lex,
} from './lexer.js';
import { analyzeTargets } from './targets.js';
import type { BindingUse, BindingUses } from './uses.js';

/** The tables and custom functions (`fn::…`) a schema declares. */
export interface DeclaredNames {
	readonly tables: ReadonlyArray<string>;
	readonly functions: ReadonlyArray<string>;
	/**
	 * What the schema says about each table and function, as
	 * `declaredNames(document)` (`effect-surrealkit/emitter`) reads it. When
	 * given, every use of a binding the analyzer finds must resolve (a field
	 * the table declares or accepts, an argument the function takes), and
	 * each part states, in `binds`, the simple kind of each binding whose
	 * every use resolves to the same one (`option<K>` when every use accepts
	 * NONE).
	 */
	readonly detail?: DeclaredDetail;
}

/** The schema detail of {@link DeclaredNames}: kinds as SurrealQL text. */
export interface DeclaredDetail {
	readonly tables: Readonly<Record<string, DeclaredTable>>;
	/** The kind text of each argument of each function, in order. */
	readonly functions: Readonly<Record<string, ReadonlyArray<string>>>;
}

export interface DeclaredTable {
	/**
	 * Each declared field (its path joined with `.`): its kind text, and
	 * `rejectsNone` when the engine rejects NONE assigned to it in every
	 * statement that can assign it (`CREATE`, `UPDATE` and `UPSERT`): a
	 * top-level field whose kind excludes NONE, without `DEFAULT`, `VALUE`
	 * or `COMPUTED`. Left out, nothing is refused for the field.
	 */
	readonly fields: Readonly<
		Record<
			string,
			{ readonly kind?: string; readonly rejectsNone?: boolean }
		>
	>;
	/**
	 * The table accepts fields it does not declare: it is schemaless, or a
	 * view, whose fields come from its SELECT.
	 */
	readonly acceptsUndeclared: boolean;
	/** The kind text of the id key, when the schema declares one. */
	readonly idKey?: string;
}

/** The field a binding is assigned to that rejects NONE; see `Part`. */
export interface RejectedNone {
	readonly table: string;
	readonly field: string;
	/** The field's kind text. */
	readonly kind: string;
}

/**
 * What a text does not resolve in the schema, at the line of the use. The
 * codes: `undeclaredTable`, `undeclaredFunction`, `undeclaredField` and
 * `extraArgument`.
 */
interface Finding {
	readonly line: number;
	readonly code: string;
	readonly message: string;
}

/** A simple kind's text: one a codec's stored type can be checked against. */
const SIMPLE =
	/^(?:string|bool|int|float|number|decimal|datetime|duration|uuid|record<[^<>|,\s]+>)$/u;

/** The simple kind a kind text is, `option<K>` read as `K`; else undefined. */
const simpleKind = (kind: string | undefined): string | undefined => {
	if (kind === undefined) return undefined;
	const inner = /^option<(.*)>$/u.exec(kind)?.[1] ?? kind;
	return SIMPLE.test(inner) ? inner : undefined;
};

/**
 * Whether NONE is acceptable to a use whose kind text is `kind`: comparing
 * a field with NONE always is; assigning to a field, or passing an
 * argument, only when its kind is `option<…>`; a record key never.
 */
const acceptsNone = (use: BindingUse, kind: string | undefined): boolean =>
	use.kind === 'field' && use.role !== 'assign'
		? true
		: use.kind !== 'key' &&
			kind !== undefined &&
			/^option<.*>$/u.test(kind);

/** Control characters, and the two line separators, as `\u{…}` in a message. */
const UNPRINTABLE = new RegExp(
	`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`,
	'gu',
);

/**
 * A schema name in backticks for a message, as SurrealQL quotes it: a
 * backtick or backslash in it escaped with a backslash, and a line break or
 * other control character written as `\u{…}`, so the name cannot end the
 * quotes or the message's line.
 */
const quoted = (name: string): string =>
	`\`${name
		.replace(/[\\`]/gu, '\\$&')
		.replace(
			UNPRINTABLE,
			(char) => `\\u{${(char.codePointAt(0) as number).toString(16)}}`,
		)}\``;

/** How a problem names a use that does not resolve. */
type Resolved =
	| {
			readonly kind: 'kind';
			readonly text: string | undefined;
			/** The use is a field that rejects NONE; see {@link DeclaredTable}. */
			readonly rejectsNone?: boolean;
	  }
	| {
			readonly kind: 'problem';
			readonly code: string;
			readonly message: string;
	  }
	| { readonly kind: 'unchecked' };

/**
 * What one use resolves to in the schema: the kind text of what it is
 * used as (undefined when the schema gives none), a problem when it cannot
 * resolve, or `unchecked` for a table or function the schema does not
 * declare (reported elsewhere) or the text defines itself.
 */
const resolve = (use: BindingUse, detail: DeclaredDetail): Resolved => {
	if (use.kind === 'argument') {
		const args = Object.hasOwn(detail.functions, use.function)
			? detail.functions[use.function]
			: undefined;
		if (args === undefined) return { kind: 'unchecked' };
		return use.index < args.length
			? { kind: 'kind', text: args[use.index] }
			: {
					kind: 'problem',
					code: 'extraArgument',
					message: `passes argument ${use.index + 1} to \`${use.function}\`, which takes ${args.length === 1 ? '1 argument' : `${args.length} arguments`}`,
				};
	}
	const table = Object.hasOwn(detail.tables, use.table)
		? detail.tables[use.table]
		: undefined;
	if (table === undefined) return { kind: 'unchecked' };
	if (use.kind === 'key') return { kind: 'kind', text: table.idKey };
	if (Object.hasOwn(table.fields, use.field)) {
		const field = table.fields[use.field];
		return {
			kind: 'kind',
			text: field?.kind,
			rejectsNone: field?.rejectsNone === true,
		};
	}
	if (use.field === 'id')
		return { kind: 'kind', text: `record<${use.table}>` };
	return table.acceptsUndeclared
		? { kind: 'kind', text: undefined }
		: {
				kind: 'problem',
				code: 'undeclaredField',
				message: `${use.role === 'assign' ? 'assigns' : 'compares'} ${quoted(use.field)} of ${quoted(use.table)}, which declares no such field`,
			};
};

/**
 * The uses that do not resolve in `detail`, the simple kind of each
 * binding whose every use resolves to the same one, and, for each binding
 * assigned to a field that rejects NONE, the first such field. With
 * `schemaChanges`, the text defines, alters or removes a table or field
 * itself, so the schema may not hold when its assignments run and no
 * field is recorded.
 */
const checkUses = (
	uses: BindingUses,
	detail: DeclaredDetail,
	schemaChanges: boolean,
): {
	readonly problems: ReadonlyArray<Finding>;
	readonly binds: Readonly<Record<string, string>>;
	readonly rejectsNone: Readonly<Record<string, RejectedNone>>;
} => {
	const problems: Finding[] = [];
	const binds: Record<string, string> = {};
	const rejectsNone: Record<string, RejectedNone> = {};
	for (const [name, list] of Object.entries(uses)) {
		const kinds = new Set<string | undefined>();
		let none = true;
		for (const use of list) {
			const resolved = resolve(use, detail);
			if (resolved.kind === 'problem') {
				problems.push({
					line: use.line,
					code: resolved.code,
					message: resolved.message,
				});
				kinds.add(undefined);
			} else if (resolved.kind === 'unchecked') kinds.add(undefined);
			else {
				kinds.add(simpleKind(resolved.text));
				if (!acceptsNone(use, resolved.text)) none = false;
				if (
					!schemaChanges &&
					use.kind === 'field' &&
					use.role === 'assign' &&
					resolved.rejectsNone === true &&
					resolved.text !== undefined &&
					!Object.hasOwn(rejectsNone, name)
				)
					rejectsNone[name] = {
						table: use.table,
						field: use.field,
						kind: resolved.text,
					};
			}
		}
		const [only] = kinds;
		if (kinds.size === 1 && only !== undefined)
			binds[name] = none ? `option<${only}>` : only;
	}
	return { problems, binds, rejectsNone };
};

/** Whether `table` declares `field`, or a field nested under it. */
const declaresField = (table: DeclaredTable, field: string): boolean =>
	field === '*' ||
	field === 'id' ||
	Object.hasOwn(table.fields, field) ||
	Object.keys(table.fields).some(
		(path) => path.startsWith(`${field}.`) || path.startsWith(`${field}[`),
	);

/**
 * The projected fields that a table of `detail` does not declare and does
 * not accept: a field of a `SCHEMAFULL` table. A table the schema does not
 * declare is reported by the names check; with `schemaChanges` the text
 * changes the schema itself, or a hole starts a statement that a fill can
 * make do so, and nothing is checked.
 */
const checkProjected = (
	projected: Analysis['projected'],
	detail: DeclaredDetail,
	schemaChanges: boolean,
): ReadonlyArray<Finding> =>
	schemaChanges
		? []
		: projected.flatMap(({ table, field, line }) => {
				const declared = Object.hasOwn(detail.tables, table)
					? detail.tables[table]
					: undefined;
				return declared === undefined ||
					declared.acceptsUndeclared ||
					declaresField(declared, field)
					? []
					: [
							{
								line,
								code: 'undeclaredField',
								message: `selects ${quoted(field)} of ${quoted(table)}, which declares no such field`,
							},
						];
			});

/**
 * The compared and assigned fields that a table of `detail` does not declare and does
 * not accept, by the rule {@link checkProjected} applies: the same tables,
 * nested fields and `id` resolve, and with `schemaChanges` nothing is
 * checked.
 */
const checkFields = (
	fields: Analysis['fields'],
	detail: DeclaredDetail,
	schemaChanges: boolean,
): ReadonlyArray<Finding> =>
	schemaChanges
		? []
		: fields.flatMap(({ table, field, role, line }) => {
				const declared = Object.hasOwn(detail.tables, table)
					? detail.tables[table]
					: undefined;
				return declared === undefined ||
					declared.acceptsUndeclared ||
					declaresField(declared, field)
					? []
					: [
							{
								line,
								code: 'undeclaredField',
								message: `${role === 'assign' ? 'assigns' : 'compares'} ${quoted(field)} of ${quoted(table)}, which declares no such field`,
							},
						];
			});

/**
 * Whether a hole of `sql` starts a statement: at the start of the text, or
 * after `;` or `{`. A fill there can be a statement that defines a table or
 * a field, which the text alone does not show.
 */
const holeStartsStatement = (
	sql: string,
	holes: HoleDelimiters | undefined,
): boolean => {
	const tokens = lex(sql, holeDelimiters(holes)).tokens;
	return tokens.some((token, i) => {
		const before = tokens[i - 1];
		return (
			token.kind === 'hole' &&
			(before === undefined ||
				(before.kind === 'punct' &&
					(before.value === ';' || before.value === '{')))
		);
	});
};

/** Whether `sql` defines, alters or removes a table or a field. */
const changesSchema = (
	sql: string,
	holes: HoleDelimiters | undefined,
): boolean => {
	const tokens = lex(sql, holeDelimiters(holes)).tokens;
	return tokens.some(
		(token, i) =>
			token.kind === 'word' &&
			['DEFINE', 'ALTER', 'REMOVE'].includes(token.value.toUpperCase()) &&
			tokens[i + 1]?.kind === 'word' &&
			['TABLE', 'FIELD'].includes(
				(tokens[i + 1]?.value ?? '').toUpperCase(),
			),
	);
};

/**
 * The tables and functions `sql` names that `declared` does not include and
 * the text does not define itself, each at the line of its first use.
 */
const undeclared = (
	sql: string,
	declared: DeclaredNames,
	holes: HoleDelimiters | undefined,
): ReadonlyArray<Finding> => {
	const targets = analyzeTargets(lex(sql, holeDelimiters(holes)).tokens);
	const tables = new Set([...declared.tables, ...targets.definedTables]);
	const functions = new Set([
		...declared.functions,
		...targets.definedFunctions,
	]);
	const found: Finding[] = [];
	const check = (
		at: ReadonlyMap<string, number>,
		known: ReadonlySet<string>,
		what: string,
		code: string,
	) => {
		for (const [name, line] of at)
			if (!known.has(name))
				found.push({
					line,
					code,
					message: `${what} \`${name}\`, which the schema does not declare`,
				});
	};
	check(targets.readAt, tables, 'reads table', 'undeclaredTable');
	check(targets.writtenAt, tables, 'writes table', 'undeclaredTable');
	check(
		targets.functionsAt,
		functions,
		'calls function',
		'undeclaredFunction',
	);
	return found.sort((a, b) => a.line - b.line);
};

/** One text, checked; see {@link checkText}. */
export type CheckedText =
	| { readonly kind: 'refused'; readonly error: AnalyzeError }
	| {
			readonly kind: 'analysed';
			readonly analysis: Analysis;
			/** What does not resolve in `declared`, sorted by line. */
			readonly problems: ReadonlyArray<Finding>;
			/** With `declared` detail: see `binds` of a part. */
			readonly binds?: Readonly<Record<string, string>>;
			/** With `declared` detail: see `rejectsNone` of a part. */
			readonly rejectsNone?: Readonly<Record<string, RejectedNone>>;
	  };

/**
 * Analyses `sql` and, with `declared`, checks it against the schema: the
 * tables and functions it names, and, with `declared.detail`, the uses of
 * its bindings and the fields it projects, compares and assigns. A template
 * is checked as written: a hole is never filled or guessed, a target behind
 * one is open and records nothing, and where a hole starts a statement no
 * field is checked, since a fill there can define one. A text the analyzer
 * refuses is `refused`; any other error is thrown.
 */
export const checkText = (
	sql: string,
	declared: DeclaredNames | undefined,
	holes: HoleDelimiters | undefined,
): CheckedText => {
	let analysis: Analysis;
	try {
		analysis = analyzeSync(sql, { ...(holes !== undefined && { holes }) });
	} catch (error) {
		if (error instanceof AnalyzeError) return { kind: 'refused', error };
		throw error;
	}
	if (declared === undefined)
		return { kind: 'analysed', analysis, problems: [] };
	const detail = declared.detail;
	const schemaChanges = detail !== undefined && changesSchema(sql, holes);
	const checked =
		detail === undefined
			? undefined
			: checkUses(analysis.uses, detail, schemaChanges);
	// A fill at a hole that starts a statement can define a field.
	const fieldsUnknown = schemaChanges || holeStartsStatement(sql, holes);
	const problems = [
		...undeclared(sql, declared, holes),
		...(checked?.problems ?? []),
		...(detail === undefined
			? []
			: [
					...checkProjected(
						analysis.projected,
						detail,
						fieldsUnknown,
					),
					...checkFields(analysis.fields, detail, fieldsUnknown),
				]),
	].sort((a, b) => a.line - b.line);
	return {
		kind: 'analysed',
		analysis,
		problems,
		...(checked !== undefined && {
			binds: checked.binds,
			rejectsNone: checked.rejectsNone,
		}),
	};
};

/** One named text for {@link schemaProblems}. */
export interface SchemaText {
	/** Any non-empty string; it only labels the text's problems. */
	readonly name: string;
	readonly sql: string;
}

/**
 * A problem {@link schemaProblems} found in one text. `code` is
 * `undeclaredTable`, `undeclaredFunction`, `undeclaredField` or
 * `extraArgument` for the schema checks (`line` of the use, no `column`),
 * the analyzer's code with `line` and `column` for a text it refuses or an
 * error it diagnoses, or `unnamed` for a text whose name is empty.
 */
export interface SchemaProblem {
	readonly name: string;
	readonly line: number;
	readonly column?: number;
	readonly code: string;
	readonly message: string;
	/** `<name>:<line>[:<column>]: <message> (<code>)`. */
	readonly text: string;
}

export interface SchemaProblemsOptions {
	/** The literal delimiters of a hole; default `__` and `__`. */
	readonly holes?: HoleDelimiters;
}

const compareProblems = (a: SchemaProblem, b: SchemaProblem): number =>
	a.name !== b.name
		? a.name < b.name
			? -1
			: 1
		: a.line !== b.line
			? a.line - b.line
			: (a.column ?? 0) !== (b.column ?? 0)
				? (a.column ?? 0) - (b.column ?? 0)
				: a.text < b.text
					? -1
					: a.text > b.text
						? 1
						: 0;

/**
 * The problems `renderStatementModules` reports for a scope with these
 * `declared` names, for named texts, rendering nothing: a table or custom
 * function the schema does not declare and the text does not define, and,
 * with `declared.detail`, a projected, compared or assigned field a
 * `SCHEMAFULL` table does not declare and a binding used as a field or
 * argument that does not resolve; also a text the analyzer refuses and an
 * error it diagnoses, so one bad text never hides the others. A name is any
 * non-empty string. A template is checked as written, never filled; see
 * {@link checkText}. Sorted by name, line and column, without duplicates.
 * Reads and writes nothing.
 */
export const schemaProblems = (
	texts: Iterable<SchemaText>,
	declared: DeclaredNames,
	options: SchemaProblemsOptions = {},
): ReadonlyArray<SchemaProblem> => {
	const found = new Map<string, SchemaProblem>();
	const add = (
		name: string,
		line: number,
		column: number | undefined,
		code: string,
		message: string,
	) => {
		const text = `${name}:${line}${column === undefined ? '' : `:${column}`}: ${message} (${code})`;
		if (!found.has(text))
			found.set(text, {
				name,
				line,
				...(column !== undefined && { column }),
				code,
				message,
				text,
			});
	};
	for (const { name, sql } of texts) {
		if (name === '') {
			add(name, 1, undefined, 'unnamed', 'a text has no name');
			continue;
		}
		const checked = checkText(sql, declared, options.holes);
		if (checked.kind === 'refused') {
			const { line, column, code, message } = checked.error;
			add(name, line, column, code, message);
			continue;
		}
		for (const problem of checked.problems)
			add(name, problem.line, undefined, problem.code, problem.message);
		for (const diagnostic of checked.analysis.diagnostics)
			if (diagnostic.severity === 'error')
				add(
					name,
					diagnostic.line,
					diagnostic.column,
					diagnostic.code,
					diagnostic.message,
				);
	}
	return [...found.values()].sort(compareProblems);
};
