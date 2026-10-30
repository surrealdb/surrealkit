/**
 * Lowers one schema document table to its per-operation contract (the
 * field-presence matrix, schema document v2 section 7) and prints its module.
 */
import type { FieldPresence, Operation, Presence } from '../table.js';
import {
	admitsNoneKind,
	compare,
	type ModuleScope,
	type PrintContext,
	profileFor,
} from './context.js';
import type {
	Field,
	Kind,
	Refinement,
	Table,
	TablePermissions,
} from './document.js';
import { applyRefinements, printKind, printRecord } from './kinds.js';
import { propertyAccess, propertyKey } from './names.js';
import type { EmitterOptions } from './options.js';

export const OPERATIONS: ReadonlyArray<Operation> = [
	'select',
	'create',
	'merge',
	'content',
];

interface FieldNode {
	readonly key: string;
	readonly path: ReadonlyArray<string>;
	field: Field | undefined;
	readonly children: Map<string, FieldNode>;
}

const node = (path: ReadonlyArray<string>): FieldNode => ({
	key: path[path.length - 1] ?? '',
	path,
	field: undefined,
	children: new Map(),
});

/** Builds the field tree; parents the engine accepted implicitly get nodes. */
const buildTree = (fields: ReadonlyArray<Field>): FieldNode => {
	const root = node([]);
	for (const field of fields) {
		let current = root;
		for (let depth = 0; depth < field.path.length; depth++) {
			const key = field.path[depth] as string;
			let child = current.children.get(key);
			if (child === undefined) {
				child = node(field.path.slice(0, depth + 1));
				current.children.set(key, child);
			}
			current = child;
		}
		current.field = field;
	}
	return root;
};

interface Info {
	readonly kind: Kind | undefined;
	/**
	 * A field declared without TYPE whose children are declared: the engine
	 * holds it like an implicit parent, never NONE once a child is required.
	 */
	readonly untypedParent: boolean;
	/** At least one named child is required. */
	readonly childRequired: boolean;
	readonly optional: boolean;
	readonly flexible: boolean;
	readonly readonly: boolean;
	readonly defaultMode: 'create' | 'always' | undefined;
	readonly valueIgnoresInput: boolean;
	readonly computed: boolean;
	readonly recognized: ReadonlyArray<Refinement> | undefined;
}

const sortedChildren = (parent: FieldNode): FieldNode[] =>
	[...parent.children.values()].sort((a, b) => compare(a.key, b.key));

const namedChildren = (parent: FieldNode): FieldNode[] =>
	sortedChildren(parent).filter((child) => child.key !== '*');

/**
 * Field facts, including for an implicit parent (contract section 6): the
 * engine materialises it as an empty object on every write, so it behaves
 * like `TYPE object DEFAULT ALWAYS {}` — always selected, optional and
 * resettable unless a child is required, in which case it must be written.
 */
const infoOf = (fieldNode: FieldNode): Info => {
	const field = fieldNode.field;
	const named = namedChildren(fieldNode);
	const childRequired = named.some((child) => !admitsNone(infoOf(child)));
	if (field === undefined) {
		return {
			kind: { kind: 'object' },
			untypedParent: false,
			childRequired,
			optional: false,
			flexible: false,
			readonly: false,
			defaultMode: childRequired ? undefined : 'always',
			valueIgnoresInput: false,
			computed: false,
			recognized: undefined,
		};
	}
	// Without TYPE but with children: the implicit-parent rules (contract
	// section 7, "Parents without TYPE").
	const untypedParent = field.kind === undefined && named.length > 0;
	return {
		kind: field.kind,
		untypedParent,
		childRequired,
		optional: field.optional,
		flexible: field.flexible,
		readonly: field.readonly,
		defaultMode:
			field.default?.mode ??
			(untypedParent && !childRequired ? 'always' : undefined),
		valueIgnoresInput: field.value !== undefined && !field.value.readsInput,
		computed: field.computed !== undefined,
		recognized: field.assert?.recognized,
	};
};

/**
 * `option<T>`, `any`, or no TYPE: the field may hold NONE. A parent without
 * TYPE is materialised by the engine instead, like an implicit parent.
 */
const admitsNone = (info: Info): boolean =>
	info.optional || (!info.untypedParent && admitsNoneKind(info.kind));

/** Contract section 7, first match wins within each operation. */
const derivePresence = (
	info: Info,
	operation: Operation,
	writeExcluded: boolean,
): Presence => {
	const none = admitsNone(info);
	switch (operation) {
		case 'select':
			return none ? 'optional' : 'required';
		case 'create':
			if (writeExcluded || info.computed) return 'excluded';
			return none ||
				info.defaultMode !== undefined ||
				info.valueIgnoresInput
				? 'optional'
				: 'required';
		case 'merge':
			if (writeExcluded || info.computed || info.readonly)
				return 'excluded';
			// Before `none`: NONE re-applies a DEFAULT ALWAYS, even on option<T>.
			if (info.defaultMode === 'always') return 'resettable';
			return none ? 'removable' : 'optional';
		case 'content':
			if (writeExcluded || info.computed || info.readonly)
				return 'excluded';
			return none ||
				info.defaultMode === 'always' ||
				info.valueIgnoresInput
				? 'optional'
				: 'required';
	}
};

const wrap = (code: string, presence: Presence): string => {
	switch (presence) {
		case 'required':
			return code;
		case 'optional':
			return `Schema.optionalKey(${code})`;
		case 'removable':
		case 'resettable':
			return `Schema.optional(${code})`;
		case 'excluded':
			throw new Error('excluded fields are not printed');
	}
};

const tabs = (indent: number): string => '\t'.repeat(indent);

const OPEN_RECORD = 'Schema.Record(Schema.String, Schema.Unknown)';

/** A struct literal over printed entries, multi-line. */
const struct = (
	entries: ReadonlyArray<readonly [string, string]>,
	indent: number,
	open: boolean,
): string => {
	const body =
		entries.length === 0
			? 'Schema.Struct({})'
			: `Schema.Struct({\n${entries
					.map(
						([key, value]) =>
							`${tabs(indent + 1)}${propertyKey(key)}: ${value},`,
					)
					.join('\n')}\n${tabs(indent)}})`;
	return open ? `Schema.StructWithRest(${body}, [${OPEN_RECORD}])` : body;
};

class TablePrinter {
	readonly root: FieldNode;
	readonly isRelation: boolean;
	readonly lightweight: boolean;

	constructor(
		readonly table: Table,
		readonly scope: ModuleScope,
		readonly options: EmitterOptions,
		readonly world: 'native' | 'json' = 'native',
	) {
		this.root = buildTree(table.fields);
		this.isRelation = table.type.kind === 'relation';
		this.lightweight =
			table.type.kind === 'relation' && table.type.lightweight;
	}

	location(path: ReadonlyArray<string>): string {
		return `${this.table.name}.${path.join('.')}`;
	}

	context(path: ReadonlyArray<string>): PrintContext {
		const key = this.location(path);
		return {
			scope: this.scope,
			profile: profileFor(this.options, key),
			location: key,
			...(this.world === 'json' && { world: 'json' as const }),
		};
	}

	presence(
		fieldNode: FieldNode,
		operation: Operation,
		element: boolean,
	): Presence {
		const override =
			this.options.fields?.[this.location(fieldNode.path)]?.[operation];
		if (override !== undefined) return override;
		// Elements are written whole, never merged: their children follow Create.
		const effective =
			element && operation !== 'select' ? 'create' : operation;
		const writeExcluded = this.lightweight || this.table.view !== undefined;
		return derivePresence(infoOf(fieldNode), effective, writeExcluded);
	}

	/** The schema of one field in one operation, before its own presence. */
	field(
		fieldNode: FieldNode,
		operation: Operation,
		indent: number,
		element: boolean,
	): string {
		const info = infoOf(fieldNode);
		// A SCHEMAFULL table refuses every undeclared key of an object field that
		// is not FLEXIBLE itself (a FLEXIBLE parent does not open a declared
		// child), so such an object without declared children holds only `{}`.
		const ctx: PrintContext = {
			...this.context(fieldNode.path),
			...(this.table.schemafull &&
				!info.flexible && { closedObjects: true }),
		};
		const named = namedChildren(fieldNode);
		const elementNode = fieldNode.children.get('*');
		const object =
			named.length === 0
				? undefined
				: this.object(named, operation, indent, element, info.flexible);
		const item =
			elementNode !== undefined && elementNode.children.size > 0
				? this.field(elementNode, operation, indent, true)
				: undefined;
		if (info.kind === undefined) {
			// A required child makes the engine insist on an object closed over
			// the children; otherwise any value is accepted.
			return info.untypedParent &&
				info.childRequired &&
				object !== undefined
				? object
				: 'Schema.Unknown';
		}
		const printed = printKind(info.kind, ctx, {
			...(object !== undefined && { object }),
			...(item !== undefined && { item }),
		});
		return applyRefinements(printed, info.kind, info.recognized, ctx);
	}

	object(
		children: ReadonlyArray<FieldNode>,
		operation: Operation,
		indent: number,
		element: boolean,
		open: boolean,
	): string {
		const entries: Array<readonly [string, string]> = [];
		for (const child of children) {
			const presence = this.presence(child, operation, element);
			if (presence === 'excluded') continue;
			entries.push([
				child.key,
				wrap(
					this.field(child, operation, indent + 1, element),
					presence,
				),
			]);
		}
		return struct(entries, indent, open);
	}

	topLevel(): FieldNode[] {
		const reserved = this.isRelation ? ['in', 'out'] : [];
		return namedChildren(this.root).filter(
			(child) => !reserved.includes(child.key),
		);
	}
}

export interface TableModule {
	readonly file: string;
	readonly content: string;
}

const json = (value: unknown): string => JSON.stringify(value);
const jsonList = (values: ReadonlyArray<string>): string =>
	`[${values.map((value) => JSON.stringify(value)).join(', ')}]`;

const printPermissions = (permissions: TablePermissions): string =>
	`{ select: ${json(permissions.select)}, create: ${json(permissions.create)}, update: ${json(permissions.update)}, delete: ${json(permissions.delete)} }`;

const presenceLiteral = (presence: FieldPresence): string =>
	`{ select: ${json(presence.select)}, create: ${json(presence.create)}, merge: ${json(presence.merge)}, content: ${json(presence.content)} }`;

/**
 * Why a field's schema is not the same in every operation. In practice this is
 * an object with declared children: `UPDATE … MERGE` merges nested objects, so
 * the Merge variant makes the children partial.
 */
const varianceReason = (
	perCode: Map<Operation, string> | undefined,
): string => {
	const merge = perCode?.get('merge');
	const others = [...(perCode?.entries() ?? [])]
		.filter(([operation]) => operation !== 'merge')
		.map(([, code]) => code);
	const othersAgree = others.every((code) => code === others[0]);
	return merge !== undefined && othersAgree
		? 'its children are partial in Merge'
		: 'its children differ by operation';
};

const jsonPointer = (key: string): string =>
	`/${key.replace(/~/g, '~0').replace(/\//g, '~1')}`;

export interface TableInput {
	readonly table: Table;
	readonly scope: ModuleScope;
	readonly options: EmitterOptions;
	readonly digest: string;
	/** Relation tables using this table as `in` / `out`. */
	readonly relations: {
		readonly in: ReadonlyArray<string>;
		readonly out: ReadonlyArray<string>;
	};
}

const VARIANT_DOCS: Readonly<Record<Operation, (name: string) => string>> = {
	select: (name) =>
		`A stored \`${name}\` row as a privileged reader selects it (all fields).`,
	create: (name) =>
		`Input for \`CREATE ${name} CONTENT\` / \`INSERT\`. The record id is an operation argument.`,
	merge: (name) =>
		`Input for \`UPDATE ${name} MERGE\`: omitting a field leaves it unchanged; \`undefined\` (NONE) removes a removable field and re-applies a resettable one's default.`,
	content: (name) =>
		`Input for \`UPDATE ${name} CONTENT\`: a full replacement of the writable fields.`,
};

const SUFFIX: Readonly<Record<Operation, string>> = {
	select: '',
	create: 'Create',
	merge: 'Merge',
	content: 'Content',
};

/** Prints the module of one table, view or relation. */
export const printTable = (input: TableInput): string => {
	const { table, scope, options, digest } = input;
	const registry = scope.registry;
	const pascal = registry.pascal(table.name);
	const idName = scope.useId(registry.table(table.name));
	const lines: string[] = [];
	const exportType = (name: string) => {
		lines.push(`export type ${name} = typeof ${name}.Type;`);
		lines.push(`export type ${name}Encoded = typeof ${name}.Encoded;`);
	};

	if (table.view !== undefined) {
		lines.push(
			`/** \`${table.name}\` is a view: its row shape comes from its query and is not derived. */`,
			`export const ${pascal}Table = SurrealSchema.view({`,
			`\tname: ${json(table.name)},`,
			`\ttype: "view",`,
			`\tid: ${idName},`,
			`\tview: ${json(table.view.select)},`,
			`\tpermissions: ${printPermissions(table.permissions)},`,
			`\tdigest: ${json(digest)},`,
			'});',
		);
		return lines.join('\n');
	}

	const printer = new TablePrinter(table, scope, options);
	const fields = printer.topLevel();
	const operations: ReadonlyArray<Operation> = printer.lightweight
		? ['select']
		: OPERATIONS;

	// Per field: its presence and its schema in every operation that includes it.
	const presence = new Map<string, Record<Operation, Presence>>();
	const codes = new Map<string, Map<Operation, string>>();
	for (const field of fields) {
		const perOperation = {} as Record<Operation, Presence>;
		const perCode = new Map<Operation, string>();
		for (const operation of OPERATIONS) {
			const p = operations.includes(operation)
				? printer.presence(field, operation, false)
				: 'excluded';
			perOperation[operation] = p;
			if (p !== 'excluded')
				perCode.set(
					operation,
					printer.field(field, operation, 1, false),
				);
		}
		presence.set(field.key, perOperation);
		codes.set(field.key, perCode);
	}
	const shared = fields.filter((field) => {
		const perCode = [...(codes.get(field.key)?.values() ?? [])];
		return (
			perCode.length > 0 && perCode.every((code) => code === perCode[0])
		);
	});
	const sharedKeys = new Set(shared.map((field) => field.key));
	const fieldsName = `${pascal}Fields`;
	const variant = fields
		.filter((field) => !sharedKeys.has(field.key))
		.map(
			(field) =>
				`\`${field.key}\` (${varianceReason(codes.get(field.key))})`,
		);
	const doc = (summary: string): string[] =>
		variant.length === 0
			? [`/** ${summary} */`]
			: [
					'/**',
					` * ${summary}`,
					' *',
					` * Not listed, because their schema differs by operation: ${variant.join(', ')}.`,
					' * Narrow such a field in the variant schemas that use it.',
					' */',
				];

	if (shared.length > 0) {
		lines.push(
			...doc(
				`Field schemas of \`${table.name}\` that are the same in every operation using them; \`refine\` and \`replaceField\` accept these keys.`,
			),
			`export const ${fieldsName} = {`,
			...shared.map(
				(field) =>
					`\t${propertyKey(field.key)}: ${codes.get(field.key)?.values().next().value},`,
			),
			'} as const;',
			'',
		);
	} else {
		lines.push(
			...doc(
				`No field of \`${table.name}\` is shared by every operation.`,
			),
			`export const ${fieldsName} = {} as const;`,
			'',
		);
	}

	const endpoint = (tables: ReadonlyArray<string>) =>
		printRecord(tables, {
			scope,
			profile: profileFor(options, table.name),
			location: table.name,
		});
	const relationType =
		table.type.kind === 'relation' ? table.type : undefined;
	const open = !table.schemafull;

	for (const operation of operations) {
		const name = `${pascal}${SUFFIX[operation]}`;
		const entries: Array<readonly [string, string]> = [];
		if (operation === 'select') {
			entries.push(['id', idName]);
			if (relationType !== undefined) {
				entries.push(
					['in', endpoint(relationType.in)],
					['out', endpoint(relationType.out)],
				);
			}
		}
		for (const field of fields) {
			const p = presence.get(field.key)?.[operation] ?? 'excluded';
			if (p === 'excluded') continue;
			const code = sharedKeys.has(field.key)
				? `${fieldsName}${propertyAccess(field.key)}`
				: (codes.get(field.key)?.get(operation) as string);
			entries.push([field.key, wrap(code, p)]);
		}
		lines.push(`/** ${VARIANT_DOCS[operation](table.name)} */`);
		lines.push(`export const ${name} = ${struct(entries, 0, open)};`);
		exportType(name);
		lines.push('');
		if (operation === 'select') {
			// The struct stays plain so that `refine` can reach its fields; this is
			// the schema to decode with.
			lines.push(
				`/** \`${name}\` for rows the SDK returned: NONE (an \`undefined\` property) is removed first, so an optional field decodes as absent. */`,
				`export const ${name}Row = SurrealSchema.NoneAsAbsent(${name});`,
			);
			exportType(`${name}Row`);
			lines.push('');
		}
	}

	if (options.patch === true && !printer.lightweight) {
		const ops: string[] = [];
		for (const field of fields) {
			const p = presence.get(field.key)?.merge ?? 'excluded';
			if (p === 'excluded') continue;
			const value = sharedKeys.has(field.key)
				? `${fieldsName}${propertyAccess(field.key)}`
				: (codes.get(field.key)?.get('merge') as string);
			const path = json(jsonPointer(field.key));
			ops.push(
				`\tSchema.Struct({ op: Schema.Literals(["add", "replace"]), path: Schema.Literal(${path}), value: ${value} }),`,
			);
			if (p === 'removable' || p === 'resettable') {
				ops.push(
					`\tSchema.Struct({ op: Schema.Literal("remove"), path: Schema.Literal(${path}) }),`,
				);
			}
		}
		if (ops.length > 0) {
			lines.push(
				`/** JSON Patch operations on \`${table.name}\` (generated behind the \`patch\` flag; not yet qualified against an engine). */`,
				`export const ${pascal}Patch = Schema.Union([`,
				...ops,
				']);',
			);
			exportType(`${pascal}Patch`);
			lines.push('');
		}
	}

	if (relationType !== undefined) {
		const ends = (tables: ReadonlyArray<string>) =>
			tables.length === 0
				? '[SurrealSchema.RecordIdAny]'
				: `[${tables.map((t) => scope.useId(registry.table(t))).join(', ')}]`;
		lines.push(
			`/** Endpoints of the \`${table.name}\` relation, for typed \`RELATE\` arguments. */`,
			`export const ${pascal}Edge = SurrealSchema.edge({`,
			`\ttable: ${json(table.name)},`,
			`\tin: ${ends(relationType.in)},`,
			`\tout: ${ends(relationType.out)},`,
			`\tenforced: ${relationType.enforced},`,
			`\tlightweight: ${relationType.lightweight},`,
			'});',
			'',
		);
	}

	const presenceEntries: Array<readonly [string, FieldPresence]> = [
		[
			'id',
			{
				select: 'required',
				create: 'excluded',
				merge: 'excluded',
				content: 'excluded',
			},
		],
	];
	if (relationType !== undefined) {
		presenceEntries.push(
			[
				'in',
				{
					select: 'required',
					create: 'excluded',
					merge: 'excluded',
					content: 'excluded',
				},
			],
			[
				'out',
				{
					select: 'required',
					create: 'excluded',
					merge: 'excluded',
					content: 'excluded',
				},
			],
		);
	}
	for (const field of fields) {
		presenceEntries.push([
			field.key,
			presence.get(field.key) as FieldPresence,
		]);
	}

	lines.push(
		`/** Descriptor of \`${table.name}\`${table.comment === undefined ? '' : `: ${table.comment.replace(/\*\//g, '* /')}`}. */`,
		`export const ${pascal}Table = SurrealSchema.table({`,
		`\tname: ${json(table.name)},`,
		`\ttype: ${json(table.type.kind)},`,
		`\tschemafull: ${table.schemafull},`,
		`\tdrop: ${table.drop},`,
		`\tid: ${idName},`,
		...operations.map(
			(operation) => `\t${operation}: ${pascal}${SUFFIX[operation]},`,
		),
		`\tfields: ${fieldsName},`,
		'\tpresence: {',
		...presenceEntries.map(
			([key, p]) => `\t\t${propertyKey(key)}: ${presenceLiteral(p)},`,
		),
		'\t},',
		`\trelations: { in: ${jsonList(input.relations.in)}, out: ${jsonList(input.relations.out)} },`,
		`\tpermissions: ${printPermissions(table.permissions)},`,
		...(relationType === undefined ? [] : [`\tedge: ${pascal}Edge,`]),
		`\tdigest: ${json(digest)},`,
		'});',
	);
	lines.push('', ...printJsonWorld(input, fields, endpoint, relationType));
	return lines.join('\n');
};

/**
 * The JSON codec world of a selected row (`<Table>JsonFields`,
 * `<Table>FromJson`, `<Table>JsonEncoded`): what SurrealDB's HTTP `/sql`
 * renders, decoded into the same types as the native world. Never the
 * default, and printed after the native exports so they are unchanged.
 */
const printJsonWorld = (
	input: TableInput,
	fields: ReadonlyArray<FieldNode>,
	endpoint: (tables: ReadonlyArray<string>) => string,
	relationType: Extract<Table['type'], { kind: 'relation' }> | undefined,
): string[] => {
	const { table, scope, options } = input;
	const pascal = scope.registry.pascal(table.name);
	const idName = scope.useId(scope.registry.table(table.name));
	const printer = new TablePrinter(table, scope, options, 'json');
	const jsonFields = `${pascal}JsonFields`;
	const selected = fields
		.map(
			(field) =>
				[field, printer.presence(field, 'select', false)] as const,
		)
		.filter(([, presence]) => presence !== 'excluded');
	const fromJson = (code: string) =>
		`SurrealSchema.RecordIdFromJsonAs(${code})`;
	const entries: Array<readonly [string, string]> = [
		['id', fromJson(idName)],
	];
	if (relationType !== undefined) {
		entries.push(
			['in', fromJson(endpoint(relationType.in))],
			['out', fromJson(endpoint(relationType.out))],
		);
	}
	for (const [field, presence] of selected) {
		entries.push([
			field.key,
			wrap(`${jsonFields}${propertyAccess(field.key)}`, presence),
		]);
	}
	return [
		`/** JSON world: the field schemas of a selected \`${table.name}\` row as SurrealDB's HTTP \`/sql\` renders it (lossy; see the effect-surrealkit README), decoding to the native types. */`,
		`export const ${jsonFields} = {`,
		...selected.map(
			([field]) =>
				`\t${propertyKey(field.key)}: ${printer.field(field, 'select', 1, false)},`,
		),
		'} as const;',
		'',
		`/** A selected \`${table.name}\` row from SurrealDB JSON. Not the default: rows the SDK returned decode with \`${pascal}Row\`. */`,
		`export const ${pascal}FromJson = ${struct(entries, 0, !table.schemafull)};`,
		`export type ${pascal}JsonEncoded = typeof ${pascal}FromJson.Encoded;`,
	];
};
