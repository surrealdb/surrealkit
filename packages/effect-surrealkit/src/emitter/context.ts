/** State shared while printing one generated module set. */
import type { Kind, SchemaDocument } from './document.js';
import { kebabCase, pascalCase } from './names.js';
import type { DatetimeProfile, EmitterOptions, IntProfile } from './options.js';

export interface Profile {
	readonly int: IntProfile;
	readonly datetime: DatetimeProfile;
}

/** Record-id schemas, all printed into `ids.ts` and imported from there. */
export class IdRegistry {
	readonly #names = new Map<string, string>();
	readonly #unions = new Map<string, ReadonlyArray<string>>();
	readonly #conflicts: Array<
		readonly [string, ReadonlyArray<string>, ReadonlyArray<string>]
	> = [];
	readonly #defined: ReadonlySet<string>;

	constructor(
		readonly document: SchemaDocument,
		private readonly overrides: Readonly<Record<string, string>>,
	) {
		this.#defined = new Set(document.tables.map((table) => table.name));
		for (const table of document.tables) this.table(table.name);
	}

	/** PascalCase base name of a table (`member_of` → `MemberOf`). */
	pascal(table: string): string {
		return this.overrides[table] ?? pascalCase(table);
	}

	/** Module file name of a table, without `.ts` (`member_of` → `member-of`). */
	file(table: string): string {
		const override = this.overrides[table];
		return kebabCase(override ?? table);
	}

	/** The id schema name of one table, registering it. */
	table(table: string): string {
		const existing = this.#names.get(table);
		if (existing !== undefined) return existing;
		const name = `${this.pascal(table)}Id`;
		this.#names.set(table, name);
		return name;
	}

	/**
	 * The id-union schema name for `record<a | b>`, registering it. Two different
	 * unions can spell the same name (`record<a_or_b | c>` and `record<a | b_or_c>`
	 * are both `AOrBOrCId`); the second is recorded in {@link conflicts}, never
	 * written over the first.
	 */
	union(tables: ReadonlyArray<string>): string {
		for (const table of tables) this.table(table);
		const name = `${tables.map((table) => this.pascal(table)).join('Or')}Id`;
		const existing = this.#unions.get(name);
		if (existing === undefined) {
			this.#unions.set(name, tables);
		} else if (
			existing.length !== tables.length ||
			existing.some((table, i) => table !== tables[i])
		) {
			this.#conflicts.push([name, existing, tables]);
		}
		return name;
	}

	/** Names two different unions spell alike: the name, the first union, the other. */
	get conflicts(): ReadonlyArray<
		readonly [string, ReadonlyArray<string>, ReadonlyArray<string>]
	> {
		return this.#conflicts;
	}

	/** The tagged application representation of a registered union. */
	static tagged(unionName: string): string {
		return `${unionName.slice(0, -2)}Tagged`;
	}

	get tables(): ReadonlyArray<readonly [string, string]> {
		return [...this.#names.entries()].sort(([a], [b]) => compare(a, b));
	}

	get unions(): ReadonlyArray<readonly [string, ReadonlyArray<string>]> {
		return [...this.#unions.entries()].sort(([a], [b]) => compare(a, b));
	}

	isDefined(table: string): boolean {
		return this.#defined.has(table);
	}
}

/** Byte-order comparison of the UTF-8 encodings (contract section 12). */
export const compare = (a: string, b: string): number => {
	const left = new TextEncoder().encode(a);
	const right = new TextEncoder().encode(b);
	const length = Math.min(left.length, right.length);
	for (let i = 0; i < length; i++) {
		const difference = (left[i] as number) - (right[i] as number);
		if (difference !== 0) return difference;
	}
	return left.length - right.length;
};

/** Collects the identifiers a module imports and the warnings it raises. */
export class ModuleScope {
	readonly ids = new Set<string>();

	constructor(
		readonly registry: IdRegistry,
		readonly warnings: string[],
	) {}

	useId(name: string): string {
		this.ids.add(name);
		return name;
	}
}

export interface PrintContext {
	readonly scope: ModuleScope;
	readonly profile: Profile;
	/** Where the kind sits, for warnings: `person.address.city`. */
	readonly location: string;
	/**
	 * Which codec world to print: SDK values (`"native"`, the default) or the
	 * lossy JSON SurrealDB's HTTP `/sql` renders (`"json"`).
	 */
	readonly world?: 'native' | 'json';
	/**
	 * A bare `object` here admits no keys: on a SCHEMAFULL table the engine
	 * refuses every undeclared key of an object that is not FLEXIBLE.
	 */
	readonly closedObjects?: boolean;
}

export const profileFor = (options: EmitterOptions, key: string): Profile => {
	const field = options.fields?.[key];
	return {
		int: field?.int ?? options.int ?? 'safe-number',
		datetime: field?.datetime ?? options.datetime ?? 'sdk',
	};
};

/** True when a kind admits NONE on its own (`any`, or no TYPE at all). */
export const admitsNoneKind = (kind: Kind | undefined): boolean =>
	kind === undefined || kind.kind === 'any' || kind.kind === 'none';
