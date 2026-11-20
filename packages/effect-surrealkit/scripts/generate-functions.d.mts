/** Types of `generate-functions.mjs`, for the tests that import it. */
export interface Argument {
	readonly name: string;
	readonly kind: string;
	readonly optional?: boolean;
}

export interface Entry {
	readonly name: string;
	readonly constant?: boolean;
	readonly args: ReadonlyArray<Argument>;
	readonly variadic?: boolean;
	readonly result: string;
	readonly engines: ReadonlyArray<string>;
	readonly sample: ReadonlyArray<string>;
	/** More argument sets the engine test calls, beside `sample`. */
	readonly variants?: ReadonlyArray<ReadonlyArray<string>>;
	readonly setup?: string;
}

export interface Table {
	readonly functions: ReadonlyArray<Entry>;
}

export type Kind =
	| { readonly base: string }
	| { readonly array: Kind }
	| { readonly option: Kind };

export type ResultKind =
	| Kind
	| { readonly same: number }
	| { readonly item: number }
	| { readonly items: number }
	| { readonly floatOrDecimal: ReadonlyArray<number> }
	| { readonly shape: string; readonly args: ReadonlyArray<number> };

export const packageRoot: string;
export const tablePath: string;
export const generatedDir: string;
export const majors: ReadonlyArray<string>;
export function parseKind(text: string, where: string): Kind;
export function parseKind(
	text: string,
	where: string,
	result: true,
): ResultKind;
export function codecName(kind: Kind): string;
export function familyOf(name: string): string;
export function exportName(name: string): string;
export function loadTable(path?: string): Table;
export function render(table: Table): Map<string, string>;
export function format(path: string, text: string): string;
export function generate(table?: Table): Map<string, string>;
