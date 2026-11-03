/** Prints schema document v2 kinds as Effect schema expressions. */

import type { PrintContext } from './context.js';
import type { Kind, LiteralKind, Refinement } from './document.js';
import { propertyKey } from './names.js';

const json = (value: string): string => JSON.stringify(value);
const list = (values: ReadonlyArray<string>): string =>
	`[${values.map(json).join(', ')}]`;

const SAFE = Number.MAX_SAFE_INTEGER;

const isSafeIntText = (text: string): boolean => {
	if (!/^-?\d+$/.test(text)) return false;
	const big = BigInt(text);
	return big <= BigInt(SAFE) && big >= -BigInt(SAFE);
};

/**
 * A literal as a plain JS literal when `Schema.Literal` can express it
 * exactly (strings, booleans, floats, ints under the safe-number profile);
 * otherwise `undefined`.
 */
const plainLiteral = (
	literal: LiteralKind,
	ctx: PrintContext,
): string | undefined => {
	switch (literal.type) {
		case 'string':
			return json(String(literal.value));
		case 'bool':
			return String(literal.value);
		case 'float': {
			const value = Number(literal.value);
			return Number.isFinite(value) ? String(value) : undefined;
		}
		case 'int':
			return ctx.profile.int === 'safe-number' &&
				isSafeIntText(String(literal.value))
				? String(BigInt(String(literal.value)))
				: undefined;
		default:
			return undefined;
	}
};

/** The JSON codec a non-plain literal is read through before it is checked. */
const JSON_LITERAL_BASE: Readonly<Record<string, string>> = {
	int: 'SurrealSchema.Int64FromJson',
	decimal: 'SurrealSchema.DecimalFromJson',
	duration: 'SurrealSchema.DurationFromJson',
};

const printLiteral = (literal: LiteralKind, ctx: PrintContext): string => {
	const plain = plainLiteral(literal, ctx);
	if (plain !== undefined) return `Schema.Literal(${plain})`;
	let native: string;
	switch (literal.type) {
		case 'int':
			native = `SurrealSchema.Int64Literal(${json(String(literal.value))})`;
			break;
		case 'decimal':
			native = `SurrealSchema.DecimalLiteral(${json(String(literal.value))})`;
			break;
		case 'duration':
			native = `SurrealSchema.DurationLiteral(${json(String(literal.value))})`;
			break;
		default:
			throw new Error(`unprintable literal ${JSON.stringify(literal)}`);
	}
	return ctx.world === 'json'
		? `${JSON_LITERAL_BASE[literal.type]}.pipe(Schema.decodeTo(${native}))`
		: native;
};

/** A union of literals, as one `Schema.Literals([...])` when possible. */
export const printLiterals = (
	literals: ReadonlyArray<LiteralKind>,
	ctx: PrintContext,
): string => {
	const plain = literals.map((literal) => plainLiteral(literal, ctx));
	if (plain.every((value) => value !== undefined)) {
		return plain.length === 1
			? `Schema.Literal(${plain[0]})`
			: `Schema.Literals([${plain.join(', ')}])`;
	}
	return `Schema.Union([${literals.map((literal) => printLiteral(literal, ctx)).join(', ')}])`;
};

const GEOMETRY_NAMES: Readonly<Record<string, string>> = {
	collection: 'GeometryCollection',
	line: 'GeometryLine',
	multiline: 'GeometryMultiLine',
	multipoint: 'GeometryMultiPoint',
	multipolygon: 'GeometryMultiPolygon',
	point: 'GeometryPoint',
	polygon: 'GeometryPolygon',
};

const unsupported = (kind: string, ctx: PrintContext): string => {
	ctx.scope.warnings.push(
		`${ctx.location}: kind \`${kind}\` has no SDK value representation; generated as SurrealSchema.Unsupported("${kind}"), which rejects every value`,
	);
	return `SurrealSchema.Unsupported(${json(kind)})`;
};

export interface KindHooks {
	/** Printed in place of a bare `object` at the top of the kind (declared children). */
	readonly object?: string;
	/** Printed in place of the item of a top-level `array` / `set` (declared element). */
	readonly item?: string;
}

/** Prints one record kind through the id registry. */
export const printRecord = (
	tables: ReadonlyArray<string>,
	ctx: PrintContext,
): string => {
	if (tables.length === 0) return 'SurrealSchema.RecordIdAny';
	if (tables.length === 1) {
		return ctx.scope.useId(ctx.scope.registry.table(tables[0] as string));
	}
	return ctx.scope.useId(ctx.scope.registry.union(tables));
};

const isLiteral = (kind: Kind): kind is LiteralKind => kind.kind === 'literal';

/**
 * The JSON world's codec for a kind whose SurrealDB JSON rendering differs
 * from its SDK value; `undefined` where the native codec already reads the
 * JSON value (strings, booleans, safe ints, floats, objects) or the kind is
 * structural and recurses through {@link printKind}.
 */
const printJsonKind = (
	kind: Kind,
	ctx: PrintContext,
	hooks: KindHooks,
): string | undefined => {
	switch (kind.kind) {
		case 'none':
			// NONE inside an array or tuple renders as null.
			return 'Schema.Null';
		case 'int':
			return ctx.profile.int === 'bigint'
				? 'SurrealSchema.Int64FromJson'
				: undefined;
		case 'decimal':
			return 'SurrealSchema.DecimalFromJson';
		case 'number':
			return 'SurrealSchema.SurrealNumberFromJson';
		case 'datetime':
			return ctx.profile.datetime === 'effect-datetime'
				? 'SurrealSchema.DateTimeEffectFromJson'
				: 'SurrealSchema.DateTimeFromJson';
		case 'duration':
			return 'SurrealSchema.DurationFromJson';
		case 'uuid':
			return 'SurrealSchema.UuidFromJson';
		case 'bytes':
			return 'SurrealSchema.BytesFromJson';
		case 'range':
			return 'SurrealSchema.RangeFromJson';
		case 'record':
			return `SurrealSchema.RecordIdFromJsonAs(${printRecord(kind.tables, ctx)})`;
		case 'table':
			return `SurrealSchema.TableFromJson(${list(kind.tables)})`;
		case 'file':
			return `SurrealSchema.FileRefFromJson(${list(kind.buckets)})`;
		case 'geometry':
			return `SurrealSchema.GeometryFromJson(${printKind(kind, { ...ctx, world: 'native' })})`;
		case 'set': {
			const item = hooks.item ?? printKind(kind.item, ctx);
			return kind.length === undefined
				? `SurrealSchema.SetFromJson(${item})`
				: `SurrealSchema.SetFromJson(${item}, { length: ${kind.length} })`;
		}
		case 'option':
			// NONE renders as null where it cannot be an absent key.
			return `Schema.NullOr(${printKind(kind.item, ctx)})`;
		case 'union': {
			if (kind.variants.every(isLiteral)) return undefined;
			// NONE and NULL both render as null.
			const variants = [
				...new Set(
					kind.variants.map((variant) =>
						printKind(variant, ctx, hooks),
					),
				),
			];
			const nullIndex = variants.indexOf('Schema.Null');
			if (variants.length === 2 && nullIndex !== -1) {
				return `Schema.NullOr(${variants[1 - nullIndex]})`;
			}
			return variants.length === 1
				? (variants[0] as string)
				: `Schema.Union([${variants.join(', ')}])`;
		}
		default:
			return undefined;
	}
};

/** Prints a kind as an Effect schema expression (single line). */
export const printKind = (
	kind: Kind,
	ctx: PrintContext,
	hooks: KindHooks = {},
): string => {
	if (ctx.world === 'json') {
		const printed = printJsonKind(kind, ctx, hooks);
		if (printed !== undefined) return printed;
	}
	switch (kind.kind) {
		case 'any':
			return 'Schema.Unknown';
		case 'none':
			return 'Schema.Undefined';
		case 'null':
			return 'Schema.Null';
		case 'bool':
			return 'Schema.Boolean';
		case 'int':
			return ctx.profile.int === 'bigint'
				? 'SurrealSchema.Int64'
				: 'SurrealSchema.Int';
		case 'float':
			return 'Schema.Number';
		case 'decimal':
			return 'SurrealSchema.Decimal';
		case 'number':
			return 'SurrealSchema.SurrealNumber';
		case 'string':
			return 'SurrealSchema.String';
		case 'datetime':
			return ctx.profile.datetime === 'effect-datetime'
				? 'SurrealSchema.DateTimeEffect'
				: 'SurrealSchema.DateTime';
		case 'duration':
			return 'SurrealSchema.Duration';
		case 'uuid':
			return 'SurrealSchema.Uuid';
		case 'bytes':
			return 'SurrealSchema.Bytes';
		case 'range':
			return 'SurrealSchema.Range';
		case 'regex':
		case 'function':
			return unsupported(kind.kind, ctx);
		case 'object':
			return (
				hooks.object ??
				(ctx.closedObjects === true
					? 'Schema.Record(Schema.String, Schema.Never)'
					: 'Schema.Record(Schema.String, Schema.Unknown)')
			);
		case 'record':
			return printRecord(kind.tables, ctx);
		case 'table':
			return `SurrealSchema.TableFor(${list(kind.tables)})`;
		case 'file':
			return `SurrealSchema.FileRefFor(${list(kind.buckets)})`;
		case 'geometry': {
			if (kind.types.length === 0) return 'SurrealSchema.Geometry';
			if (kind.types.length === 1) {
				return `SurrealSchema.${GEOMETRY_NAMES[kind.types[0] as string]}`;
			}
			return `SurrealSchema.GeometryOf(${list(kind.types)})`;
		}
		case 'array': {
			const item = hooks.item ?? printKind(kind.item, ctx);
			const array = `Schema.Array(${item})`;
			return kind.length === undefined
				? array
				: `${array}.check(Schema.isBetweenLength(${kind.length}, ${kind.length}))`;
		}
		case 'set': {
			const item = hooks.item ?? printKind(kind.item, ctx);
			return kind.length === undefined
				? `SurrealSchema.SetOf(${item})`
				: `SurrealSchema.SetOf(${item}, { length: ${kind.length} })`;
		}
		case 'tuple':
			return `Schema.Tuple([${kind.items.map((item) => printKind(item, ctx)).join(', ')}])`;
		case 'objectLiteral': {
			if (kind.fields.length === 0) return 'Schema.Struct({})';
			const entries = kind.fields.map((field) => {
				const value = printKind(field.kind, ctx);
				return `${propertyKey(field.name)}: ${field.optional ? `Schema.optionalKey(${value})` : value}`;
			});
			return `Schema.Struct({ ${entries.join(', ')} })`;
		}
		case 'literal':
			return printLiteral(kind, ctx);
		case 'option':
			return `Schema.UndefinedOr(${printKind(kind.item, ctx)})`;
		case 'union': {
			if (kind.variants.every(isLiteral))
				return printLiterals(kind.variants, ctx);
			const variants = kind.variants.map((variant) =>
				printKind(variant, ctx, hooks),
			);
			const nullIndex = kind.variants.findIndex(
				(variant) => variant.kind === 'null',
			);
			if (variants.length === 2 && nullIndex !== -1) {
				return `Schema.NullOr(${variants[1 - nullIndex]})`;
			}
			return `Schema.Union([${variants.join(', ')}])`;
		}
		case 'unknown':
			ctx.scope.warnings.push(
				`${ctx.location}: unrecognised kind \`${kind.source}\` (acknowledged) generated as Schema.Unknown`,
			);
			return 'Schema.Unknown';
	}
};

const literalsMatchKind = (
	values: ReadonlyArray<LiteralKind>,
	kind: Kind,
): boolean => {
	const expected: Readonly<Record<string, LiteralKind['type']>> = {
		string: 'string',
		int: 'int',
		float: 'float',
		decimal: 'decimal',
		bool: 'bool',
		duration: 'duration',
	};
	const type = expected[kind.kind];
	return type !== undefined && values.every((value) => value.type === type);
};

const numberCheck = (
	bound: { readonly value: string; readonly inclusive: boolean },
	side: 'min' | 'max',
	bigint: boolean,
): string | undefined => {
	if (bigint) {
		if (!/^-?\d+$/.test(bound.value)) return undefined;
		const value = `${BigInt(bound.value)}n`;
		if (side === 'min') {
			return bound.inclusive
				? `Schema.isGreaterThanOrEqualToBigInt(${value})`
				: `Schema.isGreaterThanBigInt(${value})`;
		}
		return bound.inclusive
			? `Schema.isLessThanOrEqualToBigInt(${value})`
			: `Schema.isLessThanBigInt(${value})`;
	}
	// An int bound must be exactly representable to be checked client-side.
	if (/^-?\d+$/.test(bound.value) && !isSafeIntText(bound.value))
		return undefined;
	const value = Number(bound.value);
	if (!Number.isFinite(value)) return undefined;
	if (side === 'min') {
		return bound.inclusive
			? `Schema.isGreaterThanOrEqualTo(${value})`
			: `Schema.isGreaterThan(${value})`;
	}
	return bound.inclusive
		? `Schema.isLessThanOrEqualTo(${value})`
		: `Schema.isLessThan(${value})`;
};

const lengthChecks = (
	refinement: Extract<Refinement, { type: 'length' }>,
	strings: boolean,
): ReadonlyArray<string> => {
	const { min, max } = refinement;
	const [atLeast, atMost, between] = strings
		? ['isMinCodePoints', 'isMaxCodePoints', 'isBetweenCodePoints']
		: ['isMinLength', 'isMaxLength', 'isBetweenLength'];
	if (min !== undefined && max !== undefined)
		return [`Schema.${between}(${min}, ${max})`];
	if (min !== undefined) return [`Schema.${atLeast}(${min})`];
	if (max !== undefined) return [`Schema.${atMost}(${max})`];
	return [];
};

/**
 * Applies the recognised assertion refinements a client can check exactly:
 * literal membership (narrowing the type), int/float ranges, and string
 * (Unicode code point) and array lengths. Decimal and duration ranges and
 * `string::is_email` stay server-side: they have no exact client equivalent.
 */
export const applyRefinements = (
	printed: string,
	kind: Kind | undefined,
	refinements: ReadonlyArray<Refinement> | undefined,
	ctx: PrintContext,
): string => {
	if (kind === undefined || refinements === undefined) return printed;
	let base = printed;
	const checks: string[] = [];
	for (const refinement of refinements) {
		switch (refinement.type) {
			case 'literals':
				if (literalsMatchKind(refinement.values, kind)) {
					base = printLiterals(refinement.values, ctx);
				}
				break;
			case 'range': {
				const bigint =
					kind.kind === 'int' && ctx.profile.int === 'bigint';
				const applies =
					(refinement.valueKind === 'int' && kind.kind === 'int') ||
					(refinement.valueKind === 'float' && kind.kind === 'float');
				if (!applies) break;
				for (const side of ['min', 'max'] as const) {
					const bound = refinement[side];
					if (bound === undefined) continue;
					const check = numberCheck(bound, side, bigint);
					if (check !== undefined) checks.push(check);
				}
				break;
			}
			case 'length':
				if (refinement.target === 'string' && kind.kind === 'string') {
					checks.push(...lengthChecks(refinement, true));
				} else if (
					refinement.target === 'array' &&
					kind.kind === 'array'
				) {
					checks.push(...lengthChecks(refinement, false));
				}
				break;
			case 'email':
				break;
		}
	}
	return checks.length === 0 ? base : `${base}.check(${checks.join(', ')})`;
};
