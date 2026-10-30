/**
 * Effect v4 schemas for SurrealDB values as surrealdb.js 2.x decodes and
 * encodes them (the native codec world), plus explicitly named JSON codecs.
 *
 * Every export imports only `effect` and `surrealdb` value classes, so it is
 * safe in browsers, Workers, Bun and Node.
 */
export {
	Range,
	type RangeBound,
	type SetEquality,
	SetOf,
	type SetOptions,
} from './collections.js';
export {
	DECIMAL_MAX_MANTISSA,
	DECIMAL_MAX_SCALE,
	Decimal,
	DecimalLiteral,
	decimalFromString,
	SurrealNumber,
} from './decimal.js';
export {
	Geometry,
	GeometryCollection,
	type GeometryKind,
	GeometryLine,
	GeometryMultiLine,
	GeometryMultiPoint,
	GeometryMultiPolygon,
	GeometryOf,
	GeometryPoint,
	GeometryPolygon,
} from './geometry.js';
export {
	INT64_MAX,
	INT64_MIN,
	Int,
	Int64,
	Int64Literal,
	type IntProfile,
	int64,
	intFor,
} from './int.js';
export {
	BytesFromJson,
	type BytesJsonEncoded,
	DateTimeEffectFromJson,
	DateTimeFromJson,
	type DateTimeJsonEncoded,
	DecimalFromJson,
	type DecimalJsonEncoded,
	DurationFromJson,
	type DurationJsonEncoded,
	FileRefFromJson,
	GeometryFromJson,
	Int64FromJson,
	type Int64JsonEncoded,
	RangeFromJson,
	RecordIdAnyFromJson,
	RecordIdFromJson,
	RecordIdFromJsonAs,
	SetFromJson,
	SurrealNumberFromJson,
	TableFromJson,
	UuidFromJson,
	type UuidJsonEncoded,
} from './json.js';
export {
	FileRefFor,
	RecordIdAny,
	RecordIdFor,
	type RecordIdOf,
	RecordKey,
	TableFor,
	type TaggedRecordId,
	taggedRecordId,
} from './record-id.js';
export { NoneAsAbsent, stripNone } from './row.js';
export {
	Bytes,
	SurrealString as String,
	toWellFormed,
	Unsupported,
	Uuid,
} from './scalars.js';
export {
	type EdgeDescriptor,
	type EdgeIn,
	type EdgeOut,
	edge,
	type FieldPresence,
	type FunctionArgs,
	type FunctionDescriptor,
	type FunctionResult,
	fn,
	type Narrowing,
	type Operation,
	type Permission,
	type Presence,
	type Refinements,
	type Replaced,
	type RowSchema,
	refine,
	replaceField,
	type TableDescriptor,
	type TablePermissions,
	table,
	type ViewDescriptor,
	view,
	withPresence,
} from './table.js';
export {
	DATETIME_MAX_SECONDS,
	DATETIME_MIN_SECONDS,
	DateTime,
	DateTimeEffect,
	DURATION_MAX_SECONDS,
	Duration,
	DurationLiteral,
	dateTimeFromString,
	dateTimeToString,
} from './temporal.js';
