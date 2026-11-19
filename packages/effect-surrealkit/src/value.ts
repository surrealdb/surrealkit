import { Effect, Schema, SchemaIssue, SchemaParser } from 'effect';
import {
	FileRef,
	type RecordId,
	type DateTime as SdkDateTime,
	type Decimal as SdkDecimal,
	type Duration as SdkDuration,
	type Geometry as SdkGeometry,
	type Range as SdkRange,
	type Uuid as SdkUuid,
	Table,
} from 'surrealdb';

import { Range } from './collections.js';
import { Decimal } from './decimal.js';
import { Geometry } from './geometry.js';
import { isPlainObject } from './internal/declare.js';
import { RecordIdAny } from './record-id.js';
import { Uuid } from './scalars.js';
import { DateTime, Duration } from './temporal.js';

// Check the input prototype before the record codec copies its properties,
// in both directions. Otherwise encoding can erase a class instance.
const plainRecord = <T>(value: Schema.Codec<T>) =>
	Schema.declareConstructor<{ readonly [key: string]: T }>()(
		[Schema.Record(Schema.String, value)],
		([record]) =>
			(input, ast, options) => {
				if (Array.isArray(input) || !isPlainObject(input)) {
					return Effect.fail(
						new SchemaIssue.InvalidType(ast, input, options),
					);
				}
				return SchemaParser.decodeUnknownEffect(record)(input, options);
			},
		{ expected: 'a plain object' },
	);

/**
 * An open native value for FLEXIBLE objects and SCHEMALESS fields.
 * The SDK also accepts a JS `Date` on writes.
 */
export type SurrealValue =
	| null
	| boolean
	| number
	| bigint
	| string
	| SdkDateTime
	| RecordId
	| SdkDuration
	| SdkDecimal
	| SdkUuid
	| SdkGeometry
	| SdkRange<unknown, unknown>
	| Date
	| Table
	| FileRef
	| Uint8Array
	| ReadonlySet<SurrealValue>
	| ReadonlyArray<SurrealValue>
	| { readonly [key: string]: SurrealValue };

const NativeElement = Schema.suspend(() => SurrealValue);

export const SurrealValue: Schema.Codec<SurrealValue> = Schema.Union([
	Schema.Null,
	Schema.Boolean,
	Schema.Number,
	Schema.BigInt,
	Schema.String,
	DateTime,
	RecordIdAny,
	Duration,
	Decimal,
	Uuid,
	Geometry,
	Range,
	Schema.Date,
	Schema.instanceOf(Table),
	Schema.instanceOf(FileRef),
	Schema.instanceOf(Uint8Array),
	Schema.ReadonlySet(NativeElement),
	Schema.Array(NativeElement),
	plainRecord(NativeElement),
]).annotate({ identifier: 'SurrealValue' });

/** An open value in SurrealDB's JSON world (HTTP `/sql`). */
export type SurrealJsonValue =
	| null
	| boolean
	| number
	| string
	| ReadonlyArray<SurrealJsonValue>
	| { readonly [key: string]: SurrealJsonValue };

const JsonElement = Schema.suspend(() => SurrealJsonValue);

export const SurrealJsonValue: Schema.Codec<SurrealJsonValue> = Schema.Union([
	Schema.Null,
	Schema.Boolean,
	Schema.Finite,
	Schema.String,
	Schema.Array(JsonElement),
	plainRecord(JsonElement),
]).annotate({ identifier: 'SurrealJsonValue' });
