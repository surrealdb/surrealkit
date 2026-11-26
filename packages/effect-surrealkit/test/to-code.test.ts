import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Codegen, Schema, SchemaRepresentation } from 'effect';
import { describe, expect, it } from 'vitest';

import * as S from '../src/index.js';

const print = (schema: Schema.Top) => {
	const { representation, references } =
		SchemaRepresentation.toRepresentation(schema.ast);
	const generator = new Codegen.Module();
	const root = generator
		.addSchemas({ GeneratedSchema: representation }, { references })
		.get('GeneratedSchema');
	const { imports, declarations } = generator.finalize();
	const [type, runtime] =
		declarations.find(({ binding }) => binding === root)?.fragments ?? [];
	if (root === undefined || type === undefined || runtime === undefined) {
		throw new Error('Missing generated schema declaration');
	}
	return {
		runtime: runtime.slice(`export const ${root.name} = `.length),
		Type: type.slice(`export type ${root.name} = `.length),
		imports,
		declarations,
	};
};

describe('toCode metadata', () => {
	it('every codec prints itself by name', () => {
		const row = Schema.Struct({
			id: S.RecordIdFor('person'),
			key: S.RecordIdFor('id_object', {
				key: Schema.Struct({ n: S.Int }),
			}),
			count: S.Int,
			big: S.Int64,
			at: S.DateTime,
			atMs: S.DateTimeEffect,
			span: S.Duration,
			price: S.Decimal,
			num: S.SurrealNumber,
			uuid: S.Uuid,
			bytes: S.Bytes,
			text: S.String,
			table: S.TableFor(['a', 'b']),
			file: S.FileRefFor(['avatars']),
			point: S.GeometryPoint,
			shape: S.GeometryOf(['line', 'polygon']),
			range: S.Range,
			tags: S.SetOf(S.String, { length: 2 }),
			owner: S.taggedRecordId({
				org: S.RecordIdFor('org'),
				team: S.RecordIdFor('team'),
			}),
			ten: S.DecimalLiteral('10'),
			day: S.DurationLiteral('1d'),
			minus: S.Int64Literal('-5'),
			regex: S.Unsupported('regex'),
			any: S.RecordIdAny,
			jsonAt: S.DateTimeFromJson,
		});
		const code = print(S.NoneAsAbsent(row));
		expect(code.runtime).toMatchSnapshot();
		expect(code.Type).toMatchSnapshot();
		expect(code.imports).toContain(
			'import * as SurrealSchema from "effect-surrealkit"',
		);
	});

	it('preserves shared references and imports in nested codec source', () => {
		const account = S.RecordIdFor('account').annotate({
			identifier: 'AccountId',
		});
		const point = S.GeometryPoint.annotate({ identifier: 'Position' });
		const code = print(
			Schema.Struct({
				owner: S.taggedRecordId({ account }),
				id: S.RecordIdFromJsonAs(account),
				point: S.GeometryFromJson(point),
				seenAt: S.SetFromJson(S.DateTimeEffectFromJson, { length: 2 }),
			}),
		);
		expect(code.runtime).toContain(
			'SurrealSchema.taggedRecordId({ "account": AccountId })',
		);
		expect(code.runtime).toContain(
			'SurrealSchema.RecordIdFromJsonAs(AccountId)',
		);
		expect(code.runtime).toContain(
			'SurrealSchema.GeometryFromJson(Position)',
		);
		expect(code.runtime).toContain(
			'SurrealSchema.SetFromJson(SurrealSchema.DateTimeEffectFromJson.annotate({ "expected": "an RFC 3339 datetime string" }), { length: 2 })',
		);
		expect(code.Type).toContain('ReadonlySet<DateTime.Utc>');
		expect(code.declarations.flatMap(({ fragments }) => fragments)).toEqual(
			expect.arrayContaining([
				'export type AccountId = Surreal.RecordId<"account">',
				'export type Position = Surreal.GeometryPoint',
			]),
		);
		expect(code.imports).toContain(
			'import type { DateTime } from "effect"',
		);
	});
});

describe('browser and Worker safety', () => {
	it('runtime modules import only effect, surrealdb and each other', () => {
		const sources = join(import.meta.dirname, '..', 'src');
		const runtime = [
			...readdirSync(sources).filter((file) => file.endsWith('.ts')),
			...readdirSync(join(sources, 'internal')).map(
				(file) => `internal/${file}`,
			),
		];
		const offenders: string[] = [];
		for (const file of runtime) {
			const text = readFileSync(join(sources, file), 'utf8');
			for (const match of text.matchAll(/from '([^']+)'/g)) {
				const target = match[1] ?? '';
				if (
					target === 'effect' ||
					target === 'surrealdb' ||
					target.startsWith('./') ||
					target.startsWith('../')
				) {
					continue;
				}
				offenders.push(`${file}: ${target}`);
			}
			if (/\bBun\.|\bprocess\.|\brequire\(/.test(text))
				offenders.push(`${file}: host global`);
		}
		expect(offenders).toEqual([]);
	});
});
