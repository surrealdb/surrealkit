import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import { Schema, SchemaRepresentation } from 'effect';
import { describe, expect, it } from 'vitest';

import * as S from '../src/index.js';

const print = (schema: Schema.Top) =>
	SchemaRepresentation.toCodeDocument(
		SchemaRepresentation.toMultiDocument(
			SchemaRepresentation.toRepresentation(schema.ast),
		),
	);

describe('toCode metadata', () => {
	it('every codec prints itself by name through toCodeDocument', () => {
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
		const document = print(S.NoneAsAbsent(row));
		const [code] = document.codes;
		expect(code?.runtime).toMatchSnapshot();
		expect(code?.Type).toMatchSnapshot();
		expect(document.artifacts).toContainEqual({
			_tag: 'Import',
			importDeclaration:
				'import * as SurrealSchema from "effect-surrealkit"',
		});
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
