/**
 * Generates the typed built-in functions of `effect-surrealkit/functions`
 * from the signature table `src/functions/signatures.json`:
 * `src/functions/generated/<family>.ts`, `codecs.ts` (one codec per kind the
 * table uses) and `index.ts` (a namespace per family).
 *
 *   node scripts/generate-functions.mjs
 *
 * The output is deterministic (sorted, no timestamps) and formatted with the
 * package's Biome configuration. `test/functions.test.ts` renders the table
 * in memory and fails when the files on disk differ.
 */
import { spawnSync } from 'node:child_process';
import {
	mkdirSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const packageRoot = join(dirname(fileURLToPath(import.meta.url)), '..');
export const tablePath = join(
	packageRoot,
	'src',
	'functions',
	'signatures.json',
);
export const generatedDir = join(packageRoot, 'src', 'functions', 'generated');

/** The SurrealDB majors an entry may list. */
export const majors = ['3', '4'];

/** Base kinds, and the codec each decodes with. */
const baseCodecs = {
	any: { name: 'AnyCodec', from: '../internal.js', source: 'AnyCodec' },
	array: { name: 'ArrayCodec', from: '../internal.js', source: 'ArrayCodec' },
	bool: { name: 'BoolCodec', from: 'effect', source: 'Schema.Boolean' },
	bytes: { name: 'BytesCodec', from: '../../scalars.js', source: 'Bytes' },
	datetime: {
		name: 'DateTimeCodec',
		from: '../../temporal.js',
		source: 'DateTime',
	},
	decimal: {
		name: 'DecimalCodec',
		from: '../../decimal.js',
		source: 'Decimal',
	},
	duration: {
		name: 'DurationCodec',
		from: '../../temporal.js',
		source: 'Duration',
	},
	float: { name: 'FloatCodec', from: '../../scalars.js', source: 'Float' },
	geometry: {
		name: 'GeometryCodec',
		from: '../../geometry.js',
		source: 'Geometry',
	},
	int: { name: 'IntCodec', from: '../../int.js', source: 'Int' },
	int64: { name: 'Int64Codec', from: '../../int.js', source: 'Int64' },
	number: {
		name: 'NumberCodec',
		from: '../../decimal.js',
		source: 'SurrealNumber',
	},
	object: {
		name: 'ObjectCodec',
		from: '../internal.js',
		source: 'ObjectCodec',
	},
	range: {
		name: 'RangeCodec',
		from: '../../collections.js',
		source: 'Range',
	},
	record: {
		name: 'RecordCodec',
		from: '../../record-id.js',
		source: 'RecordIdAny',
	},
	string: {
		name: 'StringCodec',
		from: '../../scalars.js',
		source: 'SurrealString',
	},
	uuid: { name: 'UuidCodec', from: '../../scalars.js', source: 'Uuid' },
};

// These SurrealDB 4 results have different representations in cbor and cbor.2.
const wireResults = {
	date: { codec: 'DateResult', result: 'datetime' },
	time: { codec: 'TimeResult', result: 'duration' },
	timestamp: { codec: 'TimestampResult', result: 'datetime' },
	interval: { codec: 'IntervalResult', result: 'string' },
	'sized-float': { codec: 'SizedFloatResult', result: 'float' },
	'sized-int': { codec: 'SizedIntResult', result: 'int' },
};

/**
 * The result kinds that build an array from the arguments they name
 * (`with:0,1`): how many they name, the helper and type that derive the
 * codec, and the kind of the result when an argument is untyped.
 */
const shapes = {
	adds: {
		arity: 2,
		helper: 'addsValue',
		type: 'AddsCodec',
		fallback: 'array',
	},
	chunks: {
		arity: 1,
		helper: 'chunksOf',
		type: 'ChunksCodec',
		fallback: 'array<array>',
	},
	elements: {
		arity: 2,
		helper: 'elementsOf',
		type: 'ElementsCodec',
		fallback: 'array',
	},
	flat: {
		arity: 1,
		helper: 'flattened',
		type: 'FlatCodec',
		fallback: 'array',
	},
	padded: {
		arity: 2,
		helper: 'nullPadded',
		type: 'PaddedCodec',
		fallback: 'array',
	},
	pairs: {
		arity: 2,
		helper: 'pairsOf',
		type: 'PairsCodec',
		fallback: 'array<array>',
	},
	repeats: {
		arity: 1,
		helper: 'repeated',
		type: 'RepeatsCodec',
		fallback: 'array',
	},
	transposed: {
		arity: 1,
		helper: 'transposed',
		type: 'TransposedCodec',
		fallback: 'array<array>',
	},
	with: {
		arity: 2,
		helper: 'withValue',
		type: 'WithCodec',
		fallback: 'array',
	},
};

const reserved = new Set(
	'break case catch class const continue debugger default delete do else enum export extends false finally for function if import in instanceof new null return super switch this throw true try typeof var void while with yield let static implements interface package private protected public await'.split(
		' ',
	),
);

/** Names the generated files use themselves. */
const taken = new Set([
	'apply',
	'constant',
	'floatOrDecimal',
	'item',
	'items',
	'same',
	'C',
	'Arg',
	'In',
	'Expr',
	'NoParams',
	'ParamsOfAll',
	'CodecOfArg',
	'FloatOrDecimalCodec',
	'ItemCodec',
	'ItemsCodec',
	...Object.values(shapes).flatMap((shape) => [shape.helper, shape.type]),
]);

const fail = (message) => {
	throw new Error(`signatures.json: ${message}`);
};

/**
 * A kind as a tree: `{ base }`, `{ array: kind }` or `{ option: kind }`.
 * Results may also be `{ same: n }`, `{ item: n }`, `{ items: n }` or
 * `{ floatOrDecimal: [n, …] }` (`float|decimal:n,…`) or
 * `{ shape, args: [n, …] }` (`with:n,m`, one of `shapes`).
 */
export const parseKind = (text, where, result = false) => {
	const derived = /^(same|item|items):(\d+)$/u.exec(text);
	if (derived !== null) {
		if (!result) fail(`${where}: \`${text}\` is a result kind only`);
		return { [derived[1]]: Number(derived[2]) };
	}
	const shaped = /^([a-z]+):(\d+(?:,\d+)*)$/u.exec(text);
	if (shaped !== null && Object.hasOwn(shapes, shaped[1])) {
		if (!result) fail(`${where}: \`${text}\` is a result kind only`);
		const args = shaped[2].split(',').map(Number);
		if (args.length !== shapes[shaped[1]].arity) {
			fail(
				`${where}: \`${text}\` names ${shapes[shaped[1]].arity} arguments`,
			);
		}
		return { shape: shaped[1], args };
	}
	const widened = /^float\|decimal:(\d+(?:,\d+)*)$/u.exec(text);
	if (widened !== null) {
		if (!result) fail(`${where}: \`${text}\` is a result kind only`);
		const indices = widened[1].split(',').map(Number);
		if (
			indices.join() !==
			[...new Set(indices)].sort((a, b) => a - b).join()
		) {
			fail(`${where}: \`${text}\` lists arguments once each, in order`);
		}
		return { floatOrDecimal: indices };
	}
	const wrapped = /^(array|option)<(.+)>$/u.exec(text);
	if (wrapped !== null) {
		return { [wrapped[1]]: parseKind(wrapped[2], where) };
	}
	if (!(text in baseCodecs)) fail(`${where}: unknown kind \`${text}\``);
	return { base: text };
};

const capital = (text) => text.charAt(0).toUpperCase() + text.slice(1);

/** The name of the codec of a kind in `generated/codecs.ts`. */
export const codecName = (kind) => {
	if ('base' in kind) return baseCodecs[kind.base].name;
	if ('array' in kind) return `ArrayOf${codecName(kind.array)}`;
	return `OptionOf${codecName(kind.option)}`;
};

const kindText = (kind) =>
	'base' in kind
		? kind.base
		: 'array' in kind
			? `array<${kindText(kind.array)}>`
			: `option<${kindText(kind.option)}>`;

/** The stored type an argument of `kind` accepts, as TypeScript. */
const inType = (kind) => {
	if ('base' in kind) return `In['${kind.base}']`;
	if ('array' in kind) return `ReadonlyArray<${inType(kind.array)}>`;
	return `${inType(kind.option)} | undefined`;
};

/** `string::is_email` → `string`. */
export const familyOf = (name) => name.split('::')[0];

/**
 * The export name within its family: the rest of the path in camelCase
 * (`string::is_email` → `isEmail`, `vector::distance::euclidean` →
 * `distanceEuclidean`), an underscore kept between two digits
 * (`math::log10_2` → `log10_2`) and a trailing `_` on a reserved word
 * (`rand::enum` → `enum_`).
 */
export const exportName = (name) => {
	const parts = name.split('::').slice(1).join('_').split('_');
	let out = parts[0] ?? '';
	for (const part of parts.slice(1)) {
		out +=
			/\d$/u.test(out) && /^\d/u.test(part) ? `_${part}` : capital(part);
	}
	return reserved.has(out) ? `${out}_` : out;
};

const identifier = (text) =>
	reserved.has(text) || taken.has(text) ? `${text}_` : text;

const validate = (table) => {
	if (!Array.isArray(table.functions)) fail('`functions` must be a list');
	const seen = new Set();
	const exports = new Set();
	for (const entry of table.functions) {
		const where = entry.name ?? JSON.stringify(entry);
		if (!/^[a-z][a-z0-9_]*(::[a-z0-9_]+)+$/u.test(entry.name ?? '')) {
			fail(`${where}: not a function path`);
		}
		if (seen.has(entry.name)) fail(`${where}: listed twice`);
		seen.add(entry.name);
		const key = `${familyOf(entry.name)}.${exportName(entry.name)}`;
		if (exports.has(key)) fail(`${where}: export name ${key} is taken`);
		exports.add(key);
		if (!Array.isArray(entry.args))
			fail(`${where}: \`args\` must be a list`);
		if (entry.constant === true && entry.args.length > 0) {
			fail(`${where}: a constant takes no arguments`);
		}
		let optional = false;
		const names = new Set();
		for (const [index, arg] of entry.args.entries()) {
			if (!/^[a-z][A-Za-z0-9]*$/u.test(arg.name ?? '')) {
				fail(`${where}: argument ${index} needs a camelCase name`);
			}
			if (names.has(arg.name))
				fail(`${where}: two arguments named ${arg.name}`);
			names.add(arg.name);
			parseKind(arg.kind, `${where} argument ${arg.name}`);
			if (arg.optional === true) optional = true;
			else if (optional) {
				fail(`${where}: only trailing arguments may be optional`);
			}
		}
		if (entry.variadic === true) {
			if (entry.args.length === 0)
				fail(`${where}: variadic with no argument`);
			if (optional)
				fail(`${where}: a variadic function has no optional argument`);
		}
		const result = parseKind(entry.result, `${where} result`, true);
		if (
			entry.wireResult !== undefined &&
			(!Object.hasOwn(wireResults, entry.wireResult) ||
				wireResults[entry.wireResult].result !== entry.result)
		) {
			fail(`${where}: invalid wire result for \`${entry.result}\``);
		}
		const index = result.same ?? result.item ?? result.items;
		if (index !== undefined && entry.args[index] === undefined) {
			fail(`${where}: result \`${entry.result}\` names no argument`);
		}
		if (
			result.items !== undefined &&
			(entry.variadic !== true || result.items !== entry.args.length - 1)
		) {
			fail(`${where}: \`items:N\` names the variadic argument`);
		}
		for (const named of result.args ?? []) {
			if (entry.args[named] === undefined) {
				fail(`${where}: result \`${entry.result}\` names no argument`);
			}
			if (entry.variadic === true && named === entry.args.length - 1) {
				fail(
					`${where}: \`${entry.result}\` names the variadic argument`,
				);
			}
		}
		for (const named of result.floatOrDecimal ?? []) {
			const arg = entry.args[named];
			if (arg === undefined) {
				fail(`${where}: result \`${entry.result}\` names no argument`);
			}
			if (entry.variadic === true && named === entry.args.length - 1) {
				fail(`${where}: \`float|decimal\` names the variadic argument`);
			}
		}
		if (
			!Array.isArray(entry.engines) ||
			entry.engines.length === 0 ||
			entry.engines.some((major) => !majors.includes(major)) ||
			[...entry.engines].sort().join() !== entry.engines.join()
		) {
			fail(
				`${where}: \`engines\` must list majors from ${majors.join(', ')} in order`,
			);
		}
		if (
			!Array.isArray(entry.sample) ||
			entry.sample.some((text) => typeof text !== 'string')
		) {
			fail(`${where}: \`sample\` must be a list of SurrealQL texts`);
		}
		const required = entry.args.filter(
			(arg) => arg.optional !== true,
		).length;
		const fixed =
			entry.variadic === true ? entry.args.length - 1 : required;
		const fits = (sample) =>
			sample.length >= fixed &&
			(entry.variadic === true || sample.length <= entry.args.length);
		if (!fits(entry.sample)) {
			fail(`${where}: the sample has ${entry.sample.length} arguments`);
		}
		if (entry.variants !== undefined) {
			if (
				!Array.isArray(entry.variants) ||
				entry.variants.some(
					(variant) =>
						!Array.isArray(variant) ||
						variant.some((text) => typeof text !== 'string'),
				)
			) {
				fail(`${where}: \`variants\` must be a list of samples`);
			}
			if (entry.constant === true)
				fail(`${where}: a constant has no variants`);
			for (const variant of entry.variants) {
				if (!fits(variant)) {
					fail(`${where}: a variant has ${variant.length} arguments`);
				}
			}
		}
	}
	const names = table.functions.map((entry) => entry.name);
	if (names.join('\n') !== [...names].sort().join('\n')) {
		fail('functions must be sorted by name');
	}
	return table;
};

/** Reads and checks the signature table. */
export const loadTable = (path = tablePath) =>
	validate(JSON.parse(readFileSync(path, 'utf8')));

const header = (what) =>
	[
		`// Generated by scripts/generate-functions.mjs from src/functions/signatures.json${what}.`,
		'// Do not edit: change the table and run `npm run generate:functions`.',
		'',
	].join('\n');

const engineNote = (entry) =>
	entry.engines.length === majors.length
		? `SurrealDB ${majors.join(' and ')}.`
		: `Only on SurrealDB ${entry.engines.join(' and ')}.`;

const signatureText = (entry) => {
	if (entry.constant === true) return `${entry.name} -> ${entry.result}`;
	const args = entry.args.map((arg, index) => {
		const rest =
			entry.variadic === true && index === entry.args.length - 1
				? '...'
				: '';
		return `${rest}${arg.name}${arg.optional === true ? '?' : ''}: ${arg.kind}`;
	});
	return `${entry.name}(${args.join(', ')}) -> ${entry.result}`;
};

/** Every kind a codec is needed for, nested kinds included. */
const collectKinds = (kind, into) => {
	if ('shape' in kind) {
		collectKinds(parseKind(shapes[kind.shape].fallback, kind.shape), into);
		return;
	}
	if ('array' in kind) collectKinds(kind.array, into);
	if ('option' in kind) collectKinds(kind.option, into);
	if ('base' in kind || 'array' in kind || 'option' in kind) {
		into.set(codecName(kind), kind);
	}
};

/** What a derived result kind means, for the doc comment; empty for a plain kind. */
const resultNote = (entry, result) => {
	if (!('floatOrDecimal' in result)) return '';
	const names = result.floatOrDecimal.map(
		(index) => `\`${entry.args[index].name}\``,
	);
	const list =
		names.length === 1
			? names[0]
			: `${names.slice(0, -1).join(', ')} or ${names[names.length - 1]}`;
	return ` * A float; a float or a decimal when ${list} may hold a decimal.\n *\n`;
};

const renderFunction = (entry) => {
	const result = parseKind(entry.result, entry.name, true);
	const wireCodec = wireResults[entry.wireResult]?.codec;
	const doc = `/**\n * \`${signatureText(entry)}\`\n *\n${resultNote(entry, result)} * ${engineNote(entry)}\n */\n`;
	const name = exportName(entry.name);
	if (entry.constant === true) {
		const base = `C.${codecName(result)}`;
		return `${doc}export const ${name}: Expr<typeof ${base}, NoParams> = /*#__PURE__*/ constant('${entry.name}', ${wireCodec ?? base});\n`;
	}
	const variadic = entry.variadic === true;
	const params = entry.args.map((arg, index) => {
		const kind = parseKind(arg.kind, entry.name);
		const rest = variadic && index === entry.args.length - 1;
		return {
			name: identifier(arg.name),
			type: `A${index}`,
			kind,
			rest,
			optional: arg.optional === true,
			constraint: rest
				? `ReadonlyArray<Arg<${inType(kind)}>>`
				: `Arg<${inType(kind)}>`,
		};
	});
	const generics = params.map(
		(param) =>
			`const ${param.type} extends ${param.constraint}${param.optional ? ' = never' : ''}`,
	);
	const list = params.map((param) =>
		param.rest
			? `...${param.name}: ${param.type}`
			: `${param.name}${param.optional ? '?' : ''}: ${param.type}`,
	);
	const read = params.map((param) =>
		param.rest ? `${param.type}[number]` : param.type,
	);
	const paramsType =
		read.length === 0 ? 'NoParams' : `ParamsOfAll<${read.join(' | ')}>`;
	let codecType;
	let codecValue;
	if ('floatOrDecimal' in result) {
		const named = result.floatOrDecimal.map((index) => params[index].type);
		codecType = `FloatOrDecimalCodec<${named.join(' | ')}>`;
		codecValue = `floatOrDecimal([${result.floatOrDecimal.join(', ')}])`;
	} else if ('shape' in result) {
		const shape = shapes[result.shape];
		const fallback = `C.${codecName(parseKind(shape.fallback, entry.name))}`;
		const named = result.args.map((index) => params[index].type);
		codecType = `${shape.type}<[${named.join(', ')}], typeof ${fallback}>`;
		codecValue = `${shape.helper}([${result.args.join(', ')}], ${fallback})`;
	} else if ('items' in result) {
		const source = params[result.items];
		const fallback = `C.${codecName(source.kind)}`;
		codecType = `ItemsCodec<${source.type}, typeof ${fallback}>`;
		codecValue = `items(${result.items}, ${fallback})`;
	} else if ('same' in result || 'item' in result) {
		const index = result.same ?? result.item;
		const source = params[index];
		const fallback = `C.${codecName(source.kind)}`;
		const own = `CodecOfArg<${source.type}, typeof ${fallback}>`;
		codecType = 'same' in result ? own : `ItemCodec<${own}>`;
		codecValue = `${'same' in result ? 'same' : 'item'}(${index}, ${fallback})`;
	} else {
		// A wire codec has its base codec's type, so the signature is unchanged.
		codecType = `typeof C.${codecName(result)}`;
		codecValue = wireCodec ?? `C.${codecName(result)}`;
	}
	const fixed = params.filter((param) => !param.rest);
	const required = fixed.filter((param) => !param.optional).length;
	const restParam = params.find((param) => param.rest);
	const call = [
		`'${entry.name}'`,
		codecValue,
		String(required),
		`[${fixed.map((param) => param.name).join(', ')}]`,
		...(restParam === undefined ? [] : [restParam.name]),
	];
	const typeParams = generics.length === 0 ? '' : `<${generics.join(', ')}>`;
	return `${doc}export const ${name} = ${typeParams}(${list.join(', ')}): Expr<${codecType}, ${paramsType}> => apply(${call.join(', ')});\n`;
};

const renderFamily = (family, entries) => {
	const text = entries.map(renderFunction).join('\n');
	const uses = (pattern) => pattern.test(text);
	const exprTypes = [
		'Expr',
		...(uses(/ParamsOfAll</u) ? ['ParamsOfAll'] : []),
	];
	const shaped = Object.values(shapes).flatMap((shape) => [
		...(uses(new RegExp(`\\b${shape.type}<`, 'u'))
			? [`type ${shape.type}`]
			: []),
		...(uses(new RegExp(`\\b${shape.helper}\\(`, 'u'))
			? [shape.helper]
			: []),
	]);
	const internal = [
		...(uses(/\bArg</u) ? ['type Arg'] : []),
		...(uses(/apply\(/u) ? ['apply'] : []),
		...(uses(/CodecOfArg</u) ? ['type CodecOfArg'] : []),
		...(uses(/constant\(/u) ? ['constant'] : []),
		...(uses(/FloatOrDecimalCodec</u) ? ['type FloatOrDecimalCodec'] : []),
		...(uses(/floatOrDecimal\(/u) ? ['floatOrDecimal'] : []),
		...(uses(/In\['/u) ? ['type In'] : []),
		...(uses(/ItemCodec</u) ? ['type ItemCodec'] : []),
		...(uses(/ItemsCodec</u) ? ['type ItemsCodec'] : []),
		...(uses(/\bitem\(/u) ? ['item'] : []),
		...(uses(/\bitems\(/u) ? ['items'] : []),
		...(uses(/\bsame\(/u) ? ['same'] : []),
		...shaped,
	].sort((a, b) =>
		a.replace(/^type /u, '').localeCompare(b.replace(/^type /u, '')),
	);
	const wireCodecs = [
		...new Set(
			entries.flatMap((entry) =>
				entry.wireResult === undefined
					? []
					: [wireResults[entry.wireResult].codec],
			),
		),
	].sort();
	const imports = [
		`import type { ${exprTypes.join(', ')} } from '../../statement/expr.js';`,
		...(uses(/NoParams/u)
			? ["import type { NoParams } from '../../statement/statement.js';"]
			: []),
		`import { ${internal.join(', ')} } from '../internal.js';`,
		...(wireCodecs.length === 0
			? []
			: [`import { ${wireCodecs.join(', ')} } from '../result.js';`]),
		"import * as C from './codecs.js';",
	];
	return `${header(` (the \`${family}::\` family)`)}\n${imports.join('\n')}\n\n${text}`;
};

const renderCodecs = (kinds) => {
	const sorted = [...kinds.values()].sort((a, b) => {
		const depth = (kind) =>
			'base' in kind ? 0 : 1 + depth(kind.array ?? kind.option);
		return depth(a) - depth(b) || (codecName(a) < codecName(b) ? -1 : 1);
	});
	const sources = new Map();
	for (const kind of sorted) {
		if (!('base' in kind)) continue;
		const { from, source } = baseCodecs[kind.base];
		const local = source.split('.')[0];
		sources.set(from, new Set([...(sources.get(from) ?? []), local]));
	}
	if (sorted.some((kind) => !('base' in kind))) {
		sources.set(
			'effect',
			new Set([...(sources.get('effect') ?? []), 'Schema']),
		);
	}
	const imports = [...sources.entries()]
		.sort(([a], [b]) => (a < b ? -1 : 1))
		.map(
			([from, names]) =>
				`import { ${[...names].sort().join(', ')} } from '${from}';`,
		);
	// The open kinds' codecs live in `internal.ts` under the same name.
	const shared = sorted
		.filter(
			(kind) =>
				'base' in kind &&
				baseCodecs[kind.base].source === codecName(kind),
		)
		.map(codecName);
	const lines = sorted.flatMap((kind) => {
		const name = codecName(kind);
		if ('base' in kind) {
			return shared.includes(name)
				? []
				: [
						`/** \`${kindText(kind)}\` */\nexport const ${name} = ${baseCodecs[kind.base].source};`,
					];
		}
		const inner = codecName(kind.array ?? kind.option);
		const make = 'array' in kind ? 'Schema.Array' : 'Schema.UndefinedOr';
		return [
			`/** \`${kindText(kind)}\` */\nexport const ${name} = /*#__PURE__*/ ${make}(${inner});`,
		];
	});
	const reexport =
		shared.length === 0 ? '' : `export { ${shared.join(', ')} };\n\n`;
	return `${header(' (a codec per kind the table uses)')}\n${imports.join('\n')}\n\n${reexport}${lines.join('\n\n')}\n`;
};

/**
 * The generated files for `table`, unformatted: a map from the path
 * relative to the package to the text.
 */
export const render = (table) => {
	const families = new Map();
	const kinds = new Map();
	for (const entry of table.functions) {
		const family = familyOf(entry.name);
		families.set(family, [...(families.get(family) ?? []), entry]);
		for (const arg of entry.args)
			collectKinds(parseKind(arg.kind, entry.name), kinds);
		collectKinds(parseKind(entry.result, entry.name, true), kinds);
	}
	const out = new Map();
	const dir = relative(packageRoot, generatedDir);
	out.set(join(dir, 'codecs.ts'), renderCodecs(kinds));
	const names = [...families.keys()].sort();
	for (const family of names) {
		out.set(
			join(dir, `${family}.ts`),
			renderFamily(family, families.get(family)),
		);
	}
	const index = names.map(
		(family) => `export * as ${family} from './${family}.js';`,
	);
	out.set(
		join(dir, 'index.ts'),
		`${header(' (a namespace per family)')}\n${index.join('\n')}\n`,
	);
	return out;
};

const biome = createRequire(import.meta.url).resolve(
	'@biomejs/biome/bin/biome',
);

/** `text` as Biome formats and organises it at `path` (relative to the package). */
export const format = (path, text) => {
	const result = spawnSync(
		process.execPath,
		[biome, 'check', '--write', `--stdin-file-path=${path}`],
		{ cwd: packageRoot, input: text, encoding: 'utf8' },
	);
	if (result.status !== 0) {
		throw new Error(
			`biome could not format ${path}:\n${result.stderr}${result.stdout}`,
		);
	}
	return result.stdout;
};

/** The generated files, formatted. */
export const generate = (table = loadTable()) =>
	new Map(
		[...render(table)].map(([path, text]) => [path, format(path, text)]),
	);

const main = () => {
	const files = generate();
	rmSync(generatedDir, { recursive: true, force: true });
	mkdirSync(generatedDir, { recursive: true });
	for (const [path, text] of files)
		writeFileSync(join(packageRoot, path), text);
	console.log(
		`wrote ${readdirSync(generatedDir).length} files to ${relative(packageRoot, generatedDir)}`,
	);
};

if (
	process.argv[1] !== undefined &&
	import.meta.url === pathToFileURL(process.argv[1]).href
) {
	main();
}
