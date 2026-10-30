/** `geometry` / `geometry<point | polygon | …>`: the SDK geometry classes. */
import { Schema, SchemaGetter } from 'effect';
import {
	Geometry as SdkGeometry,
	GeometryCollection as SdkGeometryCollection,
	GeometryLine as SdkGeometryLine,
	GeometryMultiLine as SdkGeometryMultiLine,
	GeometryMultiPoint as SdkGeometryMultiPoint,
	GeometryMultiPolygon as SdkGeometryMultiPolygon,
	GeometryPoint as SdkGeometryPoint,
	GeometryPolygon as SdkGeometryPolygon,
} from 'surrealdb';

import { quoteList, runtimeCode } from './internal/code.js';
import { invalid, leaf, ok, wrongType } from './internal/declare.js';

/** The seven geometry kinds of schema document v2. */
export type GeometryKind =
	| 'collection'
	| 'line'
	| 'multiline'
	| 'multipoint'
	| 'multipolygon'
	| 'point'
	| 'polygon';

interface GeometryKinds {
	readonly collection: SdkGeometryCollection;
	readonly line: SdkGeometryLine;
	readonly multiline: SdkGeometryMultiLine;
	readonly multipoint: SdkGeometryMultiPoint;
	readonly multipolygon: SdkGeometryMultiPolygon;
	readonly point: SdkGeometryPoint;
	readonly polygon: SdkGeometryPolygon;
}

const classes: { readonly [K in GeometryKind]: (u: unknown) => boolean } = {
	collection: (u) => u instanceof SdkGeometryCollection,
	line: (u) => u instanceof SdkGeometryLine,
	multiline: (u) => u instanceof SdkGeometryMultiLine,
	multipoint: (u) => u instanceof SdkGeometryMultiPoint,
	multipolygon: (u) => u instanceof SdkGeometryMultiPolygon,
	point: (u) => u instanceof SdkGeometryPoint,
	polygon: (u) => u instanceof SdkGeometryPolygon,
};

const typeNames: { readonly [K in GeometryKind]: string } = {
	collection: 'Surreal.GeometryCollection',
	line: 'Surreal.GeometryLine',
	multiline: 'Surreal.GeometryMultiLine',
	multipoint: 'Surreal.GeometryMultiPoint',
	multipolygon: 'Surreal.GeometryMultiPolygon',
	point: 'Surreal.GeometryPoint',
	polygon: 'Surreal.GeometryPolygon',
};

const kindOf = (value: unknown): GeometryKind | undefined =>
	(Object.keys(classes) as GeometryKind[]).find((kind) =>
		classes[kind](value),
	);

const coordinate = Schema.Finite.check(
	Schema.isBetween({ minimum: -180, maximum: 180 }),
);

const point = (x: number, y: number) => new SdkGeometryPoint([x, y]);

/** Arbitrary source for each kind: a list of coordinate pairs. */
const pairs = (min: number, max: number) =>
	Schema.Array(Schema.Tuple([coordinate, coordinate])).check(
		Schema.isBetweenLength(min, max),
	);

const toPoints = (list: ReadonlyArray<readonly [number, number]>) =>
	list.map(([x, y]) => point(x, y));

type AtLeastTwo = [SdkGeometryPoint, SdkGeometryPoint, ...SdkGeometryPoint[]];

const atLeastTwo = (points: ReadonlyArray<SdkGeometryPoint>): AtLeastTwo => {
	const [a, b, ...rest] = points;
	if (a === undefined || b === undefined) {
		throw new RangeError('a line needs at least two points');
	}
	return [a, b, ...rest];
};

const nonEmpty = (
	points: ReadonlyArray<SdkGeometryPoint>,
): [SdkGeometryPoint, ...SdkGeometryPoint[]] => {
	const [a, ...rest] = points;
	if (a === undefined)
		throw new RangeError('a geometry needs at least one point');
	return [a, ...rest];
};

const closedRing = (list: ReadonlyArray<readonly [number, number]>) => {
	const points = toPoints(list);
	return new SdkGeometryLine(atLeastTwo([...points, ...points.slice(0, 1)]));
};

const build: {
	readonly [K in GeometryKind]: (
		list: ReadonlyArray<readonly [number, number]>,
	) => GeometryKinds[K];
} = {
	point: (list) => nonEmpty(toPoints(list))[0],
	line: (list) => new SdkGeometryLine(atLeastTwo(toPoints(list))),
	polygon: (list) => new SdkGeometryPolygon([closedRing(list)]),
	multipoint: (list) => new SdkGeometryMultiPoint(nonEmpty(toPoints(list))),
	multiline: (list) =>
		new SdkGeometryMultiLine([
			new SdkGeometryLine(atLeastTwo(toPoints(list))),
		]),
	multipolygon: (list) =>
		new SdkGeometryMultiPolygon([
			new SdkGeometryPolygon([closedRing(list)]),
		]),
	collection: (list) => new SdkGeometryCollection(nonEmpty(toPoints(list))),
};

const minimumPairs: { readonly [K in GeometryKind]: number } = {
	point: 1,
	line: 2,
	polygon: 3,
	multipoint: 1,
	multiline: 2,
	multipolygon: 3,
	collection: 1,
};

/**
 * `geometry<…>`: an SDK geometry of one of `kinds`; any of the seven when
 * `kinds` is empty.
 */
export const GeometryOf = <const Ks extends ReadonlyArray<GeometryKind>>(
	kinds: Ks,
) => {
	type T = Ks extends readonly [] ? SdkGeometry : GeometryKinds[Ks[number]];
	const accepted: ReadonlyArray<GeometryKind> =
		kinds.length === 0 ? (Object.keys(classes) as GeometryKind[]) : kinds;
	const first = accepted[0] as GeometryKind;
	return leaf<T>({
		id: 'effect-surrealkit/GeometryOf',
		payload: [...kinds],
		expected:
			kinds.length === 0
				? 'a geometry'
				: `a geometry (${kinds.join(', ')})`,
		code: runtimeCode(
			`GeometryOf(${quoteList(kinds)})`,
			kinds.length === 0
				? 'Surreal.Geometry'
				: kinds.map((kind) => typeNames[kind]).join(' | '),
		),
		parse: (input) => {
			if (!(input instanceof SdkGeometry)) return wrongType;
			const kind = kindOf(input);
			if (kind === undefined || !accepted.includes(kind)) {
				return invalid(
					`geometry ${kind ?? 'of an unknown kind'} is not one of ${accepted.join(', ')}`,
				);
			}
			return ok(input as T);
		},
		equivalence: (self, that) => self.equals(that),
		arbitrary: () =>
			Schema.link<T>()(
				pairs(minimumPairs[first], minimumPairs[first] + 3),
				{
					decode: SchemaGetter.transform(
						(list) => build[first](list) as T,
					),
					encode: SchemaGetter.forbidden(
						() => 'geometry arbitraries only generate values',
					),
				},
			),
	});
};

/** `geometry<point>`. */
export const GeometryPoint = GeometryOf(['point']);
/** `geometry<line>`. */
export const GeometryLine = GeometryOf(['line']);
/** `geometry<polygon>`. */
export const GeometryPolygon = GeometryOf(['polygon']);
/** `geometry<multipoint>`. */
export const GeometryMultiPoint = GeometryOf(['multipoint']);
/** `geometry<multiline>`. */
export const GeometryMultiLine = GeometryOf(['multiline']);
/** `geometry<multipolygon>`. */
export const GeometryMultiPolygon = GeometryOf(['multipolygon']);
/** `geometry<collection>`. */
export const GeometryCollection = GeometryOf(['collection']);
/** Bare `geometry`: any of the seven kinds. */
export const Geometry = GeometryOf([]);
