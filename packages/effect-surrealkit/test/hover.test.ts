/**
 * A statement's input and result are read in an editor's hover, so they
 * must print as plain object types: no intersections of bindings, no
 * helper aliases (`Simplify`, `Input`, `ProjectedCodec`). Each marker of
 * `test/types/hover.ts` names a hover whose text is pinned here.
 */
import { join } from 'node:path';

import ts from 'typescript';
import { beforeAll, describe, expect, it } from 'vitest';

import { languageService, textOf, typesRoot } from './compiler.js';

const fixture = join(typesRoot, 'hover.ts');
const marker = /\/\*(hover|input):([a-z-]+)\*\//gu;

const expected: Readonly<Record<string, string>> = {
	'select-input': `type AdultsInput = {
    readonly minimum: number;
    readonly nickname: string;
    readonly city: string;
}`,
	'select-project-result': `type AdultsResult = readonly {
    readonly id: _RecordId<"person"> & Brand<"corpus/person">;
    readonly name: string;
    readonly nickname: string | undefined;
}[]`,
	'select-params': `const adultsParams: {
    readonly minimum: declareConstructor<number, number, readonly [], number>;
    readonly nickname: declareConstructor<string, string, readonly [], string>;
    readonly city: declareConstructor<string, string, readonly [], string>;
}`,
	'run-result': `const rows: readonly {
    readonly id: _RecordId<"person"> & Brand<"corpus/person">;
    readonly name: string;
    readonly nickname: string | undefined;
}[]`,
	'run-input':
		'{ readonly minimum: number; readonly nickname: string; readonly city: string; }',
	'create-result': `type Created = {
    readonly id: _RecordId<"person"> & Brand<"corpus/person">;
    readonly name: string;
}`,
	'merge-input': `type MergedInput = {
    readonly id: _RecordId<"person"> & Brand<"corpus/person">;
    readonly changes: {
        readonly address?: {
            readonly [x: string]: SurrealSchema.SurrealMergeValue;
            readonly city?: string;
        };
        readonly age?: number | undefined;
        readonly manager?: (_RecordId<"person"> & Brand<"corpus/person">) | undefined;
        readonly mentor?: (_RecordId<"person"> & Brand<"corpus/person">) | undefined;
        readonly nickname?: string | undefined;
        readonly "odd.name"?: string | undefined;
        readonly pattern?: string | undefined;
        ... 8 more ...;
        readonly updated_at?: DateTime | undefined;
    };
}`,
	'merge-result': `type Merged = {
    readonly id: _RecordId<"person"> & Brand<"corpus/person">;
    readonly email: string;
} | undefined`,
	'update-many-result': `type Renamed = readonly {
    readonly name: string;
}[]`,
	'delete-params': `const removedParams: {
    readonly id: brand<declareConstructor<_RecordId<"person">, _RecordId<"person">, readonly [declareConstructor<RecordIdValue, RecordIdValue, readonly [], RecordIdValue>], _RecordId<"person">>, "corpus/person">;
}`,
	'relate-result': `type Joined = {
    readonly id: _RecordId<"member_of"> & Brand<"corpus/member_of">;
    readonly in: _RecordId<"person"> & Brand<"corpus/person">;
    readonly out: (_RecordId<"org"> & Brand<"corpus/org">) | (_RecordId<"team"> & Brand<"corpus/team">);
    readonly role: "member" | "lead";
    readonly since: DateTime;
}`,
	'relate-params': `const joinedParams: {
    readonly person: brand<declareConstructor<_RecordId<"person">, _RecordId<"person">, readonly [declareConstructor<RecordIdValue, RecordIdValue, readonly [], RecordIdValue>], _RecordId<"person">>, "corpus/person">;
    readonly team: brand<declareConstructor<_RecordId<"team">, _RecordId<"team">, readonly [declareConstructor<RecordIdValue, RecordIdValue, readonly [], RecordIdValue>], _RecordId<...>>, "corpus/team">;
}`,
};

describe('hover types', () => {
	const found = new Map<string, string>();

	beforeAll(() => {
		const service = languageService([fixture]);
		const text = textOf(fixture);
		const program = service.getProgram();
		const source = program?.getSourceFile(fixture);
		if (program === undefined || source === undefined) {
			throw new Error('the hover fixture is not in the program');
		}
		const checker = program.getTypeChecker();
		for (const match of text.matchAll(marker)) {
			const [whole, kind, name] = match;
			if (kind === undefined || name === undefined) continue;
			let at = match.index + whole.length;
			while (/\s/u.test(text[at] ?? '')) at += 1;
			if (kind === 'hover') {
				const info = service.getQuickInfoAtPosition(fixture, at);
				found.set(name, ts.displayPartsToString(info?.displayParts));
				continue;
			}
			const call = findCall(source, at);
			// `run(statement, ...args)`: the input is the first element of the
			// rest tuple the call resolved to.
			const rest = checker.getResolvedSignature(call)?.parameters[1];
			if (rest === undefined) throw new Error(`${name}: no input`);
			const args = checker.getTypeOfSymbolAtLocation(rest, call);
			const [input] = checker.getTypeArguments(args as ts.TypeReference);
			if (input === undefined) throw new Error(`${name}: no input`);
			found.set(
				name,
				checker.typeToString(
					input,
					call,
					ts.TypeFormatFlags.NoTruncation,
				),
			);
		}
	}, 120_000);

	it('finds every marker', () => {
		expect([...found.keys()].sort()).toEqual(Object.keys(expected).sort());
	});

	it.each(Object.entries(expected))('%s', (name, text) => {
		expect(found.get(name)).toBe(text);
	});
});

/** The call expression that starts at `at`. */
const findCall = (source: ts.SourceFile, at: number): ts.CallExpression => {
	let call: ts.CallExpression | undefined;
	const visit = (node: ts.Node): void => {
		if (ts.isCallExpression(node) && node.getStart(source) === at) {
			call ??= node;
		}
		if (node.pos <= at && at < node.end) ts.forEachChild(node, visit);
	};
	visit(source);
	if (call === undefined) throw new Error(`no call at ${at}`);
	return call;
};
