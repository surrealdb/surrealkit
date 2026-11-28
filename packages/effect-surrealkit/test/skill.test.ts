/**
 * The agent skill at `skills/effect-surrealkit/` names exports, command
 * flags and sources. This suite holds every one of them to the code at this
 * revision, so the skill cannot drift from what the package does.
 */
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import * as analyze from '../src/analyze/index.js';
import { USAGE } from '../src/cli/run.js';
import { STATEMENTS_USAGE } from '../src/cli/statements.js';
import * as client from '../src/client/index.js';
import * as emitter from '../src/emitter/index.js';
import * as functions from '../src/functions/index.js';
import * as root from '../src/index.js';
import * as mapper from '../src/mapper/index.js';
import * as statement from '../src/statement/index.js';
import * as testing from '../src/testing/index.js';

const packageDir = fileURLToPath(new URL('..', import.meta.url));
const repositoryDir = join(packageDir, '..', '..');
const skillDir = join(repositoryDir, 'skills', 'effect-surrealkit');
const referencesDir = join(skillDir, 'references');

/** The package's entry points by import specifier (`register` only hooks). */
const entries: Record<string, Record<string, unknown>> = {
	'effect-surrealkit': root,
	'effect-surrealkit/analyze': analyze,
	'effect-surrealkit/client': client,
	'effect-surrealkit/emitter': emitter,
	'effect-surrealkit/functions': functions,
	'effect-surrealkit/mapper': mapper,
	'effect-surrealkit/statement': statement,
	'effect-surrealkit/testing': testing,
};
const exported = new Set(
	Object.values(entries).flatMap((entry) => Object.keys(entry)),
);

/** Names the skill writes that belong to another package of this repository. */
const elsewhere: Record<string, string> = {
	surrealkitPlugin: 'packages/vite-plugin-surrealkit/src/index.ts',
};
/** Option and configuration names the skill writes, which are not exports. */
const options: Record<string, string> = {
	contentMappers: 'src/mapper/project.ts',
	openTargets: 'src/analyze/analyze.ts',
	rejectsNone: 'src/statement/part.ts',
	uniqueIndexes: 'src/statement/statement.ts',
};

const skillPath = join(skillDir, 'SKILL.md');
const skill = readFileSync(skillPath, 'utf8');
const references = existsSync(referencesDir)
	? readdirSync(referencesDir)
			.filter((name) => name.endsWith('.md'))
			.sort()
	: [];
const documents = [
	{ name: 'SKILL.md', text: skill },
	...references.map((name) => ({
		name: `references/${name}`,
		text: readFileSync(join(referencesDir, name), 'utf8'),
	})),
];

const fencesOf = (text: string) =>
	[...text.matchAll(/^```[^\n]*\n([\s\S]*?)^```$/gmu)].map(
		(match) => match[1] ?? '',
	);
const spansOf = (text: string) =>
	[
		...text
			.replace(/^```[^\n]*\n[\s\S]*?^```$/gmu, '')
			.matchAll(/`([^`]+)`/gu),
	]
		.map((match) => match[1] ?? '')
		.filter((span) => span.length > 0);

/** Every package name the skill imports in code, with the specifier. */
const importsOf = (text: string) =>
	fencesOf(text).flatMap((fence) =>
		[
			...fence.matchAll(
				/import\s+(?:type\s+)?\{([^}]*)\}\s+from\s+'(effect-surrealkit(?:\/[\w-]+)?)'/gu,
			),
		].flatMap((match) =>
			(match[1] ?? '')
				.split(',')
				.map((name) =>
					name
						.trim()
						.replace(/^type\s+/u, '')
						.replace(/\s+as\s+\w+$/u, ''),
				)
				.filter((name) => name.length > 0)
				.map((name) => ({ name, from: match[2] ?? '' })),
		),
	);

/**
 * Every export the skill names in prose: each call (`readAs(table)`), a
 * PascalCase name (`StatementIndexConflict`) or a camelCase name with an
 * inner capital (`checkConstraintErrors`).
 */
const namesOf = (text: string) =>
	spansOf(text).flatMap((span) => {
		const calls = [
			...span.matchAll(/(?<![\w$.:])([A-Za-z_$][\w$]*)\(/gu),
		].map((match) => match[1] ?? '');
		if (calls.length > 0) return calls;
		if (/^[A-Z][a-z][A-Za-z0-9]*$/u.test(span)) return [span];
		if (/^[a-z]+(?:[A-Z][a-z0-9]*)+$/u.test(span)) return [span];
		return [];
	});

/** Every flag of the `effect-surrealkit` command the skill names. */
const flagsOf = (text: string) => {
	const commands = [
		...spansOf(text),
		...fencesOf(text).flatMap((fence) => fence.split('\n')),
	].filter((line) => /^(?:effect-surrealkit|statements)\s/u.test(line));
	const alone = spansOf(text).flatMap((span) => {
		const flag = /^(--[a-z][a-z-]*)/u.exec(span)?.[1];
		return flag === undefined ? [] : [flag];
	});
	return [
		...commands.flatMap((line) =>
			[...line.matchAll(/(?:^|\s)(--[a-z][a-z-]*)/gu)].map(
				(match) => match[1] ?? '',
			),
		),
		...alone,
	];
};

const headingsOf = (text: string) =>
	new Set(
		[...text.matchAll(/^#+\s+(.+)$/gmu)].map((match) =>
			(match[1] ?? '').replaceAll('`', '').trim(),
		),
	);

type Citation =
	| { kind: 'readme'; heading: string }
	| { kind: 'file'; path: string; text?: string }
	| { kind: 'url'; url: string };

/** The citations in a rule's closing parenthesis, `(README, "…"; `test/…`, "…")`. */
const citationsOf = (group: string): Citation[] | string => {
	const cited: Citation[] = [];
	let source: { kind: 'readme' } | { kind: 'file'; path: string } | undefined;
	for (const raw of group.split(';')) {
		const segment = raw.trim();
		const readme = /^README, "([^"]+)"$/u.exec(segment);
		const file = /^`([\w./-]+)`(?:, "([^"]+)")?$/u.exec(segment);
		const quoted = /^"([^"]+)"$/u.exec(segment);
		if (readme?.[1] !== undefined) {
			source = { kind: 'readme' };
			cited.push({ kind: 'readme', heading: readme[1] });
		} else if (file?.[1] !== undefined) {
			source = { kind: 'file', path: file[1] };
			cited.push({
				kind: 'file',
				path: file[1],
				...(file[2] !== undefined && { text: file[2] }),
			});
		} else if (/^https:\/\/surrealdb\.com\/docs\/\S+/u.test(segment)) {
			if (!/read \d{4}-\d{2}-\d{2}$/u.test(segment))
				return `${segment}: a documentation page is cited with the day it was read`;
			source = undefined;
			cited.push({ kind: 'url', url: segment.split(/[\s,]/u)[0] ?? '' });
		} else if (quoted?.[1] !== undefined && source !== undefined) {
			cited.push(
				source.kind === 'readme'
					? { kind: 'readme', heading: quoted[1] }
					: { kind: 'file', path: source.path, text: quoted[1] },
			);
		} else {
			return `${segment}: not a citation`;
		}
	}
	return cited;
};

const groupsOf = (text: string) =>
	[
		...text.matchAll(
			/\(((?:README, |`(?:test\/|package\.json)|https:\/\/)[^()]*)\)/gu,
		),
	].map((match) => match[1] ?? '');

describe('the agent skill', () => {
	it('is at most 120 lines, with a name and a description', () => {
		expect(skill.split('\n').length).toBeLessThanOrEqual(121);
		expect(skill.endsWith('\n')).toBe(true);
		const front = /^---\n([\s\S]*?)\n---\n/u.exec(skill)?.[1] ?? '';
		expect(/^name: effect-surrealkit$/mu.test(front)).toBe(true);
		expect(/^description: "Use .{40,}"$/mu.test(front)).toBe(true);
	});

	it('links every reference file, and every link exists', () => {
		const linked = [
			...skill.matchAll(/\]\(references\/([\w-]+\.md)\)/gu),
		].map((match) => match[1]);
		expect(references.length).toBeGreaterThan(0);
		expect([...new Set(linked)].sort()).toStrictEqual(references);
	});

	it('imports only what each entry point exports', () => {
		const imports = documents.flatMap(({ text }) => importsOf(text));
		expect(imports.length).toBeGreaterThan(0);
		const missing = imports.filter(
			({ name, from }) => !(name in (entries[from] ?? {})),
		);
		expect(missing).toStrictEqual([]);
	});

	it('names only exports of the package', () => {
		const names = [
			...new Set(documents.flatMap(({ text }) => namesOf(text))),
		].sort();
		expect(names.length).toBeGreaterThan(20);
		expect(names).toContain('schemaProblems');
		expect(names).toContain('checkConstraintErrors');
		const missing = names.filter((name) => {
			if (exported.has(name)) return false;
			const owner = elsewhere[name] ?? options[name];
			if (owner === undefined) return true;
			const path =
				elsewhere[name] !== undefined
					? join(repositoryDir, owner)
					: join(packageDir, owner);
			return !readFileSync(path, 'utf8').includes(name);
		});
		expect(missing).toStrictEqual([]);
	});

	it('names only flags the command lists in its usage', () => {
		const flags = [
			...new Set(documents.flatMap(({ text }) => flagsOf(text))),
		].sort();
		expect(flags).toContain('--schema');
		expect(flags).toContain('--check');
		expect(flags).toContain('--verify');
		expect(flags).toContain('--verify-timeout');
		const listed = (flag: string) =>
			new RegExp(`(?:^|\\s)${flag}(?=[\\s,]|$)`, 'mu').test(
				`${USAGE}\n${STATEMENTS_USAGE}`,
			);
		expect(flags.filter((flag) => !listed(flag))).toStrictEqual([]);
	});

	it('ends every rule with a citation of a README section, a test or a SurrealDB page that exists', () => {
		const rules = skill
			.slice(skill.indexOf('\n## Writing statements'))
			.split('\n')
			.filter((line) => line.startsWith('- '));
		expect(rules.length).toBeGreaterThan(20);
		expect(
			rules.filter((rule) => !/\([^()]+\)$/u.test(rule)),
		).toStrictEqual([]);

		const headings = headingsOf(
			readFileSync(join(packageDir, 'README.md'), 'utf8'),
		);
		const problems: string[] = [];
		let count = 0;
		for (const { name, text } of documents) {
			for (const group of groupsOf(text)) {
				const cited = citationsOf(group);
				if (typeof cited === 'string') {
					problems.push(`${name}: ${cited}`);
					continue;
				}
				for (const citation of cited) {
					count += 1;
					if (citation.kind === 'readme') {
						if (!headings.has(citation.heading))
							problems.push(
								`${name}: README has no section "${citation.heading}"`,
							);
					} else if (citation.kind === 'file') {
						const path = join(packageDir, citation.path);
						if (!existsSync(path))
							problems.push(`${name}: no file ${citation.path}`);
						else if (
							citation.text !== undefined &&
							!readFileSync(path, 'utf8').includes(citation.text)
						)
							problems.push(
								`${name}: ${citation.path} does not contain "${citation.text}"`,
							);
					}
				}
			}
		}
		expect(problems).toStrictEqual([]);
		expect(count).toBeGreaterThan(40);
	});
});
