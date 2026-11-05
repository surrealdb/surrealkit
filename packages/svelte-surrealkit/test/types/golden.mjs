// Runs svelte-check on the type goldens and compares its errors with the
// `<!-- expect-error: … -->` markers: each marker needs an error in the next
// line (or, when that line opens a tag, anywhere in the tag) whose message
// contains the marker's text, and every error needs a marker.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const file = path.join(here, 'Consumer.svelte');

const lines = readFileSync(file, 'utf8').split('\n');
const expected = [];
lines.forEach((line, index) => {
	const marker = line.match(/<!-- expect-error: (.*) -->/);
	if (!marker) return;
	// The marked line, through the end of the tag it opens.
	let last = index + 1;
	if (/^\s*<[A-Za-z]/.test(lines[last] ?? '')) {
		while (last < lines.length && !/>\s*$/.test(lines[last] ?? ''))
			last += 1;
	}
	expected.push({ from: index + 2, to: last + 1, text: marker[1] });
});
const covers = (want, line) => line >= want.from && line <= want.to;

let output = '';
try {
	output = execFileSync(
		'npx',
		[
			'svelte-check',
			'--tsconfig',
			path.join(here, 'tsconfig.json'),
			'--output',
			'machine-verbose',
		],
		{ encoding: 'utf8', cwd: path.join(here, '../..') },
	);
} catch (failure) {
	output = failure.stdout;
}

const errors = output
	.split('\n')
	.filter((line) => /^\d+ \{/.test(line))
	.map((line) => JSON.parse(line.slice(line.indexOf(' ') + 1)))
	.filter((diagnostic) => diagnostic.type === 'ERROR')
	.map((diagnostic) => ({
		file: path.resolve(here, '../..', diagnostic.filename),
		line: diagnostic.start.line + 1,
		message: diagnostic.message,
	}));

const problems = [];
for (const want of expected) {
	const hit = errors.find(
		(error) =>
			error.file === file &&
			covers(want, error.line) &&
			error.message.includes(want.text),
	);
	if (!hit) problems.push(`missing error at line ${want.from}: ${want.text}`);
}
for (const error of errors) {
	const marked =
		error.file === file &&
		expected.some(
			(want) =>
				covers(want, error.line) && error.message.includes(want.text),
		);
	if (!marked)
		problems.push(
			`unexpected error at ${error.file}:${error.line}: ${error.message}`,
		);
}

if (problems.length > 0) {
	console.error(problems.join('\n'));
	process.exit(1);
}
console.log(
	`type goldens: ${expected.length} expected errors, all reported, none other`,
);
