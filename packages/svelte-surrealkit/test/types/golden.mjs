// Runs svelte-check on the type goldens and compares its errors with the
// `<!-- expect-error: … -->` markers: each marker needs an error on the next
// line whose message contains the marker's text, and every error needs a
// marker.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const file = path.join(here, 'Consumer.svelte');

const expected = [];
readFileSync(file, 'utf8')
	.split('\n')
	.forEach((line, index) => {
		const marker = line.match(/<!-- expect-error: (.*) -->/);
		if (marker) expected.push({ line: index + 2, text: marker[1] });
	});

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
			error.line === want.line &&
			error.message.includes(want.text),
	);
	if (!hit) problems.push(`missing error at line ${want.line}: ${want.text}`);
}
for (const error of errors) {
	const marked =
		error.file === file &&
		expected.some(
			(want) =>
				want.line === error.line && error.message.includes(want.text),
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
