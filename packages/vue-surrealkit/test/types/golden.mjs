// Runs vue-tsc on the type goldens and compares its errors with the
// `<!-- expect-error: … -->` markers: each marker needs an error in the next
// line (or, when that line opens a tag, anywhere in the tag) whose message
// contains the marker's text, and every error needs a marker.

import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const file = path.join(here, 'Consumer.vue');

const lines = readFileSync(file, 'utf8').split('\n');
const expected = [];
lines.forEach((line, index) => {
	const marker = line.match(/<!-- expect-error: (.*) -->/);
	if (!marker) return;
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
			'vue-tsc',
			'--noEmit',
			'--pretty',
			'false',
			'-p',
			path.join(here, 'tsconfig.json'),
		],
		{ encoding: 'utf8', cwd: path.join(here, '../..') },
	);
} catch (failure) {
	output = `${failure.stdout ?? ''}${failure.stderr ?? ''}`;
}

// One diagnostic per `file(line,col): error TS…: message`, with its
// continuation lines.
const errors = [];
for (const line of output.split('\n')) {
	const head = line.match(/^(.+?)\((\d+),\d+\): error TS\d+: (.*)$/);
	if (head) {
		errors.push({
			file: path.resolve(here, '../..', head[1]),
			line: Number(head[2]),
			message: head[3],
		});
	} else if (errors.length > 0 && line.startsWith(' ')) {
		errors[errors.length - 1].message += `\n${line}`;
	}
}

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
	if (!marked) {
		problems.push(
			`unexpected error at ${error.file}:${error.line}: ${error.message}`,
		);
	}
}

if (problems.length > 0) {
	console.error(problems.join('\n'));
	process.exit(1);
}
console.log(
	`type goldens: ${expected.length} expected errors, all reported, none other`,
);
