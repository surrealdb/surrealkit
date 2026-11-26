/**
 * The comment that opens an authored text, as prose: what `analyze(sql)`
 * returns as `comment`.
 */
import { describe, expect, it } from 'vitest';

import { analyzeSync } from '../src/analyze/index.js';

const comment = (sql: string): string => analyzeSync(sql).comment;

describe('the comment that opens a text', () => {
	it('is empty when the text opens with a statement', () => {
		expect(comment('RETURN 1;')).toBe('');
		expect(comment('RETURN 1; -- after the statement')).toBe('');
		expect(comment('owner = $owner -- a fragment')).toBe('');
	});

	it('reads each line comment without its marker', () => {
		expect(comment('-- one\n-- two\nRETURN 1;')).toBe('one\ntwo');
		expect(comment('# one\n// two\n--three\nRETURN 1;')).toBe(
			'one\ntwo\nthree',
		);
	});

	it('reads a block comment without its delimiters or margin', () => {
		expect(comment('/* one */ RETURN 1;')).toBe('one');
		expect(comment('/**\n * one\n * two\n */\nRETURN 1;')).toBe('one\ntwo');
		expect(comment('/*\n\tone\n\t\ttwo\n*/\nRETURN 1;')).toBe('one\n\ttwo');
	});

	it('keeps a blank line between paragraphs, and nothing around them', () => {
		expect(comment('\n\n-- one\n\n-- two\n--\n-- three\n\nRETURN 1;')).toBe(
			'one\n\ntwo\n\nthree',
		);
		expect(comment('--\n-- one\n--\nRETURN 1;')).toBe('one');
	});

	it('stops at the first token, outside strings', () => {
		expect(comment("-- one\nRETURN '-- two';\n-- three\n")).toBe('one');
	});
});
