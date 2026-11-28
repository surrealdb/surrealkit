import { Effect } from 'effect';

import {
	renameParameterTokens,
	scanParameters,
} from '../../statement/rename.js';
import {
	type AnyStatement,
	assertBindingName,
} from '../../statement/statement.js';
import { StatementScopeError } from '../errors.js';
import type { Request, RequestPart } from './transport.js';

interface Entry<B> {
	readonly statement: AnyStatement;
	readonly bindings: B;
}

type Bindings = Request['bindings'];

interface JoinOptions {
	readonly mode: Request['mode'];
	/** One extra slot per side, for example BEGIN and COMMIT. */
	readonly wrap?: { readonly before: string; readonly after: string };
	readonly refuseTransactional: boolean;
}

interface Renamed {
	readonly sql: string;
	readonly names: ReadonlyMap<string, string>;
}

/** Plan renames from statement metadata and text alone, before any codec runs. */
const planRenames = (statements: ReadonlyArray<Entry<unknown>>) =>
	Effect.gen(function* () {
		const used = new Set<string>();
		const shared = new Set<string>();
		const scopes = statements.map(({ statement }) => {
			const names = new Set([
				...Object.keys(statement.params),
				...statement.defines,
			]);
			for (const name of names) {
				if (used.has(name)) shared.add(name);
				used.add(name);
			}
			return names;
		});
		const plans = new Map<number, Renamed>();
		if (shared.size === 0) return plans;
		for (const binding of shared) {
			try {
				assertBindingName(binding);
			} catch (error) {
				if (!(error instanceof RangeError)) throw error;
				const owner = statements.find(
					({ statement }) =>
						Object.hasOwn(statement.params, binding) ||
						statement.defines.includes(binding),
				);
				return yield* Effect.fail(
					new StatementScopeError({
						statement: owner?.statement.name ?? 'join',
						binding,
						message: `${error.message}; cannot rename a shared parameter`,
					}),
				);
			}
		}
		const scanned = [];
		for (const { statement } of statements) {
			const scan = scanParameters(statement.sql);
			if (scan === undefined) {
				return yield* Effect.fail(
					new StatementScopeError({
						statement: statement.name,
						message: `cannot tokenize \`${statement.name}\` with certainty while renaming shared parameters`,
					}),
				);
			}
			const { tokens, recordNames } = scan;
			for (const token of tokens) used.add(token.value);
			for (const binding of recordNames) {
				if (shared.has(binding)) {
					return yield* Effect.fail(
						new StatementScopeError({
							statement: statement.name,
							binding,
							message: `cannot rename shared parameter \`$${binding}\` inside a quoted record literal in \`${statement.name}\``,
						}),
					);
				}
				used.add(binding);
			}
			scanned.push({ statement, tokens });
		}
		for (const [index, { statement, tokens }] of scanned.entries()) {
			const names = new Map<string, string>();
			for (const name of scopes[index] ?? []) {
				if (!shared.has(name)) continue;
				let alias = `${name}__${index + 1}`;
				while (used.has(alias)) alias += '_';
				used.add(alias);
				names.set(name, alias);
			}
			if (names.size !== 0) {
				plans.set(index, {
					sql: renameParameterTokens(statement.sql, tokens, names),
					names,
				});
			}
		}
		return plans;
	});

export const requestPart = (
	statement: AnyStatement,
	bindings: Bindings,
	offset: number,
	slots: number,
	names?: ReadonlyMap<string, string>,
): RequestPart => ({
	statement,
	bindings,
	sent: Object.fromEntries(
		Object.keys(bindings).map((name) => [name, names?.get(name) ?? name]),
	),
	offset,
	slots,
});

/** Join with lazy bindings so a transaction still refuses scope before later codecs run. */
export const joinWith = <E>(
	statements: ReadonlyArray<Entry<Effect.Effect<Bindings, E>>>,
	options: JoinOptions,
): Effect.Effect<Request, E | StatementScopeError> =>
	Effect.gen(function* () {
		if (options.refuseTransactional) {
			const own = statements.find(
				({ statement }) => statement.transactional,
			)?.statement;
			if (own !== undefined)
				return yield* Effect.fail(
					new StatementScopeError({
						statement: own.name,
						message: `\`${own.name}\` writes its own BEGIN, COMMIT or CANCEL, so it cannot run inside a transaction; run it on its own`,
					}),
				);
		}
		const plans = yield* planRenames(statements);
		const bindings = new Map<string, unknown>();
		const sql: Array<string> = [];
		const parts: Array<RequestPart> = [];
		let offset = 0;
		for (const [
			index,
			{ statement, bindings: encoded },
		] of statements.entries()) {
			const planned = plans.get(index);
			const mine = yield* encoded;
			for (const [binding, value] of Object.entries(mine)) {
				const key = planned?.names.get(binding) ?? binding;
				// The SDK's transaction() silently drops this own binding when it
				// combines its inputs, even when we pass a single BoundQuery.
				if (options.mode === 'atomic' && key === '__proto__') {
					return yield* Effect.fail(
						new StatementScopeError({
							statement: statement.name,
							binding,
							message: `the SurrealDB SDK drops \`$${key}\` bound by \`${statement.name}\` when sending an atomic transaction; rename the binding`,
						}),
					);
				}
				bindings.set(key, value);
			}
			const wrap = options.wrap;
			const slots = statement.slots + (wrap === undefined ? 0 : 2);
			parts.push(
				requestPart(statement, mine, offset, slots, planned?.names),
			);
			offset += slots;
			const text = planned?.sql ?? statement.sql;
			if (wrap === undefined) {
				sql.push(text);
			} else {
				sql.push(wrap.before, text, wrap.after);
			}
		}
		return {
			mode: options.mode,
			sql: sql.join('\n'),
			bindings: Object.fromEntries(bindings),
			parts,
		};
	});

export const join = (
	statements: ReadonlyArray<Entry<Bindings>>,
	options: JoinOptions,
): Effect.Effect<Request, StatementScopeError> =>
	joinWith(
		statements.map(({ statement, bindings }) => ({
			statement,
			bindings: Effect.succeed(bindings),
		})),
		options,
	);
