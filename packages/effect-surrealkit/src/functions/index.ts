/**
 * SurrealDB's built-in functions as typed expressions, one namespace per
 * family (`string.isEmail(…)` is `string::is_email(…)`). Each function
 * checks its arguments against the kinds SurrealDB takes and yields an
 * `Expr` whose codec decodes what the engine returns, so it composes with
 * every builder of `effect-surrealkit/statement`.
 *
 * Generated from `signatures.json` by `scripts/generate-functions.mjs`.
 */
export * from './generated/index.js';
export type { Arg, CodecOfArg, In, Inline, ItemCodec } from './internal.js';
