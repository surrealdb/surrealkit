/** Emitter options: the codec profile, naming, and diagnostic acknowledgements. */
import { Data, Effect, Schema, SchemaIssue } from 'effect';

const PresenceOverride = Schema.Literals([
	'required',
	'optional',
	'removable',
	'resettable',
	'excluded',
]);

const IntProfile = Schema.Literals(['safe-number', 'bigint']);
const DatetimeProfile = Schema.Literals(['sdk', 'effect-datetime']);

/**
 * Per-field settings, keyed `table.path` (`person.age`,
 * `person.address.city`). Presence overrides exist for the cases the
 * contract leaves to the project, such as a `VALUE` with a fallback
 * (`field-value-input-conditional`), and win over the derived presence.
 */
const FieldOptions = Schema.Struct({
	int: Schema.optionalKey(IntProfile),
	datetime: Schema.optionalKey(DatetimeProfile),
	select: Schema.optionalKey(PresenceOverride),
	create: Schema.optionalKey(PresenceOverride),
	merge: Schema.optionalKey(PresenceOverride),
	content: Schema.optionalKey(PresenceOverride),
});

export const EmitterOptions = Schema.Struct({
	/**
	 * The logical store the schema belongs to. Record-id brands are
	 * `<store>/<table>`, so ids of same-named tables in different stores are
	 * different types.
	 */
	store: Schema.String.check(
		Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/),
	),
	/** Integer profile. Default `"safe-number"`. */
	int: Schema.optionalKey(IntProfile),
	/** Datetime profile. Default `"sdk"`. */
	datetime: Schema.optionalKey(DatetimeProfile),
	/** Emit `<Table>Patch` JSON Patch unions (not yet qualified). Default false. */
	patch: Schema.optionalKey(Schema.Boolean),
	/**
	 * Extension of the generated modules' imports of each other. Default
	 * `".js"`, which compilers and bundlers map to the `.ts` sources; `".ts"`
	 * lets the sources load directly where nothing maps them, such as Node's
	 * own type stripping (the consumer then compiles with
	 * `allowImportingTsExtensions` or `rewriteRelativeImportExtensions`).
	 */
	importExtension: Schema.optionalKey(Schema.Literals(['.js', '.ts'])),
	/**
	 * Error diagnostic codes generation may proceed past, each with the reason
	 * it is acceptable. Generation fails on any other error diagnostic.
	 */
	acknowledge: Schema.optionalKey(
		Schema.Record(
			Schema.String.check(
				Schema.isPattern(/^[a-z][a-z0-9]*(-[a-z0-9]+)*$/),
			),
			Schema.String.check(Schema.isMinLength(1)),
		),
	),
	fields: Schema.optionalKey(Schema.Record(Schema.String, FieldOptions)),
	/** PascalCase names for tables whose default names collide. */
	names: Schema.optionalKey(
		Schema.Record(
			Schema.String,
			Schema.String.check(Schema.isPattern(/^[A-Z][A-Za-z0-9]*$/)),
		),
	),
});
export type EmitterOptions = typeof EmitterOptions.Type;
export type FieldOptions = typeof FieldOptions.Type;
export type IntProfile = typeof IntProfile.Type;
export type DatetimeProfile = typeof DatetimeProfile.Type;

/** Emitter options that failed strict decoding. */
export class OptionsError extends Data.TaggedError('OptionsError')<{
	readonly message: string;
}> {}

const formatter = SchemaIssue.makeFormatterDefault();

/** Decodes emitter options strictly (unknown keys are errors). */
export const decodeOptions = (
	input: unknown,
): Effect.Effect<EmitterOptions, OptionsError> =>
	Schema.decodeUnknownEffect(EmitterOptions)(input, {
		onExcessProperty: 'error',
		errors: 'all',
	}).pipe(
		Effect.mapError(
			(error) =>
				new OptionsError({
					message: `invalid emitter options:\n${formatter(error.issue)}`,
				}),
		),
	);
