import {
	Cause,
	Channel,
	Clock,
	Effect,
	Exit,
	References,
	Scope,
	Stream,
	Tracer,
} from 'effect';

// A tracer receives the exit as well as the attributes. Build a fresh exit
// from its status so neither the value nor any part of the cause reaches it.
const spanExit = (exit: Exit.Exit<unknown, unknown>) => {
	if (Exit.isSuccess(exit)) return Exit.void;
	if (Cause.hasInterruptsOnly(exit.cause)) return Exit.interrupt();
	if (Cause.hasDies(exit.cause)) return Exit.die('Defect');
	return Exit.fail('Failure');
};

const end = (
	span: Tracer.Span,
	exit: Exit.Exit<unknown, unknown>,
	clock: Clock.Clock,
	timingEnabled: boolean,
) =>
	Effect.sync(() => {
		if (span.status._tag === 'Ended') return;
		span.end(
			timingEnabled ? clock.currentTimeNanosUnsafe() : 0n,
			spanExit(exit),
		);
	});

const start = (name: string, options: Tracer.SpanOptions) =>
	Effect.gen(function* () {
		const span = yield* Effect.makeSpan(name, options);
		const clock = yield* Clock.Clock;
		const timingEnabled = yield* References.TracerTimingEnabled;
		return { span, clock, timingEnabled };
	});

export const withClientSpan =
	(name: string, options: Tracer.SpanOptions) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		Effect.acquireUseRelease(
			start(name, options),
			({ span }) => effect.pipe(Effect.withParentSpan(span)),
			({ span, clock, timingEnabled }, exit) =>
				end(span, exit, clock, timingEnabled),
		);

/**
 * Runs `stream` in a span. The stream gets a scope of its own. When the
 * channel completes, one finalizer closes that scope and then ends the span,
 * so the stream's finalizers can still annotate it: since Effect 4.0.7 a span
 * ignores attributes once it has ended.
 */
export const withClientStreamSpan =
	(name: string, options: Tracer.SpanOptions) =>
	<A, E, R>(stream: Stream.Stream<A, E, R>) =>
		Stream.fromChannel(
			Channel.fromTransformBracket(
				Effect.fnUntraced(function* (upstream, _scope, channelScope) {
					const { span, streamScope } = yield* Effect.uninterruptible(
						Effect.gen(function* () {
							const { span, clock, timingEnabled } = yield* start(
								name,
								options,
							);
							const streamScope = yield* Scope.make('sequential');
							yield* Scope.addFinalizerExit(
								channelScope,
								(exit) =>
									Scope.close(streamScope, exit).pipe(
										Effect.withParentSpan(span),
										Effect.ensuring(
											end(
												span,
												exit,
												clock,
												timingEnabled,
											),
										),
									),
							);
							return { span, streamScope };
						}),
					);
					const channel = Stream.toChannel(stream).pipe(
						Channel.provideService(Tracer.ParentSpan, span),
					);
					return yield* Channel.toTransform(channel)(
						upstream,
						streamScope,
					);
				}),
			),
		);
