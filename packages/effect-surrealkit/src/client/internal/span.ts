import {
	Cause,
	Channel,
	Clock,
	Effect,
	Exit,
	References,
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

export const withClientSpan =
	(name: string, options: Tracer.SpanOptions) =>
	<A, E, R>(effect: Effect.Effect<A, E, R>) =>
		Effect.acquireUseRelease(
			Effect.gen(function* () {
				const span = yield* Effect.makeSpan(name, options);
				const clock = yield* Clock.Clock;
				const timingEnabled = yield* References.TracerTimingEnabled;
				return { span, clock, timingEnabled };
			}),
			({ span }) => effect.pipe(Effect.withParentSpan(span)),
			({ span, clock, timingEnabled }, exit) =>
				end(span, exit, clock, timingEnabled),
		);

export const withClientStreamSpan =
	(name: string, options: Tracer.SpanOptions) =>
	<A, E, R>(stream: Stream.Stream<A, E, R>) =>
		Stream.fromChannel(
			Channel.acquireUseRelease(
				Effect.makeSpan(name, options),
				(span) =>
					Stream.toChannel(stream).pipe(
						Channel.provideService(Tracer.ParentSpan, span),
					),
				(span, exit) =>
					Effect.gen(function* () {
						const clock = yield* Clock.Clock;
						const timingEnabled =
							yield* References.TracerTimingEnabled;
						yield* end(span, exit, clock, timingEnabled);
					}),
			),
		);
