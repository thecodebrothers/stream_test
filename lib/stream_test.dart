import 'dart:async';

import 'package:meta/meta.dart';
import 'package:test/test.dart';
import 'package:test_api/hooks.dart';

/// Records every event of a stream so it can be asserted on later.
///
/// Create one with [ObserverExtension.test]:
///
/// ```dart
/// final observer = myStream.test();
/// await observer.assertValues([1, 2]);
/// ```
///
/// Every `assert*` method first lets already scheduled async work run
/// (microtasks and zero-length timers). For streams that depend on real time
/// (`Future.delayed`, `debounceTime`, ...) call [awaitCount] or [awaitDone]
/// before asserting.
///
/// When created inside a test the subscription is cancelled automatically when
/// the test ends. Otherwise call [dispose].
class TestObserver<T> {
  TestObserver(Stream<T> stream) {
    _subscription = stream.listen(
      (value) => _record(_Event<T>.data(value)),
      onError: (Object error, StackTrace stackTrace) =>
          _record(_Event<T>.error(error)),
      onDone: () => _record(_Event<T>.done()),
    );
    try {
      addTearDown(dispose);
      // addTearDown throws a StateError when called outside of a test.
      // ignore: avoid_catching_errors
    } on StateError {
      // Not inside a test - the caller is responsible for calling dispose().
    }
  }

  late final StreamSubscription<T> _subscription;
  final List<_Event<T>> _events = [];
  final List<void Function()> _listeners = [];

  /// All values received so far, in order.
  List<T> get values => [
        for (final event in _events)
          if (event.kind == _Kind.data) event.value as T,
      ];

  /// All errors received so far, in order.
  List<Object> get errors => [
        for (final event in _events)
          if (event.kind == _Kind.error) event.error!,
      ];

  /// Whether the stream has emitted its done event.
  bool get isDone => _events.any((event) => event.kind == _Kind.done);

  void _record(_Event<T> event) {
    _events.add(event);
    for (final listener in List.of(_listeners)) {
      listener();
    }
  }

  /// Cancels the subscription to the observed stream.
  Future<void> dispose() => _subscription.cancel();

  // ---------------------------------------------------------------------------
  // Waiting
  // ---------------------------------------------------------------------------

  /// Lets already scheduled async work (microtasks, zero-length timers) run.
  ///
  /// Called automatically by every `assert*` method.
  ///
  /// Inside a fake-time zone (`fakeAsync`, Flutter's `testWidgets`) timers
  /// only fire when fake time is advanced, so only microtasks are flushed
  /// there. Advance time yourself (`async.elapse`, `tester.pump`) for
  /// timer-based streams.
  Future<void> settle() =>
      _isFakeAsyncZone() ? _flushMicrotasks() : pumpEventQueue();

  static bool _isFakeAsyncZone() {
    final probe = Timer(Duration.zero, () {})..cancel();
    return probe.runtimeType.toString() == 'FakeTimer';
  }

  static Future<void> _flushMicrotasks() async {
    for (var i = 0; i < 100; i++) {
      await Future<void>.microtask(() {});
    }
  }

  /// Waits until at least [count] values were received, or the stream is done.
  ///
  /// Fails the test if that does not happen within [timeout].
  Future<void> awaitCount(
    int count, {
    Duration timeout = const Duration(seconds: 5),
  }) =>
      _awaitUntil(
        () => values.length >= count || isDone,
        timeout,
        () => 'Expected at least $count values within $timeout, '
            'got ${values.length}: $values',
      );

  /// Waits until the stream is done.
  ///
  /// Fails the test if that does not happen within [timeout].
  Future<void> awaitDone({Duration timeout = const Duration(seconds: 5)}) =>
      _awaitUntil(
        () => isDone,
        timeout,
        () => 'Expected stream to be done within $timeout. Values: $values',
      );

  /// Waits until a value matching [valueOrMatcher] is received.
  ///
  /// Use it when you know the value the stream ends up emitting, but not how
  /// many values come before it. Values received before the call count too.
  ///
  /// Fails the test if the stream finishes without a matching value, or if
  /// none is received within [timeout].
  Future<void> awaitValue(
    Object? valueOrMatcher, {
    Duration timeout = const Duration(seconds: 5),
  }) {
    final matcher = _syncMatcher(valueOrMatcher);
    final description = StringDescription().addDescriptionOf(matcher);
    return _awaitUntil(
      () => values.any((value) => matcher.matches(value, {})),
      timeout,
      () => 'Expected a value matching $description within $timeout. '
          'Values: $values',
      doneMessage: () => 'Stream finished without a value matching '
          '$description. Values: $values',
    );
  }

  /// Waits until an error matching [errorOrMatcher] is received.
  ///
  /// Errors received before the call count too.
  ///
  /// Fails the test if the stream finishes without a matching error, or if
  /// none is received within [timeout].
  Future<void> awaitError(
    Object? errorOrMatcher, {
    Duration timeout = const Duration(seconds: 5),
  }) {
    final matcher = _syncMatcher(errorOrMatcher);
    final description = StringDescription().addDescriptionOf(matcher);
    return _awaitUntil(
      () => errors.any((error) => matcher.matches(error, {})),
      timeout,
      () => 'Expected an error matching $description within $timeout. '
          'Errors: $errors',
      doneMessage: () => 'Stream finished without an error matching '
          '$description. Errors: $errors',
    );
  }

  /// Waits until [condition] returns `true`.
  ///
  /// [condition] is checked right away and again after every event of the
  /// stream, so it should only depend on the observer (`values`, `errors`,
  /// `isDone`). If [condition] throws, the error is rethrown from here.
  ///
  /// ```dart
  /// await observer.awaitUntil((o) => o.values.contains('user'));
  /// ```
  ///
  /// Fails the test if the stream finishes without [condition] becoming
  /// `true`, or if it does not become `true` within [timeout].
  Future<void> awaitUntil(
    bool Function(TestObserver<T> observer) condition, {
    Duration timeout = const Duration(seconds: 5),
  }) =>
      _awaitUntil(
        () => condition(this),
        timeout,
        () => 'Condition not met within $timeout. '
            'Values: $values, errors: $errors',
        doneMessage: () => 'Stream finished without the condition being met. '
            'Values: $values, errors: $errors',
      );

  /// Waits until the stream goes quiet: no event for [idle], or it is done.
  ///
  /// Use it when you don't know how many events will come, nor which one is
  /// the last. This is a heuristic: if the gap between two events is longer
  /// than [idle], it returns too early. Increase [idle] for slow sources (for
  /// example above a `debounceTime`). Prefer [awaitValue], [awaitUntil] or
  /// [awaitDone] when you know the end state.
  ///
  /// Every call takes at least [idle] (unless the stream is already done).
  /// Under `fakeAsync`, advance fake time past [idle].
  ///
  /// Fails the test if the stream does not go quiet within [timeout], for
  /// example for a periodic stream faster than [idle].
  Future<void> awaitIdle({
    Duration idle = const Duration(milliseconds: 100),
    Duration timeout = const Duration(seconds: 5),
  }) async {
    await settle();
    if (isDone) return;

    final completer = Completer<void>();
    Timer? idleTimer;
    void restartIdleTimer() {
      idleTimer?.cancel();
      if (completer.isCompleted) return;
      if (isDone) {
        completer.complete();
        return;
      }
      idleTimer = Timer(idle, () {
        if (!completer.isCompleted) completer.complete();
      });
    }

    _listeners.add(restartIdleTimer);
    restartIdleTimer();
    try {
      await completer.future.timeout(timeout);
    } on TimeoutException {
      fail(
        'Stream did not go quiet for $idle within $timeout. '
        'Received ${values.length} values and ${errors.length} errors.',
      );
    } finally {
      idleTimer?.cancel();
      _listeners.remove(restartIdleTimer);
    }
  }

  static Matcher _syncMatcher(Object? valueOrMatcher) {
    if (valueOrMatcher is StreamMatcher) {
      throw ArgumentError.value(
        valueOrMatcher,
        'valueOrMatcher',
        'Stream matchers (emits...) are not supported here. '
            'Use assertStreamMatchers instead.',
      );
    }
    return wrapMatcher(valueOrMatcher);
  }

  /// Completes when [condition] is `true`, checking it now and after every
  /// event. Fails after [timeout] with [message], or - if [doneMessage] is
  /// given - as soon as the stream is done without [condition] being `true`.
  Future<void> _awaitUntil(
    bool Function() condition,
    Duration timeout,
    String Function() message, {
    String Function()? doneMessage,
  }) async {
    final completer = Completer<void>();
    void check() {
      if (completer.isCompleted) return;
      try {
        if (condition()) {
          completer.complete();
        } else if (doneMessage != null && isDone) {
          completer.completeError(TestFailure(doneMessage()));
        }
      } catch (error, stackTrace) {
        // A throwing condition must surface from the await, not hang it.
        completer.completeError(error, stackTrace);
      }
    }

    check();
    _listeners.add(check);
    try {
      await completer.future.timeout(timeout);
    } on TimeoutException {
      fail(message());
    } finally {
      _listeners.remove(check);
    }
  }

  // ---------------------------------------------------------------------------
  // Assertions
  // ---------------------------------------------------------------------------

  /// Asserts that no values were received.
  Future<void> assertNoValues() async {
    await settle();
    expect(values, isEmpty, reason: 'Expected no values');
  }

  /// Asserts that exactly [expected] values were received, in that order.
  ///
  /// Each element is compared with `==` or, if it is a `Matcher`, matched.
  Future<void> assertValues(List<Object?> expected) async {
    await settle();
    expect(values, expected);
  }

  /// Asserts that exactly one value was received and that it matches [value]
  /// (an object compared with `==`, or a `Matcher`).
  Future<void> assertValue(Object? value) async {
    await settle();
    expect(values, hasLength(1), reason: 'Expected exactly one value');
    expect(values.single, value);
  }

  /// Asserts that the value at [index] matches [value]
  /// (an object compared with `==`, or a `Matcher`).
  Future<void> assertValueAt(int index, Object? value) async {
    await settle();
    expect(
      values.length,
      greaterThan(index),
      reason: 'No value at index $index. Values: $values',
    );
    expect(values[index], value);
  }

  /// Asserts that exactly [count] values were received.
  Future<void> assertValueCount(int count) async {
    await settle();
    expect(values, hasLength(count), reason: 'Values: $values');
  }

  /// Asserts that the stream is done.
  Future<void> assertDone() async {
    await settle();
    expect(isDone, isTrue, reason: 'Expected done event');
  }

  /// Asserts that the stream is not done.
  Future<void> assertNotDone() async {
    await settle();
    expect(isDone, isFalse, reason: 'Expected stream not to be done');
  }

  /// Asserts that exactly one error was received and that it matches [error]
  /// (an object compared with `==`, or a `Matcher` such as `isA<MyError>()`).
  Future<void> assertError(Object error) async {
    await settle();
    expect(errors, hasLength(1), reason: 'Expected exactly one error');
    expect(errors.single, error);
  }

  /// Asserts that exactly [count] errors were received.
  Future<void> assertErrorCount(int count) async {
    await settle();
    expect(errors, hasLength(count), reason: 'Errors: $errors');
  }

  /// Asserts that no errors were received.
  Future<void> assertNoErrors() async {
    await settle();
    expect(errors, isEmpty, reason: 'Expected no errors');
  }

  /// Replays all recorded events - values, errors and done - in their original
  /// order against a `StreamMatcher` such as `emitsInOrder`, `emitsError` or
  /// `emitsDone`.
  ///
  /// Only events recorded so far are replayed. If the matcher needs more
  /// events than were recorded, the assertion fails instead of waiting.
  Future<void> assertStreamMatchers(StreamMatcher matcher) async {
    await settle();
    final snapshot = List.of(_events);
    final controller = StreamController<T>();
    for (final event in snapshot) {
      if (event.kind == _Kind.data) {
        controller.add(event.value as T);
      } else if (event.kind == _Kind.error) {
        controller.addError(event.error!);
      } else {
        unawaited(controller.close());
      }
    }

    var finished = false;
    final result = expectLater(controller.stream, matcher);
    // Handled here so a failure is not reported as unhandled while pumping;
    // it is rethrown by `await result` below.
    unawaited(
      result.then<void>(
        (_) => finished = true,
        onError: (Object _) => finished = true,
      ),
    );
    await pumpEventQueue();

    if (!finished) {
      // The matcher is waiting for events that were never recorded. Close the
      // replayed stream so the pending matcher completes, then fail.
      await controller.close();
      await result.then<void>((_) {}, onError: (Object _) {});
      fail(
        'The stream matcher expected more events than were recorded: '
        '${snapshot.map((event) => event.describe()).toList()}',
      );
    }
    await result;
  }
}

enum _Kind { data, error, done }

class _Event<T> {
  _Event.data(T this.value)
      : kind = _Kind.data,
        error = null;

  _Event.error(Object this.error)
      : kind = _Kind.error,
        value = null;

  _Event.done()
      : kind = _Kind.done,
        value = null,
        error = null;

  final _Kind kind;
  final T? value;
  final Object? error;

  String describe() {
    if (kind == _Kind.data) return 'value($value)';
    if (kind == _Kind.error) return 'error($error)';
    return 'done';
  }
}

extension ObserverExtension<T> on Stream<T> {
  /// Subscribes to this stream and returns a [TestObserver] recording it.
  // ignore: use_to_and_as_if_applicable
  TestObserver<T> test() => TestObserver<T>(this);
}

/// Runs [body] and returns its result.
///
/// This used to count microtasks, which never worked reliably. Assertions now
/// settle the event queue themselves, so this wrapper is not needed.
///
/// The enclosing test is kept running until [body] finishes, so a failure
/// inside [body] is reported in that test even when the returned future is
/// not awaited.
@Deprecated(
  'Not needed anymore - call the test body directly. '
  'Will be removed in a future release.',
)
Future<void> streamTestZone(FutureOr<void> Function() body) {
  OutstandingWork? pendingWork;
  try {
    pendingWork = TestHandle.current.markPending();
  } on OutsideTestException {
    // Not inside a test - nothing to keep alive.
  }
  return Future.sync(body).whenComplete(() => pendingWork?.complete());
}

/// Same as `test` from `package:test`.
@Deprecated(
  'Use `test` from package:test. Will be removed in a future release.',
)
@isTest
void streamTest(
  Object description,
  dynamic Function() body, {
  String? testOn,
  Timeout? timeout,
  Object? skip,
  Object? tags,
  Map<String, dynamic>? onPlatform,
  int? retry,
}) =>
    test(
      description,
      body,
      skip: skip,
      tags: tags,
      timeout: timeout,
      testOn: testOn,
      onPlatform: onPlatform,
      retry: retry,
    );
