# Stream test

Stream testing library.

Frustrated by the `expect` approach to stream testing, which makes "given, when, then" tests a lot harder to write,
we present the stream test library. You attach a test observer to your stream, then assert on what it recorded.
This is a more predictable and clean way of testing streams.
It is heavily inspired by the RxJava test observer:

https://github.com/ReactiveX/RxJava/blob/3.x/src/main/java/io/reactivex/rxjava3/observers/BaseTestConsumer.java
https://github.com/ReactiveX/RxJava/blob/3.x/src/main/java/io/reactivex/rxjava3/observers/TestObserver.java

## Installation

```yaml
dev_dependencies:
  stream_test: ^0.3.0
```

```dart
import 'package:stream_test/stream_test.dart';
```

## Usage

Call `.test()` on any stream to get a `TestObserver`, then use its `assert*` methods inside a regular `test`:

```dart
test('should emit values from given stream source', () async {
  final sourceStream = PublishSubject<int>();

  final testObserver = calculate(sourceStream, 1).test();
  sourceStream
    ..add(10)
    ..add(12);

  await testObserver.assertValues([5, 12]);
  await testObserver.assertNotDone();
  await testObserver.assertNoErrors();
});
```

The subscription is cancelled automatically when the test ends.

### Available assertions

| Method                           | Checks that                                                         |
|----------------------------------|---------------------------------------------------------------------|
| `assertValues(list)`             | exactly these values were emitted, in this order                    |
| `assertValue(valueOrMatcher)`    | exactly one value was emitted and it matches                        |
| `assertValueAt(index, value)`    | the value at `index` matches                                        |
| `assertValueCount(n)`            | exactly `n` values were emitted                                     |
| `assertNoValues()`               | no values were emitted                                              |
| `assertError(errorOrMatcher)`    | exactly one error was emitted and it matches                        |
| `assertErrorCount(n)`            | exactly `n` errors were emitted                                     |
| `assertNoErrors()`               | no errors were emitted                                              |
| `assertDone()` / `assertNotDone()` | the stream is / is not done                                       |
| `assertStreamMatchers(matcher)`  | all recorded events, in order, match a `StreamMatcher`              |

Values and errors can be compared with `==` or with any matcher:

```dart
await testObserver.assertError(isA<DivideByZeroError>());
await testObserver.assertValue(greaterThan(3));
```

`assertStreamMatchers` replays values, errors and the done event in the order they happened:

```dart
final controller = StreamController<int>();
final testObserver = controller.stream.test();
controller
  ..add(1)
  ..addError(StateError('boom'))
  ..add(2);
await controller.close();

await testObserver.assertStreamMatchers(
  emitsInOrder(<Object>[1, emitsError(isA<StateError>()), 2, emitsDone]),
);
```

Only events recorded so far are replayed. If the matcher needs more events (for example `emitsDone` on a stream that is
still open), the assertion fails instead of waiting.

You can also read the recorded events directly: `values`, `errors` and `isDone`.

### Async callbacks and streams that depend on time

Every `assert*` method first lets already scheduled async work finish: microtasks, `async*` generators, zero-length
timers, and async callbacks that only await other futures, for example mocks set up with
`thenAnswer((_) async => ...)`. In those cases you can assert right away:

```dart
when(() => repository.load(any())).thenAnswer((_) async => 'user');
final testObserver = loader.state.test();

loader.onTap(1); // async callback, not awaited

await testObserver.assertValues(['idle', 'loading', 'user']);
```

Assertions do **not** wait for real time to pass or for real I/O. For `Future.delayed`, `debounceTime`, `interval`,
files, HTTP and similar, wait for the events first.

#### Which wait to use

| You know...                                 | Use                                    |
|---------------------------------------------|----------------------------------------|
| the value the stream ends up with           | `awaitValue(valueOrMatcher)`           |
| the error it ends up with                   | `awaitError(errorOrMatcher)`           |
| some other end condition                    | `awaitUntil((o) => ...)`               |
| that the stream completes                   | `awaitDone()`                          |
| how many values will come                   | `awaitCount(n)`                        |
| none of the above                           | `awaitIdle()` - a heuristic, see below |

```dart
test('should emit only the last query after debounce', () async {
  final query = PublishSubject<String>();

  final testObserver = query.debounceTime(const Duration(milliseconds: 300)).test();
  query
    ..add('f')
    ..add('fo')
    ..add('foo');

  await testObserver.awaitValue('foo');
  await testObserver.assertValues(['foo']);
});
```

`awaitValue`, `awaitError` and `awaitUntil` also count events received before the call. They fail right away if the
stream finishes without a match. `awaitUntil` checks its condition after every event, so it should only look at the
observer (`o.values`, `o.errors`, `o.isDone`).

All waits fail the test after a `timeout` (5 seconds by default).

#### awaitIdle

When you don't know how many events will come, nor which one is the last, `awaitIdle` waits until the stream goes
quiet: no event for `idle` (100 ms by default), or the stream is done.

```dart
loader.onTap(1);
await testObserver.awaitIdle();
await testObserver.assertValues(['idle', 'loading', 'retrying', 'anonymous']);
```

It's a heuristic. If the gap between two events is longer than `idle`, it returns too early. Then `assertValues`
fails, but `assertNoErrors` or `assertNotDone` may pass when they shouldn't. Raise `idle` for slow sources, and prefer
the other waits when you can. Every call takes at least `idle`, unless the stream is already done.

### fakeAsync and testWidgets

Inside a fake-time zone (`fakeAsync` from `package:fake_async`, or Flutter's `testWidgets`), timers only fire when you
advance fake time. There, assertions flush microtasks only. For timer-based streams, advance time first with
`async.elapse(...)` or `tester.pump(...)`, or use `tester.runAsync`. The same applies to the `await*` methods,
including the `idle` window of `awaitIdle` and every `timeout`.

## Migrating from 0.2.x

- `streamTest` and `streamTestZone` are deprecated and no longer needed. Use a plain `test` with an `async` body.
  Existing code keeps working. `streamTestZone` now keeps the enclosing test running until its body finishes, so
  failures are reported in the right test.
- `assertError` now requires exactly one error. Use `assertErrorCount` or `assertStreamMatchers` for streams with
  several errors.
- `TestObserver` is generic: `Stream<T>.test()` returns `TestObserver<T>`.
- The public `sub`, `observe`, `onData`, `onError` and `onDone` members and the `ZoneMicrotaskCounter` class were
  removed.
- The example classes (`calculate`, `DivideByZeroError`, `LoginUseCase`) are no longer part of the package, and
  `rxdart` is no longer a dependency.
