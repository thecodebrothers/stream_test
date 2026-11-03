// Tests of the stream_test library itself.
//
// Every assertion is checked both ways: it passes on matching input and fails
// with a `TestFailure` (not a crash, not a silent pass) on non-matching input.

// ignore_for_file: deprecated_member_use_from_same_package

import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:meta/meta.dart';
import 'package:rxdart/rxdart.dart';
import 'package:stream_test/stream_test.dart';
import 'package:test/test.dart';

/// Asserts that [assertion] fails the test with a message containing
/// [messageContains] (if given).
Future<void> expectFailure(
  Future<void> assertion, [
  String? messageContains,
]) =>
    expectLater(
      assertion,
      throwsA(
        isA<TestFailure>().having(
          (failure) => failure.message,
          'message',
          contains(messageContains ?? ''),
        ),
      ),
    );

/// Stream where every value is separated by an async gap.
Stream<int> asyncChain(int length) async* {
  for (var i = 0; i < length; i++) {
    await Future<void>.value();
    yield i;
  }
}

@immutable
class _TestError implements Exception {
  const _TestError(this.id);

  final int id;

  @override
  bool operator ==(Object other) => other is _TestError && other.id == id;

  @override
  int get hashCode => id;

  @override
  String toString() => '_TestError($id)';
}

/// Emits a number of values that a test does not know up front, each after
/// [gap], then stays open.
PublishSubject<int> _emitSpacedOut(int count, Duration gap) {
  final subject = PublishSubject<int>();
  () async {
    for (var i = 0; i < count; i++) {
      await Future<void>.delayed(gap);
      subject.add(i);
    }
  }();
  return subject;
}

abstract class _UserRepository {
  Future<String> load(int id);
}

/// Typical async-callback driven class: `onTap` is fire-and-forget and emits
/// a varying number of states.
class _UserLoader {
  _UserLoader(this._repository);

  final _UserRepository _repository;
  final _state = BehaviorSubject<String>.seeded('idle');

  Stream<String> get state => _state.stream;

  Future<void> onTap(int id) async {
    _state.add('loading');
    try {
      final user = await _repository.load(id);
      if (user.isEmpty) _state.add('retrying');
      _state.add(user.isEmpty ? 'anonymous' : user);
    } catch (error) {
      _state.addError(error);
    }
  }
}

class _DelayedRepository implements _UserRepository {
  _DelayedRepository(this._result);

  final FutureOr<String> Function() _result;

  @override
  Future<String> load(int id) =>
      Future.delayed(const Duration(milliseconds: 30), _result);
}

void main() {
  group('collecting events', () {
    test('records synchronous values', () async {
      final observer = Stream.fromIterable([1, 2, 3]).test();

      await observer.assertValues([1, 2, 3]);
    });

    test('records values emitted synchronously by a subject', () async {
      final subject = PublishSubject<int>();
      final observer = subject.test();

      subject
        ..add(1)
        ..add(2);

      await observer.assertValues([1, 2]);
      await observer.assertNotDone();
    });

    test('records values scheduled with a zero-length timer', () async {
      final observer = Stream.fromFuture(Future(() => 1)).test();

      await observer.assertValue(1);
    });

    test('records a long async* chain (200 async gaps)', () async {
      final observer = asyncChain(200).test();

      await observer.assertValues(List.generate(200, (i) => i));
      await observer.assertDone();
    });

    test('records values, errors and done in their original order', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();

      controller
        ..add(1)
        ..addError(const _TestError(1))
        ..add(2);
      unawaited(controller.close());

      await observer.assertStreamMatchers(
        emitsInOrder(<Object>[
          1,
          emitsError(const _TestError(1)),
          2,
          emitsDone,
        ]),
      );
    });

    test('exposes values, errors and isDone', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();

      controller
        ..add(1)
        ..addError(const _TestError(1));
      await observer.settle();

      expect(observer.values, [1]);
      expect(observer.errors, [const _TestError(1)]);
      expect(observer.isDone, isFalse);

      await controller.close();
      expect(observer.isDone, isTrue);
    });

    test('is typed after the observed stream', () {
      expect(Stream.value('a').test(), isA<TestObserver<String>>());
    });

    test('does not wait for real delays without awaitCount', () async {
      // Documents the limitation: assertions only settle already scheduled
      // work. Real time needs awaitCount / awaitDone.
      final observer = Stream.fromFuture(
        Future.delayed(const Duration(milliseconds: 200), () => 1),
      ).test();

      await observer.assertNoValues();
      await observer.awaitCount(1);
      await observer.assertValue(1);
    });
  });

  group('awaitCount', () {
    test('waits for values that arrive after a real delay', () async {
      final observer = Stream.fromFuture(
        Future.delayed(const Duration(milliseconds: 20), () => 1),
      ).test();

      await observer.awaitCount(1);

      await observer.assertValue(1);
    });

    test('waits for debounced values', () async {
      final query = PublishSubject<String>();
      final observer =
          query.debounceTime(const Duration(milliseconds: 20)).test();

      query
        ..add('f')
        ..add('fo')
        ..add('foo');
      await observer.awaitCount(1);

      await observer.assertValue('foo');
    });

    test('returns immediately when enough values were received', () async {
      final observer = Stream.fromIterable([1, 2]).test();
      await observer.settle();

      await observer.awaitCount(2, timeout: Duration.zero);
    });

    test('returns when the stream ends with fewer values', () async {
      final observer = Stream.fromIterable([1]).test();

      await observer.awaitCount(5);

      await observer.assertValues([1]);
      await observer.assertDone();
    });

    test('fails after the timeout', () async {
      final observer = PublishSubject<int>().test();

      await expectFailure(
        observer.awaitCount(1, timeout: const Duration(milliseconds: 20)),
        'Expected at least 1 values',
      );
    });
  });

  group('awaitDone', () {
    test('waits for a stream that completes after a real delay', () async {
      final observer = Stream<int>.fromFuture(
        Future.delayed(const Duration(milliseconds: 20), () => 1),
      ).test();

      await observer.awaitDone();

      await observer.assertDone();
    });

    test('fails after the timeout', () async {
      final observer = PublishSubject<int>().test();

      await expectFailure(
        observer.awaitDone(timeout: const Duration(milliseconds: 20)),
        'Expected stream to be done',
      );
    });
  });

  group('awaitValue', () {
    test('returns immediately for an already received value', () async {
      final observer = Stream.fromIterable([1, 2]).test();
      await observer.settle();

      await observer.awaitValue(1, timeout: Duration.zero);
    });

    test('waits for a value that arrives after a real delay', () async {
      final observer =
          _emitSpacedOut(3, const Duration(milliseconds: 10)).test();

      await observer.awaitValue(2);

      await observer.assertValues([0, 1, 2]);
    });

    test('accepts a matcher', () async {
      final observer =
          _emitSpacedOut(5, const Duration(milliseconds: 5)).test();

      await observer.awaitValue(greaterThan(2));

      await observer.assertValueAt(3, 3);
    });

    test('fails fast when the stream finishes without a match', () async {
      final observer = Stream.fromIterable([1, 2]).test();
      final stopwatch = Stopwatch()..start();

      await expectFailure(
        observer.awaitValue(3, timeout: const Duration(seconds: 30)),
        'Stream finished without a value matching <3>. Values: [1, 2]',
      );
      expect(stopwatch.elapsed, lessThan(const Duration(seconds: 1)));
    });

    test('fails after the timeout on an open stream', () async {
      final observer = PublishSubject<int>().test();

      await expectFailure(
        observer.awaitValue(1, timeout: const Duration(milliseconds: 20)),
        'Expected a value matching <1>',
      );
    });

    test('rejects stream matchers', () {
      final observer = Stream.value(1).test();

      expect(
        () => observer.awaitValue(emits(1)),
        throwsArgumentError,
      );
    });
  });

  group('awaitError', () {
    test('returns immediately for an already received error', () async {
      final observer = Stream<int>.error(const _TestError(1)).test();
      await observer.settle();

      await observer.awaitError(const _TestError(1), timeout: Duration.zero);
    });

    test('waits for an error that arrives after a real delay', () async {
      final observer = Stream<int>.fromFuture(
        Future.delayed(
          const Duration(milliseconds: 20),
          () => throw const _TestError(1),
        ),
      ).test();

      await observer.awaitError(isA<_TestError>());

      await observer.assertError(const _TestError(1));
    });

    test('fails fast when the stream finishes without a match', () async {
      final observer = Stream<int>.error(const _TestError(1)).test();

      await expectFailure(
        observer.awaitError(
          const _TestError(2),
          timeout: const Duration(seconds: 30),
        ),
        'Stream finished without an error matching',
      );
    });

    test('fails after the timeout on an open stream', () async {
      final observer = PublishSubject<int>().test();

      await expectFailure(
        observer.awaitError(
          isA<StateError>(),
          timeout: const Duration(milliseconds: 20),
        ),
        'Expected an error matching',
      );
    });
  });

  group('awaitUntil', () {
    test('waits until the condition over values is true', () async {
      final observer =
          _emitSpacedOut(5, const Duration(milliseconds: 5)).test();

      await observer.awaitUntil((o) => o.values.length >= 2);

      expect(observer.values, [0, 1]);
    });

    test('rethrows an error thrown by the condition instead of hanging',
        () async {
      final observer =
          _emitSpacedOut(1, const Duration(milliseconds: 5)).test();

      await expectLater(
        observer.awaitUntil(
          (o) =>
              o.values.isNotEmpty ? throw StateError('bad condition') : false,
          timeout: const Duration(seconds: 30),
        ),
        throwsA(isA<StateError>()),
      );
    });

    test('fails fast when the stream finishes without a match', () async {
      final observer = Stream.value(1).test();

      await expectFailure(
        observer.awaitUntil(
          (o) => o.values.contains(2),
          timeout: const Duration(seconds: 30),
        ),
        'Stream finished without the condition being met',
      );
    });

    test('fails after the timeout on an open stream', () async {
      final observer = PublishSubject<int>().test();

      await expectFailure(
        observer.awaitUntil(
          (o) => o.values.isNotEmpty,
          timeout: const Duration(milliseconds: 20),
        ),
        'Condition not met within',
      );
    });
  });

  group('awaitIdle', () {
    test('collects an unknown number of spaced-out values', () async {
      final observer =
          _emitSpacedOut(4, const Duration(milliseconds: 10)).test();

      await observer.awaitIdle(idle: const Duration(milliseconds: 50));

      await observer.assertValues([0, 1, 2, 3]);
      await observer.assertNotDone();
    });

    test('returns immediately for a finished stream', () async {
      final observer = Stream.fromIterable([1, 2]).test();
      final stopwatch = Stopwatch()..start();

      await observer.awaitIdle(idle: const Duration(seconds: 10));

      expect(stopwatch.elapsed, lessThan(const Duration(seconds: 1)));
      await observer.assertValues([1, 2]);
    });

    test('returns when the stream finishes before going quiet', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();
      final stopwatch = Stopwatch()..start();

      unawaited(
        Future<void>.delayed(const Duration(milliseconds: 10), () async {
          controller.add(1);
          await controller.close();
        }),
      );
      await observer.awaitIdle(idle: const Duration(seconds: 10));

      expect(stopwatch.elapsed, lessThan(const Duration(seconds: 1)));
      await observer.assertValues([1]);
    });

    test('restarts the quiet window on errors too', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();

      unawaited(() async {
        await Future<void>.delayed(const Duration(milliseconds: 30));
        controller.addError(const _TestError(1));
        await Future<void>.delayed(const Duration(milliseconds: 30));
        controller.add(1);
      }());
      await observer.awaitIdle(idle: const Duration(milliseconds: 50));

      await observer.assertError(const _TestError(1));
      await observer.assertValues([1]);
    });

    test('fails for a stream that never goes quiet', () async {
      final observer = Stream.periodic(
        const Duration(milliseconds: 5),
        (i) => i,
      ).test();

      await expectFailure(
        observer.awaitIdle(
          idle: const Duration(milliseconds: 50),
          timeout: const Duration(milliseconds: 200),
        ),
        'Stream did not go quiet',
      );
    });

    test('returns too early when a gap is longer than the idle window',
        () async {
      // Documents the limitation of the heuristic.
      final observer =
          _emitSpacedOut(2, const Duration(milliseconds: 150)).test();

      await observer.awaitIdle(idle: const Duration(milliseconds: 20));

      await observer.assertNoValues();
    });

    test('works under fakeAsync when time is advanced past the window', () {
      fakeAsync((async) {
        final observer = _emitSpacedOut(3, const Duration(seconds: 1)).test();

        var idle = false;
        observer
            .awaitIdle(
              idle: const Duration(seconds: 2),
              timeout: const Duration(seconds: 30),
            )
            .then((_) => idle = true);
        async.elapse(const Duration(seconds: 4));
        expect(idle, isFalse);
        async.elapse(const Duration(seconds: 1));

        expect(idle, isTrue);
        expect(observer.values, [0, 1, 2]);
      });
    });
  });

  group('async callbacks with an unknown number of events', () {
    test('awaitValue waits for the final state', () async {
      final loader = _UserLoader(_DelayedRepository(() => 'user'));
      final observer = loader.state.test();

      unawaited(loader.onTap(1));
      await observer.awaitValue('user');

      await observer.assertValues(['idle', 'loading', 'user']);
    });

    test('awaitIdle collects every state, however many there are', () async {
      final loader = _UserLoader(_DelayedRepository(() => ''));
      final observer = loader.state.test();

      unawaited(loader.onTap(1));
      await observer.awaitIdle();

      await observer.assertValues(['idle', 'loading', 'retrying', 'anonymous']);
      await observer.assertNoErrors();
    });

    test('awaitError waits for a failing callback', () async {
      final loader =
          _UserLoader(_DelayedRepository(() => throw const _TestError(1)));
      final observer = loader.state.test();

      unawaited(loader.onTap(1));
      await observer.awaitError(isA<_TestError>());

      await observer.assertValues(['idle', 'loading']);
    });
  });

  group('assertNoValues', () {
    test('passes for a stream without values', () async {
      await const Stream<int>.empty().test().assertNoValues();
    });

    test('fails when a value was received', () async {
      await expectFailure(
        Stream.value(1).test().assertNoValues(),
        'Expected no values',
      );
    });
  });

  group('assertValues', () {
    test('passes for the same values in the same order', () async {
      await Stream.fromIterable([1, 2]).test().assertValues([1, 2]);
    });

    test('accepts matchers as elements', () async {
      await Stream.fromIterable([1, 7])
          .test()
          .assertValues([isA<int>(), greaterThan(5)]);
    });

    test('fails for different values', () async {
      await expectFailure(Stream.value(1).test().assertValues([999]));
    });

    test('fails for the same values in a different order', () async {
      await expectFailure(
        Stream.fromIterable([1, 2]).test().assertValues([2, 1]),
      );
    });

    test('fails when values are missing', () async {
      await expectFailure(Stream.value(1).test().assertValues([1, 2]));
    });
  });

  group('assertValue', () {
    test('passes for a single matching value', () async {
      await Stream.value(1).test().assertValue(1);
    });

    test('accepts a matcher', () async {
      await Stream.value(5).test().assertValue(greaterThan(3));
    });

    test('fails when there are no values', () async {
      await expectFailure(
        const Stream<int>.empty().test().assertValue(1),
        'Expected exactly one value',
      );
    });

    test('fails when there are too many values', () async {
      await expectFailure(
        Stream.fromIterable([1, 2]).test().assertValue(1),
        'Expected exactly one value',
      );
    });

    test('fails for a different value', () async {
      await expectFailure(Stream.value(1).test().assertValue(2));
    });
  });

  group('assertValueAt', () {
    test('passes for a matching value at the index', () async {
      await Stream.fromIterable([1, 2]).test().assertValueAt(1, 2);
    });

    test('fails for a different value at the index', () async {
      await expectFailure(
        Stream.fromIterable([1, 2]).test().assertValueAt(1, 1),
      );
    });

    test('fails with a test failure, not a RangeError, when out of range',
        () async {
      await expectFailure(
        Stream.value(1).test().assertValueAt(3, 1),
        'No value at index 3',
      );
    });
  });

  group('assertValueCount', () {
    test('passes for the right number of values', () async {
      await Stream.fromIterable([1, 2]).test().assertValueCount(2);
    });

    test('fails for a different number of values', () async {
      await expectFailure(Stream.value(1).test().assertValueCount(2));
    });
  });

  group('assertDone / assertNotDone', () {
    test('assertDone passes for a completed stream', () async {
      await Stream.value(1).test().assertDone();
    });

    test('assertDone fails for an open stream', () async {
      await expectFailure(
        PublishSubject<int>().test().assertDone(),
        'Expected done event',
      );
    });

    test('assertNotDone passes for an open stream', () async {
      await PublishSubject<int>().test().assertNotDone();
    });

    test('assertNotDone fails for a completed stream', () async {
      await expectFailure(
        Stream.value(1).test().assertNotDone(),
        'Expected stream not to be done',
      );
    });
  });

  group('assertError', () {
    test('passes for an equal error', () async {
      await Stream<int>.error(const _TestError(1))
          .test()
          .assertError(const _TestError(1));
    });

    test('accepts a type matcher', () async {
      await Stream<int>.error(StateError('boom'))
          .test()
          .assertError(isA<StateError>());
    });

    test('fails for a different error', () async {
      await expectFailure(
        Stream<int>.error(const _TestError(1))
            .test()
            .assertError(const _TestError(2)),
      );
    });

    test('fails when there is no error', () async {
      await expectFailure(
        Stream.value(1).test().assertError(isA<StateError>()),
        'Expected exactly one error',
      );
    });

    test('fails when there is more than one error', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();
      controller
        ..addError(const _TestError(1))
        ..addError(const _TestError(2));

      await expectFailure(
        observer.assertError(const _TestError(1)),
        'Expected exactly one error',
      );
    });
  });

  group('assertErrorCount', () {
    test('passes for the right number of errors', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();
      controller
        ..addError(const _TestError(1))
        ..addError(const _TestError(2));

      await observer.assertErrorCount(2);
    });

    test('fails for a different number of errors', () async {
      await expectFailure(Stream.value(1).test().assertErrorCount(1));
    });
  });

  group('assertNoErrors', () {
    test('passes for a stream without errors', () async {
      await Stream.value(1).test().assertNoErrors();
    });

    test('fails when an error was received', () async {
      await expectFailure(
        Stream<int>.error(const _TestError(1)).test().assertNoErrors(),
        'Expected no errors',
      );
    });
  });

  group('assertStreamMatchers', () {
    test('passes for a matching emitsInOrder', () async {
      await Stream.fromIterable([1, 2])
          .test()
          .assertStreamMatchers(emitsInOrder(<Object>[1, 2]));
    });

    test('passes for a prefix of an open stream', () async {
      final subject = PublishSubject<int>();
      final observer = subject.test();
      subject
        ..add(1)
        ..add(2);

      await observer.assertStreamMatchers(emitsInOrder(<Object>[1, 2]));
    });

    test('fails for a non-matching emitsInOrder', () async {
      await expectFailure(
        Stream.fromIterable([1, 2])
            .test()
            .assertStreamMatchers(emitsInOrder(<Object>[2, 1])),
      );
    });

    test('can match an error', () async {
      await Stream<int>.error(const _TestError(1))
          .test()
          .assertStreamMatchers(emitsError(const _TestError(1)));
    });

    test('fails emitsError when a value came first', () async {
      final controller = StreamController<int>();
      final observer = controller.stream.test();
      controller
        ..add(1)
        ..addError(const _TestError(1));

      await expectFailure(
        observer.assertStreamMatchers(emitsError(const _TestError(1))),
      );
    });

    test('passes emitsDone for a completed stream', () async {
      await const Stream<int>.empty().test().assertStreamMatchers(emitsDone);
    });

    test('fails emitsDone for an open stream without hanging', () async {
      await expectFailure(
        PublishSubject<int>().test().assertStreamMatchers(emitsDone),
        'expected more events than were recorded',
      );
    });

    test('fails when the matcher needs more events than recorded', () async {
      final subject = PublishSubject<int>();
      final observer = subject.test();
      subject.add(1);

      await expectFailure(
        observer.assertStreamMatchers(emitsInOrder(<Object>[1, 2])),
        'expected more events than were recorded: [value(1)]',
      );
    });
  });

  // Tear-downs run in reverse order of registration, so a tear-down registered
  // before `.test()` runs after the observer has been disposed.
  group('subscription lifecycle', () {
    test('stays subscribed while the test runs, cancels when it ends',
        () async {
      final controller = StreamController<int>();
      addTearDown(() => expect(controller.hasListener, isFalse));

      controller.stream.test();
      await pumpEventQueue();

      expect(controller.hasListener, isTrue);
    });

    test('stops a periodic stream when the test ends', () async {
      var ticks = 0;
      addTearDown(() async {
        final ticksAfterTest = ticks;
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(ticks, ticksAfterTest);
      });

      final observer = Stream.periodic(
        const Duration(milliseconds: 5),
        (i) => ticks++,
      ).test();

      await observer.awaitCount(2);
    });

    test('can be created outside of a test and disposed manually', () async {
      final controller = StreamController<int>();
      late TestObserver<int> observer;

      Zone.root.run(() => observer = controller.stream.test());
      controller.add(1);
      await observer.assertValue(1);

      await observer.dispose();
      expect(controller.hasListener, isFalse);
    });
  });

  group('inside fakeAsync (as used by testWidgets)', () {
    test('assertions complete after flushing microtasks only', () {
      fakeAsync((async) {
        final controller = StreamController<int>();
        final observer = controller.stream.test();
        controller.add(1);

        var checked = false;
        observer.assertValue(1).then((_) => checked = true);
        async.flushMicrotasks();

        expect(checked, isTrue);
      });
    });

    test('assertions report failures', () {
      fakeAsync((async) {
        Object? failure;
        Stream.value(1).test().assertValue(2).catchError(
              (Object error) => failure = error,
            );
        async.flushMicrotasks();

        expect(failure, isA<TestFailure>());
      });
    });

    test('timer-based values arrive when fake time is advanced', () {
      fakeAsync((async) {
        final observer = Stream.fromFuture(
          Future.delayed(const Duration(seconds: 10), () => 1),
        ).test();

        var checked = false;
        async.elapse(const Duration(seconds: 10));
        observer.assertValue(1).then((_) => checked = true);
        async.flushMicrotasks();

        expect(checked, isTrue);
      });
    });

    test('awaitCount completes when fake time is advanced', () {
      fakeAsync((async) {
        final observer = Stream.fromFuture(
          Future.delayed(const Duration(seconds: 10), () => 1),
        ).test();

        var counted = false;
        observer.awaitCount(1, timeout: const Duration(seconds: 30)).then(
              (_) => counted = true,
            );
        async.elapse(const Duration(seconds: 10));

        expect(counted, isTrue);
      });
    });

    test('awaitCount times out in fake time', () {
      fakeAsync((async) {
        final observer = PublishSubject<int>().test();

        Object? failure;
        observer.awaitCount(1).catchError((Object error) => failure = error);
        async.elapse(const Duration(seconds: 5));

        expect(failure, isA<TestFailure>());
      });
    });
  });

  group('deprecated helpers', () {
    streamTest('streamTest still registers a working test', () async {
      await Stream.value(1).test().assertValue(1);
    });

    test('streamTestZone runs the body and returns its future', () async {
      var finished = false;

      await streamTestZone(() async {
        await Future<void>.delayed(const Duration(milliseconds: 10));
        finished = true;
      });

      expect(finished, isTrue);
    });

    test('streamTestZone propagates failures from the body', () async {
      await expectFailure(
        streamTestZone(() async {
          await Future<void>.delayed(const Duration(milliseconds: 10));
          fail('inside zone');
        }),
        'inside zone',
      );
    });

    // 0.2.0 README pattern: non-async callback, returned future discarded.
    test('streamTestZone keeps a non-async test running until the body ends',
        () {
      var finished = false;
      addTearDown(() => expect(finished, isTrue));

      streamTestZone(() async {
        await Future<void>.delayed(const Duration(milliseconds: 20));
        finished = true;
      });
    });

    test('streamTestZone works outside of a test', () async {
      var finished = false;

      await Zone.root.run(
        () => streamTestZone(() async {
          await Future<void>.delayed(const Duration(milliseconds: 10));
          finished = true;
        }),
      );

      expect(finished, isTrue);
    });

    test('streamTestZone works with a long async* chain', () async {
      await streamTestZone(() async {
        final observer = asyncChain(10).test();

        await observer.assertValues(List.generate(10, (i) => i));
      });
    });
  });
}
