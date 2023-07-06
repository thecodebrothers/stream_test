import 'dart:async';

import 'package:meta/meta.dart';
import 'package:test/test.dart';

class TestObserver {
  List<dynamic> _values = <dynamic>[];
  bool _done = false;
  List<dynamic> _errors = <dynamic>[];

  StreamSubscription<dynamic>? sub;

  void observe(Stream<dynamic> stream) {
    sub?.cancel();

    sub = stream.listen(onData, onDone: onDone, onError: onError);
  }

  void onData(dynamic item) {
    _values = [
      ..._values,
      item,
    ];
  }

  void onDone() {
    _done = true;
  }

  void onError(dynamic error, StackTrace stack) {
    _errors = [
      ..._errors,
      error,
    ];
  }

  Future<void> assertNoValues() async {
    await _flushMicrotaskQueue();
    expect(_values, <dynamic>[]);
  }

  Future<void> assertValues(List<dynamic> values) async {
    await _flushMicrotaskQueue();
    expect(_values, values);
  }

  Future<void> assertValue(dynamic value) async {
    await _flushMicrotaskQueue();
    expect(
      _values.length == 1,
      true,
      reason: 'Not enough values. Expected: 1, Actual: ${_values.length}',
    );
    expect(_values.first, value);
  }

  Future<void> assertDone() async {
    await _flushMicrotaskQueue();
    expect(
      _done,
      true,
      reason: 'Expected done, but there was no done event',
    );
  }

  Future<void> assertNotDone() async {
    await _flushMicrotaskQueue();
    expect(
      _done,
      false,
      reason: 'Expected not done, but stream emitted done event',
    );
  }

  Future<void> assertError(dynamic error) async {
    await _flushMicrotaskQueue();

    expect(_errors.isNotEmpty, true);
    expect(_errors.first, error);
  }

  Future<void> assertNoErrors() async {
    await _flushMicrotaskQueue();

    expect(
      _errors.isEmpty,
      true,
      reason: 'Expected no errors. Actual: $_errors',
    );
  }

  Future<void> assertValueAt(int index, dynamic value) async {
    await _flushMicrotaskQueue();
    expect(_values[index], value);
  }

  Future<void> assertStreamMatchers(StreamMatcher matcher) async {
    expect(matcher, Stream.fromIterable(_values));
  }

  Future<void> _flushMicrotaskQueue() async {
    final microtaskCounter =
        (Zone.current['microtaskCounter'] as ZoneMicrotaskCounter?) ??
            ZoneMicrotaskCounter.withCount(100);

    final queueCount = microtaskCounter._count;
    await Future.forEach(
      List.generate(queueCount, (index) => index),
      (element) => Future.microtask(() {}),
    );
    microtaskCounter.clear();
  }
}

extension ObserverExtension on Stream<dynamic> {
  TestObserver test() {
    final observer = TestObserver()..observe(this);

    return observer;
  }
}

class ZoneMicrotaskCounter {
  ZoneMicrotaskCounter();

  ZoneMicrotaskCounter.withCount(this._count);

  int _count = 0;

  void add() => _count++;

  void clear() => _count = 0;
}

void streamTestZone(void Function() body) {
  runZoned(
    body,
    zoneSpecification: ZoneSpecification(
      scheduleMicrotask: (self, parent, zone, f) {
        (self['microtaskCounter'] as ZoneMicrotaskCounter?)?.add();
        parent.scheduleMicrotask(zone, f);
      },
    ),
    zoneValues: {
      'microtaskCounter': ZoneMicrotaskCounter(),
    },
  );
}

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
}) {
  streamTestZone(() {
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
  });
}
