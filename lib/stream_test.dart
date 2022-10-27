import 'dart:async';

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
    await Future.microtask(() {}); // await pending callback operations
    expect(_values, <dynamic>[]);
  }

  Future<void> assertValues(List<dynamic> values) async {
    await Future.forEach(values, (element) => Future.microtask(() {}));
    expect(_values, values);
  }

  Future<void> assertValue(dynamic value) async {
    await Future.microtask(() {}); // await pending callback operations
    expect(_values.length == 1, true);
    expect(_values.first, value);
  }

  Future<void> assertDone([int emitsToSkip = 0]) async {
    await Future.forEach(
      List.generate(emitsToSkip + 1, (index) => index),
      (element) => Future.microtask(() {}),
    );
    expect(
      _done,
      true,
      reason: 'Expected don, but there was no done event',
    );
  }

  Future<void> assertNotDone() async {
    await Future.microtask(() {}); // await pending callback operations
    expect(
      _done,
      false,
      reason: 'Expected not done, but stream emitted done event',
    );
  }

  Future<void> assertError(dynamic error) async {
    await Future.microtask(() {}); // await pending callback operations

    expect(_errors.isNotEmpty, true);
    expect(_errors.first, error);
  }

  Future<void> assertNoErrors(dynamic error) async {
    await Future.microtask(() {}); // await pending callback operations

    expect(_errors.isEmpty, true);
  }

  Future<void> assertValueAt(int index, dynamic value) async {
    await Future.forEach(
      List.generate(index + 1, (index) => index),
      (element) => Future.microtask(() {}),
    );
    expect(_values[index], value);
  }

  Future<void> assertStreamMatchers(StreamMatcher matcher) async {
    expect(matcher, Stream.fromIterable(_values));
  }
}

extension ObserverExtension on Stream<dynamic> {
  TestObserver test() {
    final observer = TestObserver()..observe(this);

    return observer;
  }
}
