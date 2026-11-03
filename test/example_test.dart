import 'package:rxdart/rxdart.dart';
import 'package:stream_test/stream_test.dart';
import 'package:test/test.dart';

import 'fixtures/calculate.dart';

void main() {
  test(
    'should throw error on divide by 0',
    () async {
      final sourceStream = Stream.value(1);
      const divideBy = 0;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertError(isA<DivideByZeroError>());
    },
  );

  test(
    'should emits value from given stream source',
    () async {
      final sourceStream = PublishSubject<int>();
      const divideBy = 1;

      final testObserver = calculate(sourceStream, divideBy).test();
      sourceStream
        ..add(10)
        ..add(12);

      await testObserver.assertValues([5, 12]);
      await testObserver.assertNotDone();
      await testObserver.assertNoErrors();
    },
  );

  test(
    'should skip initial source value',
    () async {
      final sourceStream = Stream.value(1);
      const divideBy = 1;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertValue(5);
      await testObserver.assertDone();
      await testObserver.assertNoErrors();
    },
  );

  // --- New in 0.3.0 ----------------------------------------------------------

  test(
    'should emit only the last query after debounce',
    () async {
      final query = PublishSubject<String>();

      final testObserver =
          query.debounceTime(const Duration(milliseconds: 50)).test();
      query
        ..add('f')
        ..add('fo')
        ..add('foo');

      await testObserver.awaitCount(1);
      await testObserver.assertValue('foo');
    },
  );

  test(
    'should emit an error for every source value, then complete',
    () async {
      final sourceStream = Stream.fromIterable([1, 2]);
      const divideBy = 0;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertStreamMatchers(
        emitsInOrder(<Object>[
          emitsError(isA<DivideByZeroError>()),
          emitsError(isA<DivideByZeroError>()),
          emitsDone,
        ]),
      );
    },
  );
}
