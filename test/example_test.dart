import 'package:rxdart/rxdart.dart';
import 'package:stream_test/stream_test.dart';
import 'package:test/test.dart';

import 'divide_by_zero_error.dart';

void main() {
  test(
    'should skip initial source value',
    () async {
      final sourceStream = Stream.value(1);
      const divideBy = 1;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertValue(5);
      await testObserver.assertNotDone();
      await testObserver.assertNoErrors();
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
    'should throw error on divide by 0',
    () async {
      final sourceStream = Stream.value(1);
      const divideBy = 0;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertError(DivideByZeroError());
    },
  );
}
