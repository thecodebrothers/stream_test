# Stream test

Stream testing library

Frustrated by "expect" stream testing approach, which makes "given, when, then" tests a lot harder to do,
we present stream test library. Stream test approach to testing is by explicitly using test observer on you streams.
This is more predictable and clean way of testing streams.
This is heavily inspired by RxJava Test observer:

https://github.com/ReactiveX/RxJava/blob/3.x/src/main/java/io/reactivex/rxjava3/observers/BaseTestConsumer.java
https://github.com/ReactiveX/RxJava/blob/3.x/src/main/java/io/reactivex/rxjava3/observers/TestObserver.java

## Usage

To start writing stream tests just use `.test()` extension on your stream.

To get the best performance out of your tests use `streamTest` as a test function or `streamTestZone` inside your test
function.

## Examples

```dart
  streamTest(
    'should throw error on divide by 0',
        () async {
      final sourceStream = Stream.value(1);
      const divideBy = 0;

      final testObserver = calculate(sourceStream, divideBy).test();

      await testObserver.assertError(DivideByZeroError());
    },
  );

  test(
    'should emits value from given stream source',
        () {
      streamTestZone(() async {
        final sourceStream = PublishSubject<int>();
        const divideBy = 1;

        final testObserver = calculate(sourceStream, divideBy).test();
        sourceStream
          ..add(10)
          ..add(12);

        await testObserver.assertValues([5, 12]);
        await testObserver.assertNotDone();
        await testObserver.assertNoErrors();
      });
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
```
