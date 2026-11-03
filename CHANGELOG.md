## 0.3.0

- **Breaking:** `TestObserver` is now generic (`TestObserver<T>`), and the public `sub`, `observe`, `onData`,
  `onError` and `onDone` members and the `ZoneMicrotaskCounter` class were removed
- **Breaking:** `assertError` requires exactly one error (it used to check only the first one)
- **Breaking:** example code (`calculate`, `DivideByZeroError`, `LoginUseCase`) moved out of the package, and
  `rxdart` is no longer a dependency
- Fix: assertions wait for all already scheduled async work (`pumpEventQueue`) instead of a fixed number of
  microtasks. Long `async*` chains and zero-length timers no longer lose values
- Fix: `assertStreamMatchers` no longer fails for every matcher. It replays values, errors and done in their original
  order and fails instead of hanging when the matcher needs more events than were recorded
- Fix: `assertValue` and `assertValueAt` give correct failure messages instead of "Not enough values" or a
  `RangeError`
- Fix: subscriptions are cancelled automatically when the test ends
- Inside `fakeAsync` / `testWidgets`, assertions flush microtasks only, because timers need fake time to be advanced
- Add `awaitCount` and `awaitDone` for streams that depend on real time
- Add `awaitValue`, `awaitError` and `awaitUntil` to wait for an end state without knowing how many events come first.
  They fail right away when the stream finishes without a match
- Add `awaitIdle`, which waits until the stream goes quiet, for when neither the count nor the end state is known
- Add `assertValueCount`, `assertErrorCount`, `values`, `errors`, `isDone`, `settle` and `dispose`
- Deprecate `streamTest` and `streamTestZone`. They never worked as intended and are no longer needed.
  `streamTestZone` now keeps the enclosing test running until its body finishes, so failures are reported in the
  right test even when its future is not awaited
- Add a test suite for the library itself

## 0.2.0

- Add `streamTest` and `streamTestZone` to fix hardcoded microtask task count and improve overall performance
- Change maximum dart version to 4.0.0

## 0.1.2

- Always await multiple microtasks to fill async gaps - this is not affecting test times - await microtask and await 99
  microtasks take same amount of time

## 0.1.1

- Remove assert no errors argument
- Update examples and readme

## 0.1.0

- Initial version.
