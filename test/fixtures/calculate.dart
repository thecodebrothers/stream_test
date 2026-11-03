import 'package:meta/meta.dart';
import 'package:rxdart/rxdart.dart';

@immutable
class DivideByZeroError extends Error {
  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DivideByZeroError && runtimeType == other.runtimeType;

  @override
  int get hashCode => 0;
}

Stream<double> calculate(Stream<int> sourceStream, int divideBy) {
  return sourceStream.skip(1).startWith(5).map((event) {
    if (divideBy == 0) {
      throw DivideByZeroError();
    }

    return event / divideBy;
  });
}
