import 'package:mocktail/mocktail.dart';
import 'package:stream_test/stream_test.dart';
import 'package:test/test.dart';

import 'fixtures/login_use_case.dart';

class MockAuthRepository extends Mock implements AuthRepository {}

class MockAuthEventService extends Mock implements AuthEventService {}

void main() {
  late MockAuthRepository mockAuthRepository;
  late MockAuthEventService mockAuthEventService;
  late LoginUseCase loginUseCase;

  setUp(() {
    mockAuthRepository = MockAuthRepository();
    mockAuthEventService = MockAuthEventService();
    loginUseCase = LoginUseCase(mockAuthRepository, mockAuthEventService);
  });

  test('should login user by default', () async {
    const expectedId = 'userId2';
    when(() => mockAuthRepository.login(any(), any()))
        .thenAnswer((_) => Stream.value(expectedId));
    when(() => mockAuthEventService.notifyAuthenticated(any()))
        .thenAnswer((_) => Stream.value(null));

    final testObserver =
        loginUseCase(email: 'email', password: 'password').test();

    await testObserver.assertValue(expectedId);
    await testObserver.assertDone();
    await testObserver.assertNoErrors();
  });
}
