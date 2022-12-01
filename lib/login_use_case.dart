import 'package:rxdart/rxdart.dart';

class LoginUseCase {
  LoginUseCase(this._authRepository, this._authEventsService);

  final AuthRepository _authRepository;
  final AuthEventService _authEventsService;

  Stream<String> call({
    required String email,
    required String password,
  }) {
    return _authRepository
        .login(email, password)
        .flatMap(
          (value) => _authEventsService
              .notifyAuthenticated(value)
              .map((event) => value),
        );
  }
}

abstract class AuthRepository {
  Stream<String> login(String email,String password);
}

abstract class AuthEventService {
  Stream<void> notifyAuthenticated(String userId);
}
