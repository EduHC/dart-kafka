import '../../../dart_kafka.dart';

class LeaveGroupResponse {
  LeaveGroupResponse({
    required this.errorCode,
    this.throttleTimeMs,
    this.errorMessage,
    this.members,
  });
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final List<Member>? members;

  @override
  String toString() =>
      'LeaveGroupResponse -> errorCode: $errorCode, errorMessage: $errorMessage, throttleTimeMs: $throttleTimeMs, members: $members';
}
