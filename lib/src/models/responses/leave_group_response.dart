import 'package:dart_kafka/dart_kafka.dart';

class LeaveGroupResponse {
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final List<Member>? members;

  LeaveGroupResponse({
    this.throttleTimeMs,
    required this.errorCode,
    this.errorMessage,
    this.members,
  });

  @override
  String toString() {
    return "LeaveGroupResponse -> errorCode: $errorCode, errorMessage: $errorMessage, throttleTimeMs: $throttleTimeMs, members: $members";
  }
}
