import '../../common/assignment.dart';

class SyncGroupResponse {
  SyncGroupResponse({
    required this.errorCode,
    required this.assignment,
    this.throttleTimeMs,
    this.errorMessage,
    this.protocolType,
    this.protocolName,
  });
  final int? throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final String? protocolType;
  final String? protocolName;
  final Assignment? assignment;

  @override
  String toString() =>
      'SyncGroupResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, '
      'protocolType: $protocolType, protocolName: $protocolName, assignment: $assignment';
}
