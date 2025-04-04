import 'package:dart_kafka/dart_kafka.dart';

class Member {
  final String memberId;
  final String? groupInstanceId;
  final MemberMetadata? metadata;
  final String? reason;
  final int? errorCode;
  final String? errorMessage;

  Member({
    required this.memberId,
    this.metadata,
    this.groupInstanceId,
    this.reason,
    this.errorCode,
    this.errorMessage,
  });

  @override
  String toString() {
    return "Member -> id: $memberId, groupInstanceId: $groupInstanceId, metadata: $metadata, reason: $reason, errorCode: $errorCode, errorMessage: $errorMessage";
  }
}
