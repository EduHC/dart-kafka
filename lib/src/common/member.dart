import 'metadata/member_metadata.dart';

class Member {
  Member({
    required this.memberId,
    this.metadata,
    this.groupInstanceId,
    this.reason,
    this.errorCode,
    this.errorMessage,
  });
  final String memberId;
  final String? groupInstanceId;
  final MemberMetadata? metadata;
  final String? reason;
  final int? errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'Member -> id: $memberId, groupInstanceId: $groupInstanceId, metadata: $metadata, reason: $reason, errorCode: $errorCode, errorMessage: $errorMessage';
}
