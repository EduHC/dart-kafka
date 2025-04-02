import 'package:dart_kafka/src/models/components/member.dart';

class JoinGroupResponse {
  final int throttleTimeMs;
  final int errorCode;
  final String? errorMessage;
  final int generationId;
  final String? protocolType;
  final String? protocolName;
  final String leader;
  final bool? skipAssignment;
  final String memberId;
  final List<Member>? members;

  JoinGroupResponse(
      {required this.throttleTimeMs,
      required this.errorCode,
      required this.generationId,
      this.errorMessage,
      this.protocolType,
      this.protocolName,
      required this.leader,
      this.skipAssignment,
      required this.memberId,
      this.members});

  @override
  String toString() {
    return "JoinGroupResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, generationId: $generationId, "
        "protocolType: $protocolType, protocolName: $protocolName, leader: $leader, skipAssignment: $skipAssignment, "
        "memberId: $memberId, members: $members";
  }
}
