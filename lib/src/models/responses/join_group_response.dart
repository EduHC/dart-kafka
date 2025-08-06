import '../components/member.dart';

class JoinGroupResponse {
  JoinGroupResponse({
    required this.throttleTimeMs,
    required this.errorCode,
    required this.generationId,
    required this.leader,
    required this.memberId,
    this.errorMessage,
    this.protocolType,
    this.protocolName,
    this.skipAssignment,
    this.members,
  });
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

  @override
  String toString() =>
      'JoinGroupResponse -> throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, errorMessage: $errorMessage, generationId: $generationId, '
      'protocolType: $protocolType, protocolName: $protocolName, leader: $leader, skipAssignment: $skipAssignment, '
      'memberId: $memberId, members: $members';
}
