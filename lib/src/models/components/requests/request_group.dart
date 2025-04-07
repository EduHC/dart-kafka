import 'package:dart_kafka/dart_kafka.dart';

class RequestGroup {
  final String groupId;
  final String? memberId;
  final int memberEpoch;
  final List<GroupTopic> topics;

  RequestGroup({
    required this.groupId,
    required this.memberEpoch,
    required this.topics,
    this.memberId,
  });

  @override
  String toString() {
    return "Group -> id: $groupId, memberId: $memberId, memberEpoch: $memberEpoch, topics: $topics";
  }
}
