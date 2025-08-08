import 'request_group_topic.dart';

class RequestGroup {
  RequestGroup({
    required this.groupId,
    required this.memberEpoch,
    required this.topics,
    this.memberId,
  });
  final String groupId;
  final String? memberId;
  final int memberEpoch;
  final List<GroupTopic> topics;

  @override
  String toString() =>
      'Group -> id: $groupId, memberId: $memberId, memberEpoch: $memberEpoch, topics: $topics';
}
