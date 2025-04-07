import 'package:dart_kafka/src/models/components/responses/offset_fetch_topic.dart';
import 'package:dart_kafka/src/models/components/responses/response_group.dart';

class OffsetFetchResponse {
  final int? throttleTimeMs;
  final List<ResponseGroup> groups;
  final List<OffsetFetchTopic>? topics;

  OffsetFetchResponse({
    required this.groups,
    this.throttleTimeMs,
    this.topics,
  });

  @override
  String toString() {
    return "OffsetFetchResponse -> throttleTimeMs: $throttleTimeMs, groups: $groups, topics: $topics";
  }
}
