import '../../common/response_group.dart';
import 'component/response/offset_fetch_topic.dart';

class OffsetFetchResponse {
  OffsetFetchResponse({
    required this.groups,
    this.throttleTimeMs,
    this.topics,
  });
  final int? throttleTimeMs;
  final List<ResponseGroup> groups;
  final List<OffsetFetchTopic>? topics;

  @override
  String toString() =>
      'OffsetFetchResponse -> throttleTimeMs: $throttleTimeMs, groups: $groups, topics: $topics';
}
