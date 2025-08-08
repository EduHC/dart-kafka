import 'component/response/offset_commit_topic_response.dart';

class OffsetCommitResponse {
  OffsetCommitResponse({
    required this.throttleTimeMs,
    required this.topics,
  });
  final int throttleTimeMs;
  List<OffsetCommitTopicResponse> topics;

  @override
  String toString() =>
      'OffsetCommitResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics';
}
