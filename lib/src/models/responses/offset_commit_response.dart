import '../../../dart_kafka.dart';

class OffsetCommitResponse {
  OffsetCommitResponse({
    required this.throttleTimeMs,
    required this.topics,
  });
  final int throttleTimeMs;
  List<ResOffsetCommitTopic> topics;

  @override
  String toString() =>
      'OffsetCommitResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics';
}
