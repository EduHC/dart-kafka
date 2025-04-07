import 'package:dart_kafka/dart_kafka.dart';

class OffsetCommitResponse {
  final int throttleTimeMs;
  List<ResOffsetCommitTopic> topics;

  OffsetCommitResponse({
    required this.throttleTimeMs,
    required this.topics,
  });

  @override
  String toString() {
    return "OffsetCommitResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics";
  }
}
