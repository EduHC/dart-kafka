import 'package:dart_kafka/dart_kafka.dart';

class ListOffsetResponse {
  final int throttleTimeMs;
  final List<Topic> topics;

  ListOffsetResponse({required this.throttleTimeMs, required this.topics});

  @override
  String toString() {
    return "ListOffsetResponse -> throttleTimeMs: $throttleTimeMs, topics: $topics";
  }
}
