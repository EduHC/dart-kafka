import 'package:dart_kafka/src/models/topic.dart';

class FetchResponse {
  final int throttleTimeMs;
  final int errorCode;
  final int sessionId;
  final List<Topic> topics;

  FetchResponse({
    required this.throttleTimeMs,
    required this.errorCode,
    required this.sessionId,
    required this.topics,
  });

  @override
  String toString() {
    return "FetchResponse -> sessionId: $sessionId, throttleTimeMs: $throttleTimeMs, errorCode: $errorCode, topics: $topics";
  }
}
