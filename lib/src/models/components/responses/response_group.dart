import 'package:dart_kafka/src/models/components/responses/offset_fetch_topic.dart';

class ResponseGroup {
  final String name;
  final List<OffsetFetchTopic> topics;
  final int errorCode;
  final String? errorMessage;

  ResponseGroup({
    required this.name,
    required this.topics,
    required this.errorCode,
    this.errorMessage,
  });

  @override
  String toString() {
    return "Group -> name: $name, errorCode: $errorCode, errorMessage: $errorMessage, topics: $topics";
  }
}
