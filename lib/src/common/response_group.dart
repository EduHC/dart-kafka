import '../api/offset_fetch/component/response/offset_fetch_topic.dart';

class ResponseGroup {
  ResponseGroup({
    required this.name,
    required this.topics,
    required this.errorCode,
    this.errorMessage,
  });
  final String name;
  final List<OffsetFetchTopic> topics;
  final int errorCode;
  final String? errorMessage;

  @override
  String toString() =>
      'Group -> name: $name, errorCode: $errorCode, errorMessage: $errorMessage, topics: $topics';
}
