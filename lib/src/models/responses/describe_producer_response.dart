import 'package:dart_kafka/src/models/topic.dart';

class DescribeProducerResponse {
  final int throttleTime;
  final List<Topic> topics;

  DescribeProducerResponse({required this.throttleTime, required this.topics});

  @override
  String toString() {
    return "DescribeProducerResponse -> throttleTime: $throttleTime, topics: $topics";
  }
}
