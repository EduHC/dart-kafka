import '../topic.dart';

class DescribeProducerResponse {
  DescribeProducerResponse({required this.throttleTime, required this.topics});
  final int throttleTime;
  final List<Topic> topics;

  @override
  String toString() =>
      'DescribeProducerResponse -> throttleTime: $throttleTime, topics: $topics';
}
