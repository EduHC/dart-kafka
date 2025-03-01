import 'package:dart_kafka/src/models/partition.dart';

class Topic {
  final String topicName;
  final List<Partition> partitions;

  Topic({required this.topicName, required this.partitions});

  @override
  String toString() {
    return "FetchTopic -> topicName: $topicName, FetchPartitions: $partitions";
  }
}
