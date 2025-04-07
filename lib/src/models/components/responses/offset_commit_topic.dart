import 'package:dart_kafka/dart_kafka.dart';

class ResOffsetCommitTopic {
  final String name;
  final List<ResOffsetCommitPartition> partitions;

  ResOffsetCommitTopic({
    required this.name,
    required this.partitions,
  });

  @override
  String toString() {
    return "ResOffsetCommitTopic -> name: $name, partitions: $partitions";
  }
}
