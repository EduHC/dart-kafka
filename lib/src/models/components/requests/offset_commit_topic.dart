import 'package:dart_kafka/dart_kafka.dart';

class OffsetCommitTopic {
  final String name;
  final List<OffsetCommitPartition> partitions;

  OffsetCommitTopic({
    required this.name,
    required this.partitions,
  });

  @override
  String toString() {
    return "OffsetCommitTopic -> name: $name, partitions: $partitions";
  }
}
