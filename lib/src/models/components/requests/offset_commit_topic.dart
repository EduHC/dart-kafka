import '../../../../dart_kafka.dart';

class OffsetCommitTopic {
  OffsetCommitTopic({
    required this.name,
    required this.partitions,
  });
  final String name;
  final List<OffsetCommitPartition> partitions;

  @override
  String toString() =>
      'OffsetCommitTopic -> name: $name, partitions: $partitions';
}
