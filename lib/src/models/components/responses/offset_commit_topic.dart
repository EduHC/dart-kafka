import '../../../../dart_kafka.dart';

class ResOffsetCommitTopic {
  ResOffsetCommitTopic({
    required this.name,
    required this.partitions,
  });
  final String name;
  final List<ResOffsetCommitPartition> partitions;

  @override
  String toString() =>
      'ResOffsetCommitTopic -> name: $name, partitions: $partitions';
}
