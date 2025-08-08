import 'request/offset_commit_partition_request.dart';

class OffsetCommitTopic {
  OffsetCommitTopic({
    required this.name,
    required this.partitions,
  });
  final String name;
  final List<OffsetCommitPartitionRequest> partitions;

  @override
  String toString() =>
      'OffsetCommitTopic -> name: $name, partitions: $partitions';
}
