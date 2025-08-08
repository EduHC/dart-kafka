import 'offset_commit_partition_response.dart';

class OffsetCommitTopicResponse {
  OffsetCommitTopicResponse({
    required this.name,
    required this.partitions,
  });
  final String name;
  final List<OffsetCommitPartitionResponse> partitions;

  @override
  String toString() =>
      'ResOffsetCommitTopic -> name: $name, partitions: $partitions';
}
