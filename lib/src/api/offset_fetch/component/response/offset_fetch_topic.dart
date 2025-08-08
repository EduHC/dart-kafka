import 'offset_fetch_partition.dart';

class OffsetFetchTopic {
  OffsetFetchTopic({
    required this.name,
    required this.partitions,
    this.errorCode,
    this.errorMessage,
  });
  final String name;
  final List<OffsetFetchPartition> partitions;
  final int? errorCode;
  final String? errorMessage;

  @override
  String toString() => 'Topic -> name: $name, partition: $partitions';
}
