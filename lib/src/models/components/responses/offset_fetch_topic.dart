import 'package:dart_kafka/src/models/components/responses/offset_fetch_partition.dart';

class OffsetFetchTopic {
  final String name;
  final List<OffsetFetchPartition> partitions;
  final int? errorCode;
  final String? errorMessage;

  OffsetFetchTopic({
    required this.name,
    required this.partitions,
    this.errorCode,
    this.errorMessage,
  });

  @override
  String toString() {
    return "Topic -> name: $name, partition: $partitions";
  }
}
