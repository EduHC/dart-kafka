import 'kafka_partition_metadata.dart';

class KafkaTopicMetadata {
  KafkaTopicMetadata({
    required this.errorCode,
    required this.topicName,
    required this.isInternal,
    required this.partitions,
    this.topicAuthorizedOperations,
  });
  final int errorCode;
  final String topicName;
  final bool isInternal;
  final List<KafkaPartitionMetadata> partitions;
  final int? topicAuthorizedOperations;

  @override
  String toString() =>
      'TopicMetadata: topicName: $topicName, isInternal: $isInternal, topicAuthorizedOperations: $topicAuthorizedOperations, '
      'errorCode: $errorCode, partitions: $partitions';
}
