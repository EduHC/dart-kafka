import 'package:dart_kafka/src/models/metadata/kafka_partition_metadata.dart';

class KafkaTopicMetadata {
  final int errorCode;
  final String topicName;
  final bool isInternal;
  final List<KafkaPartitionMetadata> partitions;
  final int? topicAuthorizedOperations;

  KafkaTopicMetadata(
      {required this.errorCode,
      required this.topicName,
      required this.isInternal,
      required this.partitions,
      this.topicAuthorizedOperations});

  @override
  String toString() {
    return "TopicMetadata: topicName: $topicName, isInternal: $isInternal, topicAuthorizedOperations: $topicAuthorizedOperations, "
           "errorCode: $errorCode, partitions: $partitions";
  }
}
