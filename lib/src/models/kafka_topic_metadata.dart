import 'package:dart_kafka/src/models/kafka_partition_metadata.dart';

class KafkaTopicMetadata {
  final int errorCode;
  final String topicName;
  final bool isInternal;
  final List<KafkaPartitionMetadata> partitions;

  KafkaTopicMetadata({
    required this.errorCode,
    required this.topicName,
    required this.isInternal,
    required this.partitions,
  });
}
