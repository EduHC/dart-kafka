import 'package:dart_kafka/src/models/kafka_broker.dart';
import 'package:dart_kafka/src/models/kafka_topic_metadata.dart';

class MetadataResponse {
  final int? throttleTimeMs;
  final List<KafkaBroker> brokers;
  final List<KafkaTopicMetadata> topics;

  MetadataResponse({
    this.throttleTimeMs,
    required this.brokers,
    required this.topics,
  });
}
