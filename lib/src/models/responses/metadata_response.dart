import 'package:dart_kafka/src/models/broker.dart';
import 'package:dart_kafka/src/models/metadata/kafka_topic_metadata.dart';

class MetadataResponse {
  final int? throttleTimeMs;
  final List<Broker> brokers;
  final List<KafkaTopicMetadata> topics;
  final int? controllerId;
  final String? clusterId;
  final int? clusterAuthorizedOperations;
  final int? topicAuthorizedOperations;
  final int? leaderEpoch;

  MetadataResponse({
    this.throttleTimeMs,
    required this.brokers,
    required this.topics,
    this.controllerId,
    this.clusterId,
    this.clusterAuthorizedOperations,
    this.topicAuthorizedOperations,
    this.leaderEpoch
  });

  @override
  String toString() {
    return "MetadataResponse: throttleTimeMs: $throttleTimeMs, controllerId: $controllerId, clusterId: $clusterId, leaderEpoch: $leaderEpoch, "
           "clusterAuthorizedOperations: $clusterAuthorizedOperations, topicAuthorizedOperations: $topicAuthorizedOperations, "
           "BrokersMetada: $brokers, "
           "TopicMetadata: $topics";
  }

}
