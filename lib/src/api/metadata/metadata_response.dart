import '../../common/broker.dart';
import '../../common/metadata/kafka_topic_metadata.dart';

class MetadataResponse {
  MetadataResponse({
    required this.brokers,
    required this.topics,
    this.throttleTimeMs,
    this.controllerId,
    this.clusterId,
    this.clusterAuthorizedOperations,
    this.topicAuthorizedOperations,
    this.leaderEpoch,
  });
  final int? throttleTimeMs;
  final List<Broker> brokers;
  final List<KafkaTopicMetadata> topics;
  final int? controllerId;
  final String? clusterId;
  final int? clusterAuthorizedOperations;
  final int? topicAuthorizedOperations;
  final int? leaderEpoch;

  @override
  String toString() =>
      'MetadataResponse: throttleTimeMs: $throttleTimeMs, controllerId: $controllerId, clusterId: $clusterId, leaderEpoch: $leaderEpoch, '
      'clusterAuthorizedOperations: $clusterAuthorizedOperations, topicAuthorizedOperations: $topicAuthorizedOperations, '
      'BrokersMetada: $brokers, '
      'TopicMetadata: $topics';
}
