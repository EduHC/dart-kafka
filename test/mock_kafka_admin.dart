import 'dart:io';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/api/api_version/api_version_api.dart';
import 'package:dart_kafka/src/api/metadata/metadata_api.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

import 'mock_kafka_client.dart';

class MockKafkaAdmin implements KafkaAdmin {
  MockKafkaAdmin(this._client);
  final MockKafkaClient _client;

  @override
  Future<void> updateTopicsMetadata({
    required List<String> topics,
    bool allowAutoTopicCreation = true,
    int apiVersion = 12,
    int? correlationId,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
  }) {
    // Simulate metadata fetch
    final metadataResponse = MetadataResponse(
      brokers: [Broker(nodeId: 1, host: 'localhost', port: 9092)],
      topics: topics
          .map(
            (t) => KafkaTopicMetadata(
              topicName: t,
              isInternal: false,
              partitions: [
                KafkaPartitionMetadata(
                  partitionId: 0,
                  leaderId: 1,
                  replicas: [1],
                  isr: [1],
                  offlineReplicas: [],
                  errorCode: 0,
                ),
              ],
              errorCode: 0,
            ),
          )
          .toList(),
      clusterId: 'mock_cluster',
      controllerId: 1,
      throttleTimeMs: 0,
    );

    // In a real scenario, this would update the client's internal metadata cache.
    // For this mock, we just ensure it returns successfully.
    _client.setResponder(3, () => Future.value(metadataResponse));
    return Future.value();
  }

  Future sendCreateTopicsRequest({
    required List<Topic> topics,
    int? correlationId,
    int apiVersion = 7,
    bool async = true,
    int timeout = 5000,
    bool validateOnly = false,
  }) =>
      throw UnimplementedError();

  Future sendDeleteTopicsRequest({
    required List<String> topics,
    int? correlationId,
    int apiVersion = 6,
    bool async = true,
    int timeout = 5000,
  }) =>
      throw UnimplementedError();

  @override
  Future<dynamic> sendMetadataRequest({
    required List<String> topics,
    String? clientId,
    int? correlationId,
    int? apiVersion,
    bool async = true,
    bool allowAutoTopicCreation = true,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
  }) {
    throw UnimplementedError();
  }

  @override
  Future sendApiVersionRequest({
    int? correlationId,
    int? apiVersion,
    bool async = true,
    String? clientId,
    Socket? sock,
  }) {
    throw UnimplementedError();
  }

  @override
  KafkaClient get kafka => throw UnimplementedError();

  @override
  KafkaMetadataApi get metadataApi => throw UnimplementedError();

  Future<void> updateClusterMetadata({
    bool allowAutoTopicCreation = true,
    int apiVersion = 12,
    int? correlationId,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
  }) {
    throw UnimplementedError();
  }

  @override
  Utils get utils => Utils();

  @override
  KafkaVersionApi get versionApi => throw UnimplementedError();
}
