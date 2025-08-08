import 'dart:io';
import 'dart:typed_data';

import '../api/api_version/api_version_api.dart';
import '../api/metadata/metadata_api.dart';
import '../api/metadata/metadata_response.dart';
import '../protocol/definition/api.dart';
import '../protocol/utils.dart';
import 'kafka_client.dart';

class KafkaAdmin {
  factory KafkaAdmin({required KafkaClient kafka}) {
    _instance ??= KafkaAdmin._(kafka: kafka);
    return _instance!;
  }

  KafkaAdmin._({required this.kafka});
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();
  final KafkaMetadataApi metadataApi = KafkaMetadataApi();
  final Utils utils = Utils();

  static KafkaAdmin? _instance;

  Future<dynamic> sendApiVersionRequest({
    String? clientId,
    int? apiVersion,
    int? correlationId,
    bool async = true,
    Socket? sock,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = versionApi.serialize(
      correlationId: finalCorrelationId,
      clientId: clientId,
      apiVersion: apiVersion ?? 0,
    );

    // print("${DateTime.now()} || [APP] ApiVersionRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: API_VERSIONS,
      apiVersion: apiVersion ?? 5,
      function: versionApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendMetadataRequest({
    required List<String> topics,
    int? correlationId,
    int? apiVersion,
    bool allowAutoTopicCreation = false,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
    String? clientId,
    bool async = true,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = metadataApi.serialize(
      correlationId: finalCorrelationId,
      topics: topics,
      apiVersion: apiVersion ?? 5,
      clientId: clientId,
      allowAutoTopicCreation: allowAutoTopicCreation,
      includeClusterAuthorizedOperations: includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations: includeTopicAuthorizedOperations,
    );

    // print("${DateTime.now()} || [APP] MetadataRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: METADATA,
      apiVersion: apiVersion ?? 5,
      function: metadataApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<void> updateTopicsMetadata({
    required List<String> topics,
    int? correlationId,
    int apiVersion = 9,
    bool allowAutoTopicCreation = false,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
  }) async {
    final MetadataResponse metadata = await sendMetadataRequest(
      topics: topics,
      async: false,
      apiVersion: apiVersion,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: allowAutoTopicCreation,
      includeClusterAuthorizedOperations: includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations: includeTopicAuthorizedOperations,
      correlationId: correlationId,
    );

    await kafka.updateTopicsBroker(metadata: metadata);
  }
}
