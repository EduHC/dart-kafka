import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_metadata_api.dart';
import 'package:dart_kafka/src/apis/kafka_version_api.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaAdmin {
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();
  final KafkaMetadataApi metadataApi = KafkaMetadataApi();
  late final Utils utils;

  KafkaAdmin({required this.kafka}) {
    utils = kafka.utils;
  }

  Future<dynamic> sendApiVersionRequest({
    String? clientId,
    int? apiVersion,
    int? correlationId,
    bool async = true,
    Socket? sock,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = versionApi.serialize(
      correlationId: finalCorrelationId,
      clientId: clientId,
      apiVersion: apiVersion ?? 0,
    );

    // print("${DateTime.now()} || [APP] ApiVersionRequest: $message");
    Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: API_VERSIONS,
      apiVersion: apiVersion ?? 5,
      function: versionApi.deserialize,
      topic: null,
      partition: null,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendMetadataRequest({
    int? correlationId,
    int? apiVersion,
    bool allowAutoTopicCreation = false,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
    required List<String> topics,
    String? clientId,
    bool async = true,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = metadataApi.serialize(
      correlationId: finalCorrelationId,
      topics: topics,
      apiVersion: apiVersion ?? 5,
      clientId: clientId,
      allowAutoTopicCreation: allowAutoTopicCreation,
      includeClusterAuthorizedOperations: includeClusterAuthorizedOperations,
      includeTopicAuthorizedOperations: includeTopicAuthorizedOperations,
    );

    // print("${DateTime.now()} || [APP] MetadataRequest: $message");
    Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: METADATA,
      apiVersion: apiVersion ?? 5,
      function: metadataApi.deserialize,
      topic: null,
      partition: null,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<void> updateTopicsMetadata({
    int? correlationId,
    int apiVersion = 9,
    bool allowAutoTopicCreation = false,
    bool includeClusterAuthorizedOperations = false,
    bool includeTopicAuthorizedOperations = false,
    required List<String> topics,
  }) async {
    MetadataResponse metadata = await sendMetadataRequest(
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
