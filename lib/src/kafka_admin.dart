import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_metadata_api.dart';
import 'package:dart_kafka/src/apis/kafka_version_api.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/kafka_client.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaAdmin {
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();
  final KafkaMetadataApi metadataApi = KafkaMetadataApi();
  late final Utils utils;

  KafkaAdmin({required this.kafka}) {
    utils = kafka.utils;
  }

  Future<dynamic> sendApiVersionRequest(
      {String? clientId,
      int? apiVersion,
      int? correlationId,
      bool async = true,
      Socket? sock}) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = versionApi.serialize(
        correlationId: finalCorrelationId,
        clientId: clientId,
        apiVersion: apiVersion ?? 0);

    // print("${DateTime.now()} || [APP] ApiVersionRequest: $message");
    kafka.enqueueRequest(
        request: message,
        correlationId: finalCorrelationId,
        async: async,
        sock: sock ?? kafka.getAnyBroker());

    Future<dynamic> res = kafka.storeProcessingRequest(
      correlationId: finalCorrelationId,
      deserializer: versionApi.deserialize,
      apiVersion: apiVersion ?? 0,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendMetadataRequest(
      {int? correlationId,
      int? apiVersion,
      bool? allowAutoTopicCreation,
      bool? includeClusterAuthorizedOperations,
      bool? includeTopicAuthorizedOperations,
      required List<String> topics,
      String? clientId,
      bool async = true}) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = metadataApi.serialize(
        correlationId: finalCorrelationId,
        topics: topics,
        apiVersion: apiVersion ?? 5,
        clientId: clientId,
        allowAutoTopicCreation: true,
        includeClusterAuthorizedOperations: false,
        includeTopicAuthorizedOperations: false);

    // print("${DateTime.now()} || [APP] MetadataRequest: $message");
    // kafka.enqueueRequest(
    //     request: message,
    //     correlationId: finalCorrelationId,
    //     async: async,
    //     sock: kafka.getAnyBroker());

    // Future<dynamic> res = kafka.storeProcessingRequest(
    //   correlationId: finalCorrelationId,
    //   deserializer: metadataApi.deserialize,
    //   apiVersion: apiVersion ?? 5,
    //   async: async,
    // );

    Future<dynamic> res = kafka.tEnqueueRequest(
        message: message,
        correlationId: finalCorrelationId,
        apiKey: METADATA,
        apiVersion: apiVersion ?? 5,
        function: metadataApi.deserialize,
        topic: null,
        partition: null,
        async: async);

    if (async) return;

    return res;
  }
}
