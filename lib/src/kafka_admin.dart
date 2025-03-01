import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_metadata_api.dart';
import 'package:dart_kafka/src/apis/kafka_version_api.dart';
import 'package:dart_kafka/src/kafka_client.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaAdmin {
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();
  final KafkaMetadataApi metadataApi = KafkaMetadataApi();
  final Utils utils = Utils();
  
  KafkaAdmin({required this.kafka});

  Future<void> sendApiVersionRequest({
    String? clientId,
    int? apiVersion,
    int? correlationId,
  }) async {
    if (kafka.server == null) return;

    Socket server = kafka.server!;
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = versionApi.serialize(
        correlationId: finalCorrelationId,
        clientId: clientId,
        apiVersion: apiVersion ?? 0);

    print("${DateTime.now()} || [APP] ApiVersionRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: versionApi.deserialize);
  }

  Future<void> sendMetadataRequest(
      {int? correlationId,
      int? apiVersion,
      bool? allowAutoTopicCreation,
      bool? includeClusterAuthorizedOperations,
      bool? includeTopicAuthorizedOperations,
      required List<String> topics,
      String? clientId}) async {
    if (kafka.server == null) return;

    Socket server = kafka.server!;
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = metadataApi.serialize(
        correlationId: finalCorrelationId,
        topics: topics,
        apiVersion: 5,
        clientId: clientId,
        allowAutoTopicCreation: true,
        includeClusterAuthorizedOperations: false,
        includeTopicAuthorizedOperations: false);

    print("${DateTime.now()} || [APP] MetadataRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: metadataApi.deserialize);
  }
}
