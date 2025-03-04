import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_fetch_api.dart';
import 'package:dart_kafka/src/kafka_client.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaConsumer {
  final KafkaClient kafka;
  final KafkaFetchApi fetchApi = KafkaFetchApi();
  final Utils utils = Utils();

  KafkaConsumer({required this.kafka});

  Future<void> sendFetchRequest({
    int? correlationId,
    int? apiVersion,
    required String clientId,
    int? replicaId,
    int? maxWaitMs,
    int? minBytes,
    int? maxBytes,
    int? isolationLevel,
    required List<Topic> topics,
  }) async {
    Socket? server = kafka.server;

    if (server == null) return;

    int finalCorrelationId = utils.generateCorrelationId();

    Uint8List message = fetchApi.serialize(
        correlationId: finalCorrelationId,
        apiVersion: apiVersion ?? 17,
        clientId: clientId,
        replicaId: replicaId,
        maxWaitMs: maxWaitMs,
        minBytes: minBytes,
        maxBytes: maxBytes,
        isolationLevel: isolationLevel ?? 1,
        topics: topics);

    print("${DateTime.now()} || [APP] FetchRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: fetchApi.deserialize,
        apiVersion: apiVersion ?? 17);
  }
}
