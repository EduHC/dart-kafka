import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_produce_api.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaProducer {
  final KafkaClient kafka;
  final KafkaProduceApi produceApi = KafkaProduceApi();
  final Utils utils = Utils();

  KafkaProducer({required this.kafka});

  Future<void> produce({
    int? correlationId,
    int? apiVersion,
    String? transactionalId,
    required int acks,
    required int timeoutMs,
    required List<Topic> topics,
  }) async {
    Socket? server = kafka.server;

    if (server == null) return;

    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = produceApi.serialize(
        correlationId: finalCorrelationId,
        acks: acks,
        timeoutMs: timeoutMs,
        topics: topics,
        apiVersion: apiVersion,
        transactionalId: transactionalId);

    print("${DateTime.now()} || [APP] ProduceRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: produceApi.deserialize,
        apiVersion: apiVersion ?? 7);
  }
}
