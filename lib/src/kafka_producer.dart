import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_initi_producer_id_api.dart';
import 'package:dart_kafka/src/apis/kafka_produce_api.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaProducer {
  final KafkaClient kafka;
  final KafkaProduceApi produceApi = KafkaProduceApi();
  final KafkaInitProducerIdApi initProducerIdApi = KafkaInitProducerIdApi();
  final Utils utils = Utils();

  KafkaProducer({required this.kafka});

  Future<void> produce({
    int? correlationId,
    int apiVersion = 11,
    int attributes = 0,
    int baseSequence = 0,
    int batchOffset = 0,
    int lastOffsetDelta = 0,
    int partitionLeaderEpoch = -1,
    int producerEpoch = 0,
    int producerId = 0,
    String? transactionalId,
    String? clientId,
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
      transactionalId: transactionalId,
      clientId: clientId,
      attributes: attributes,
      baseSequence: baseSequence,
      batchOffset: batchOffset,
      lastOffsetDelta: lastOffsetDelta,
      partitionLeaderEpoch: partitionLeaderEpoch,
      producerEpoch: producerEpoch,
      producerId: producerId,
    );

    print("${DateTime.now()} || [APP] ProduceRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: produceApi.deserialize,
        apiVersion: apiVersion ?? 7);
  }

  Future<void> initProduceId({
    required int transactionTimeoutMs,
    required int producerId,
    required int producerEpoch,
    int? correlationId,
    int? apiVersion,
    String? clientId,
  }) async {
    Socket? server = kafka.server;
    if (server == null) return;

    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();
    Uint8List message = initProducerIdApi.serialize(
        transactionTimeoutMs: transactionTimeoutMs,
        producerId: producerId,
        producerEpoch: producerEpoch,
        apiVersion: apiVersion ?? 4,
        clientId: clientId,
        correlationId: finalCorrelationId);

    print("${DateTime.now()} || [APP] InitProduceId: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: initProducerIdApi.deserialize,
        apiVersion: apiVersion ?? 4);
  }
}
