import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_describe_producer_api.dart';
import 'package:dart_kafka/src/apis/kafka_initi_producer_id_api.dart';
import 'package:dart_kafka/src/apis/kafka_produce_api.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaProducer {
  final KafkaClient kafka;
  final KafkaProduceApi produceApi = KafkaProduceApi();
  final KafkaInitProducerIdApi initProducerIdApi = KafkaInitProducerIdApi();
  final KafkaDescribeProducerApi describeProducerApi =
      KafkaDescribeProducerApi();
  final Utils utils = Utils();

  KafkaProducer({required this.kafka});

  Future<dynamic> produce(
      {int? correlationId,
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
      bool async = true}) async {
    final List<Future<dynamic>> responses = [];

    for (Topic topic in topics) {
      for (Partition partition in topic.partitions ?? []) {
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

        // print("${DateTime.now()} || [APP] ProduceRequest: $message");
        Future<dynamic> res = kafka.enqueueRequest(
          message: message,
          correlationId: finalCorrelationId,
          apiKey: PRODUCE,
          apiVersion: apiVersion,
          function: produceApi.deserialize,
          topicName: topic.topicName,
          partition: partition.id,
          async: async,
        );

        responses.add(res);
      }
    }

    if (async) {
      responses.clear();
      return;
    }

    return Future.wait(responses);
  }

  Future<dynamic> initProduceId({
    required int transactionTimeoutMs,
    required int producerId,
    required int producerEpoch,
    int? correlationId,
    int apiVersion = 4,
    String? clientId,
    bool async = true,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();
    Uint8List message = initProducerIdApi.serialize(
        transactionTimeoutMs: transactionTimeoutMs,
        producerId: producerId,
        producerEpoch: producerEpoch,
        apiVersion: apiVersion,
        clientId: clientId,
        correlationId: finalCorrelationId);

    // print("${DateTime.now()} || [APP] InitProduceId: $message");
    Future<dynamic> res = kafka.enqueueRequest(
        message: message,
        correlationId: finalCorrelationId,
        apiKey: INIT_PRODUCER_ID,
        apiVersion: apiVersion,
        function: initProducerIdApi.deserialize,
        topic: null,
        partition: null,
        async: async);

    if (async) return;

    return res;
  }

  Future<dynamic> describeProducer({
    required List<Topic> topics,
    int apiVersion = 0,
    String? clientId,
    int? correlationId,
    bool async = true,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    Uint8List message = describeProducerApi.serialize(
        correlationId: finalCorrelationId,
        topics: topics,
        apiVersion: apiVersion,
        clientId: clientId);

    // print("${DateTime.now()} || [APP] DescribeProducerRequest: $message");
    Future<dynamic> res = kafka.enqueueRequest(
        message: message,
        correlationId: finalCorrelationId,
        apiKey: DESCRIBE_PRODUCERS,
        apiVersion: apiVersion,
        function: describeProducerApi.deserialize,
        topic: null,
        partition: null,
        async: async);

    if (async) return;

    return res;
  }
}
