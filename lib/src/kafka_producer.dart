import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_describe_producer_api.dart';
import 'package:dart_kafka/src/apis/kafka_initi_producer_id_api.dart';
import 'package:dart_kafka/src/apis/kafka_produce_api.dart';
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
        kafka.enqueueRequest(
            request: message,
            correlationId: finalCorrelationId,
            async: async,
            sock: kafka.getBrokerForPartition(
                topic: topic.topicName, partition: partition.id));

        Future<dynamic> res = kafka.storeProcessingRequest(
          correlationId: finalCorrelationId,
          deserializer: produceApi.deserialize,
          apiVersion: apiVersion,
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
    int? apiVersion,
    String? clientId,
    bool async = true,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();
    Uint8List message = initProducerIdApi.serialize(
        transactionTimeoutMs: transactionTimeoutMs,
        producerId: producerId,
        producerEpoch: producerEpoch,
        apiVersion: apiVersion ?? 4,
        clientId: clientId,
        correlationId: finalCorrelationId);

    // print("${DateTime.now()} || [APP] InitProduceId: $message");
    kafka.enqueueRequest(
        request: message,
        correlationId: finalCorrelationId,
        async: async,
        sock: kafka.getAnyBroker());

    Future<dynamic> res = kafka.storeProcessingRequest(
      correlationId: finalCorrelationId,
      deserializer: initProducerIdApi.deserialize,
      apiVersion: apiVersion ?? 4,
      async: async,
    );

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
    kafka.enqueueRequest(
        request: message,
        correlationId: finalCorrelationId,
        async: async,
        sock: kafka.getAnyBroker());

    Future<dynamic> res = kafka.storeProcessingRequest(
      correlationId: finalCorrelationId,
      deserializer: describeProducerApi.deserialize,
      apiVersion: apiVersion,
      async: async,
    );

    if (async) return;

    return res;
  }
}
