import 'dart:typed_data';

import '../api/describe_producer/describe_producer_api.dart';
import '../api/init_producer_id/init_producer_id_api.dart';
import '../api/produce/produce_api.dart';
import '../common/partition.dart';
import '../common/topic.dart';
import '../protocol/definition/api.dart';
import '../protocol/utils.dart';
import 'kafka_client.dart';

class KafkaProducer {
  factory KafkaProducer({required KafkaClient kafka}) {
    _instance ??= KafkaProducer._(kafka: kafka);
    return _instance!;
  }

  KafkaProducer._({required this.kafka});
  final KafkaClient kafka;
  final KafkaProduceApi produceApi = KafkaProduceApi();
  final KafkaInitProducerIdApi initProducerIdApi = KafkaInitProducerIdApi();
  final KafkaDescribeProducerApi describeProducerApi =
      KafkaDescribeProducerApi();
  final Utils utils = Utils();

  static KafkaProducer? _instance;

  Future<dynamic> produce({
    required int acks,
    required int timeoutMs,
    required List<Topic> topics,
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
    bool async = true,
  }) async {
    print(
      '${DateTime.now()} || [KAFKA-PRODUCER] Received to produce topic: ${topics[0].topicName}',
    );
    final List<Future<dynamic>> responses = [];

    for (final Topic topic in topics) {
      for (final Partition partition in topic.partitions ?? []) {
        final int finalCorrelationId =
            correlationId ?? utils.generateCorrelationId();

        final Uint8List message = produceApi.serialize(
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

        print(
          '${DateTime.now()} || [KAFKA-PRODUCER] Request Serialized and sendind to the TrafficController',
        );
        final Future<dynamic> res = kafka.enqueueRequest(
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
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();
    final Uint8List message = initProducerIdApi.serialize(
      transactionTimeoutMs: transactionTimeoutMs,
      producerId: producerId,
      producerEpoch: producerEpoch,
      apiVersion: apiVersion,
      clientId: clientId,
      correlationId: finalCorrelationId,
    );

    // print("${DateTime.now()} || [APP] InitProduceId: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: INIT_PRODUCER_ID,
      apiVersion: apiVersion,
      function: initProducerIdApi.deserialize,
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

    final Uint8List message = describeProducerApi.serialize(
      correlationId: finalCorrelationId,
      topics: topics,
      apiVersion: apiVersion,
      clientId: clientId,
    );

    // print("${DateTime.now()} || [APP] DescribeProducerRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: DESCRIBE_PRODUCERS,
      apiVersion: apiVersion,
      function: describeProducerApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }
}
