import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_fetch_api.dart';
import 'package:dart_kafka/src/apis/kafka_join_group_api.dart';
import 'package:dart_kafka/src/apis/kafka_list_offset_api.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaConsumer {
  final KafkaClient kafka;
  final KafkaFetchApi fetchApi = KafkaFetchApi();
  final KafkaJoinGroupApi joinGroupApi = KafkaJoinGroupApi();
  final KafkaListOffsetApi listOffsetApi = KafkaListOffsetApi();
  final Utils utils = Utils();

  KafkaConsumer({required this.kafka});

  Future<dynamic> sendFetchRequest(
      {int? correlationId,
      int? apiVersion,
      required String clientId,
      int? replicaId,
      int? maxWaitMs,
      int? minBytes,
      int? maxBytes,
      int? isolationLevel,
      required List<Topic> topics,
      bool async = true}) async {
    final List<Future<dynamic>> responses = [];

    for (Topic topic in topics) {
      for (Partition partition in topic.partitions ?? []) {
        int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

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
        // kafka.enqueueRequest(
        //   request: message,
        //   correlationId: finalCorrelationId,
        //   async: async,
        // );

        // Future<dynamic> res = kafka.storeProcessingRequest(
        //   correlationId: finalCorrelationId,
        //   deserializer: fetchApi.deserialize,
        //   apiVersion: apiVersion ?? 17,
        //   async: async,
        // );

        Future<dynamic> res = kafka.tEnqueueRequest(
            apiKey: FETCH,
            apiVersion: apiVersion ?? 17,
            correlationId: finalCorrelationId,
            function: fetchApi.deserialize,
            topic: topic.topicName,
            partition: partition.id,
            message: message,
            async: async);

        responses.add(res);
      }
    }

    if (async) {
      responses.clear();
      return;
    }

    return Future.wait(responses);
  }

  Future<dynamic> sendJoinGroupRequest({
    int? correlationId,
    int? apiVersion,
    required String groupId,
    required int sessionTimeoutMs,
    required int rebalanceTimeoutMs,
    required String memberId,
    String? groupInstanceId,
    required String protocolType,
    required List<Protocol> protocols,
    String? reason,
    bool async = true,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = joinGroupApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion ?? 9,
      groupId: groupId,
      memberId: memberId,
      protocolType: protocolType,
      rebalanceTimeoutMs: rebalanceTimeoutMs,
      sessionTimeoutMs: sessionTimeoutMs,
      groupInstanceId: groupInstanceId,
      protocols: protocols,
      reason: reason,
    );
    // print("${DateTime.now()} || [APP] JoinGroupRequest: $message");
    kafka.enqueueRequest(
        request: message,
        correlationId: finalCorrelationId,
        async: async,
        sock: kafka.getAnyBroker());

    Future<dynamic> res = kafka.storeProcessingRequest(
      correlationId: finalCorrelationId,
      deserializer: joinGroupApi.deserialize,
      apiVersion: apiVersion ?? 9,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendListOffsetsRequest({
    int? correlationId,
    int apiVersion = 9,
    bool async = true,
    required int isolationLevel,
    int leaderEpoch = -1,
    int limit = 10,
    int replicaId = 0,
    String? clientId,
    required List<Topic> topics,
  }) async {
    final List<Future<dynamic>> responses = [];

    for (Topic topic in topics) {
      for (Partition partition in topic.partitions ?? []) {
        int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

        Uint8List message = listOffsetApi.serialize(
          correlationId: finalCorrelationId,
          apiVersion: apiVersion,
          isolationLevel: isolationLevel,
          leaderEpoch: leaderEpoch,
          limit: limit,
          replicaId: replicaId,
          clientId: clientId,
          topics: topics,
        );

        // print("${DateTime.now()} || [APP] ListOffsetRequest: $message");
        kafka.enqueueRequest(
            request: message,
            correlationId: finalCorrelationId,
            async: async,
            sock: kafka.getBrokerForPartition(
                topic: topic.topicName, partition: partition.id));

        Future<dynamic> res = kafka.storeProcessingRequest(
          correlationId: finalCorrelationId,
          deserializer: listOffsetApi.deserialize,
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
}
