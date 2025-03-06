import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_fetch_api.dart';
import 'package:dart_kafka/src/apis/kafka_join_group_api.dart';
import 'package:dart_kafka/src/kafka_client.dart';
import 'package:dart_kafka/src/models/components/protocol.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaConsumer {
  final KafkaClient kafka;
  final KafkaFetchApi fetchApi = KafkaFetchApi();
  final KafkaJoinGroupApi joinGroupApi = KafkaJoinGroupApi();
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
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: fetchApi.deserialize,
        apiVersion: apiVersion ?? 17);
  }

  Future<void> sendJoinGroupRequest({
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
  }) async {
    Socket? server = kafka.server;

    if (server == null) return;

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
    print("${DateTime.now()} || [APP] JoinGroupRequest: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(
        correlationId: finalCorrelationId,
        deserializer: joinGroupApi.deserialize,
        apiVersion: apiVersion ?? 9);
  }
}
