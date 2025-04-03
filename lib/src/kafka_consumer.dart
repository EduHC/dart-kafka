import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/apis/kafka_fetch_api.dart';
import 'package:dart_kafka/src/apis/kafka_find_group_coordinator_api.dart';
import 'package:dart_kafka/src/apis/kafka_heartbeat_api.dart';
import 'package:dart_kafka/src/apis/kafka_join_group_api.dart';
import 'package:dart_kafka/src/apis/kafka_list_offset_api.dart';
import 'package:dart_kafka/src/apis/kafka_sync_group_api.dart';
import 'package:dart_kafka/src/definitions/apis.dart';
import 'package:dart_kafka/src/definitions/coordinator_types.dart';
import 'package:dart_kafka/src/protocol/utils.dart';

class KafkaConsumer {
  final KafkaClient kafka;
  final KafkaFetchApi _fetchApi = KafkaFetchApi();
  final KafkaJoinGroupApi _joinGroupApi = KafkaJoinGroupApi();
  final KafkaListOffsetApi _listOffsetApi = KafkaListOffsetApi();
  final KafkaFindGroupCoordinatorApi _coordinatorApi =
      KafkaFindGroupCoordinatorApi();
  final KafkaHeartbeatApi _heartbeatApi = KafkaHeartbeatApi();
  final KafkaSyncGroupApi _syncGroupApi = KafkaSyncGroupApi();
  final Utils utils = Utils();

  KafkaConsumer({required this.kafka});

  Future<dynamic> sendFetchRequest({
    int? correlationId,
    int apiVersion = 17,
    required String clientId,
    int replicaId = -1,
    int maxWaitMs = 30000,
    int minBytes = 1,
    int maxBytes = 10000,
    int isolationLevel = 1,
    required List<Topic> topics,
    bool async = true,
  }) async {
    final List<Future<dynamic>> responses = [];

    for (Topic topic in topics) {
      for (Partition partition in topic.partitions ?? []) {
        int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

        Uint8List message = _fetchApi.serialize(
            correlationId: finalCorrelationId,
            apiVersion: apiVersion,
            clientId: clientId,
            replicaId: replicaId,
            maxWaitMs: maxWaitMs,
            minBytes: minBytes,
            maxBytes: maxBytes,
            isolationLevel: isolationLevel,
            topics: topics);

        // print("${DateTime.now()} || [APP] FetchRequest: $message");
        Future<dynamic> res = kafka.enqueueRequest(
            apiKey: FETCH,
            apiVersion: apiVersion,
            correlationId: finalCorrelationId,
            function: _fetchApi.deserialize,
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
    int apiVersion = 9,
    required String groupId,
    required int sessionTimeoutMs,
    required int rebalanceTimeoutMs,
    required String memberId,
    String? groupInstanceId,
    required String protocolType,
    required List<Protocol> protocols,
    String? reason,
    bool async = true,
    Socket? broker,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    FindGroupCoordinatorResponse coordinatorResponse =
        await sendFindGroupCoordinatorRequest(
            groups: [groupId], async: false, apiVersion: 6);

    if (coordinatorResponse.coordinators == null ||
        coordinatorResponse.coordinators!.isEmpty) {
      throw Exception("GroupCoordinator not found for groupId: $groupId");
    }

    print(coordinatorResponse);

    Coordinator c = coordinatorResponse.coordinators!.first;
    Socket? broker = kafka.getBrokerByHost(host: c.host, port: c.port);

    Uint8List message = _joinGroupApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
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
    Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: JOIN_GROUP,
      apiVersion: apiVersion,
      function: _joinGroupApi.deserialize,
      topic: null,
      partition: null,
      async: async,
      broker: broker,
    );

    if (async) return;

    return await res;
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

        Uint8List message = _listOffsetApi.serialize(
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
        Future<dynamic> res = kafka.enqueueRequest(
            message: message,
            correlationId: finalCorrelationId,
            apiKey: LIST_OFFSETS,
            apiVersion: apiVersion,
            function: _listOffsetApi.deserialize,
            topic: topic.topicName,
            partition: partition.id,
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

  /// @Param groups in ApiVersion < 4 will consider only the FISRT element for the Kafka gets a single Key per request
  Future<dynamic> sendFindGroupCoordinatorRequest({
    int? correlationId,
    int apiVersion = 6,
    required List<String> groups,
    bool async = true,
    int coordinatorType = COORDINATOR_TYPE_GROUP,
    String? clientId,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = _coordinatorApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      coordinatorType: coordinatorType,
      groups: groups,
      clientId: clientId,
    );

    print("${DateTime.now()} || [APP] FindGroupCoordinatorRequest: $message");
    Future<dynamic> res = kafka.enqueueRequest(
        message: message,
        correlationId: finalCorrelationId,
        apiKey: FIND_COORDINATOR,
        apiVersion: apiVersion,
        function: _coordinatorApi.deserialize,
        topic: null,
        partition: null,
        async: async);

    if (async) return;

    return await res;
  }

  Future<dynamic> sendHeartbeatRequest({
    int? correlationId,
    String? clientId,
    String? groupInstanceId,
    bool async = true,
    int apiVersion = 4,
    int generationId = -1,
    required String groupId,
    required String memberId,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = _heartbeatApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      generationId: generationId,
      clientId: clientId,
    );

    print("${DateTime.now()} || [APP] HeartbeatRequest: $message");
    Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: HEARTBEAT,
      apiVersion: apiVersion,
      function: _heartbeatApi.deserialize,
      topic: null,
      partition: null,
      async: async,
    );

    if (async) return;

    return await res;
  }

  Future<dynamic> sendSyncGroupRequest({
    int? correlationId,
    String? clientId,
    bool async = true,
    int apiVersion = 5,
    int generationId = -1,
    String? groupInstanceId,
    String? protocolName,
    String? protocolType,
    required String memberId,
    required String groupId,
    required List<AssignmentSyncGroup> assignment,
  }) async {
    int finalCorrelationId = correlationId ?? utils.generateCorrelationId();

    Uint8List message = _syncGroupApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      clientId: clientId,
      generationId: generationId,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      protocolName: protocolName,
      protocolType: protocolType,
      assignments: assignment,
    );

    print("${DateTime.now()} || [APP] SyncGroup: $message");
    Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: SYNC_GROUP,
      apiVersion: apiVersion,
      function: _syncGroupApi.deserialize,
      topic: null,
      partition: null,
      async: async,
    );

    if (async) return;

    return await res;
  }

  Future<dynamic> subscribe() async {}
}
