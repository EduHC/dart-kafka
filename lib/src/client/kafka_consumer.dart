import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import '../api/fetch/fetch_api.dart';
import '../api/find_group_coordinator/find_group_coordinator_api.dart';
import '../api/find_group_coordinator/find_group_coordinator_response.dart';
import '../api/heartbeat/heartbeat_api.dart';
import '../api/join_group/join_group_api.dart';
import '../api/join_group/join_group_response.dart';
import '../api/list_offset/list_offset_api.dart';
import '../api/offset_commit/component/offset_commit_topic.dart';
import '../api/offset_commit/offset_commit_api.dart';
import '../api/offset_fetch/component/response/offset_fetch_topic.dart';
import '../api/offset_fetch/offset_fetch_api.dart';
import '../api/offset_fetch/offset_fetch_response.dart';
import '../api/sync_group/sync_group_api.dart';
import '../api/sync_group/sync_group_response.dart';
import '../common/assignment_sync_group.dart';
import '../common/coordinator.dart';
import '../common/partition.dart';
import '../common/protocol.dart';
import '../common/request_group.dart';
import '../common/request_group_topic.dart';
import '../common/response_group.dart';
import '../common/topic.dart';
import '../protocol/assigner.dart';
import '../protocol/definition/api.dart';
import '../protocol/definition/coordinator_type.dart';
import '../protocol/utils.dart';
import 'kafka_client.dart';

class KafkaConsumer {
  factory KafkaConsumer({required KafkaClient kafka}) {
    _instance ??= KafkaConsumer._(kafka: kafka);
    return _instance!;
  }

  KafkaConsumer._({required this.kafka});
  final KafkaClient kafka;
  final Utils utils = Utils();
  final KafkaFetchApi _fetchApi = KafkaFetchApi();
  final KafkaJoinGroupApi _joinGroupApi = KafkaJoinGroupApi();
  final KafkaListOffsetApi _listOffsetApi = KafkaListOffsetApi();
  final KafkaFindGroupCoordinatorApi _coordinatorApi =
      KafkaFindGroupCoordinatorApi();
  final KafkaHeartbeatApi _heartbeatApi = KafkaHeartbeatApi();
  final KafkaSyncGroupApi _syncGroupApi = KafkaSyncGroupApi();
  final KafkaOffsetFetchApi _offsetFetchApi = KafkaOffsetFetchApi();
  final KafkaOffsetCommitApi _offsetCommitApi = KafkaOffsetCommitApi();

  final Set<String> _topicsToSubscribe = {};
  final Map<String, List<GroupData>> _memberIdPartitions = {};
  final Map<String, Timer> _heartbeatSchedules = {};
  final Map<String, Timer> _fetchSchedules = {};

  static KafkaConsumer? _instance;

  int _oldTopicsQtd = 0;

  Socket? _brokerGroupLeader;
  String? _groupInstanceId;
  String? _memberIdGroupLeader;
  String? _memberId;
  int? _generationId;

  bool get _isLeader =>
      (_memberId != null && _memberIdGroupLeader != null) &&
      _memberIdGroupLeader == _memberId;

  Future<dynamic> sendFetchRequest({
    required List<Topic> topics,
    int? correlationId,
    int apiVersion = 17,
    int replicaId = -1,
    int maxWaitMs = 30000,
    int minBytes = 1,
    int maxBytes = 10000,
    int isolationLevel = 1,
    bool async = true,
    String? groupId,
    String? memberId,
    bool autoCommit = false,
    String? groupInstanceId,
  }) async {
    final List<Future<dynamic>> responses = [];

    for (final Topic topic in topics) {
      for (final Partition partition in topic.partitions ?? []) {
        final int finalCorrelationId =
            correlationId ?? utils.generateCorrelationId();

        final Uint8List message = _fetchApi.serialize(
          correlationId: finalCorrelationId,
          apiVersion: apiVersion,
          clientId: kafka.clientId,
          replicaId: replicaId,
          maxWaitMs: maxWaitMs,
          minBytes: minBytes,
          maxBytes: maxBytes,
          isolationLevel: isolationLevel,
          topics: topics,
        );

        // debugPrint("${DateTime.now()} || [APP] FetchRequest: $message");
        final Future<dynamic> res = kafka.enqueueRequest(
          apiKey: FETCH,
          apiVersion: apiVersion,
          correlationId: finalCorrelationId,
          function: _fetchApi.deserialize,
          topicName: topic.topicName,
          partition: partition.id,
          message: message,
          async: async,
          autoCommit: autoCommit,
          groupId: groupId,
          memberId: memberId,
          groupInstanceId: groupInstanceId,
          topic: topic,
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

  Future<dynamic> sendJoinGroupRequest({
    required String groupId,
    required int sessionTimeoutMs,
    required int rebalanceTimeoutMs,
    required String memberId,
    required String protocolType,
    required List<Protocol> protocols,
    int? correlationId,
    int apiVersion = 9,
    String? groupInstanceId,
    String? reason,
    bool async = true,
    Socket? broker,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _joinGroupApi.serialize(
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

    // debugPrint("${DateTime.now()} || [APP] JoinGroupRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: JOIN_GROUP,
      apiVersion: apiVersion,
      function: _joinGroupApi.deserialize,
      async: async,
      broker: broker,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendListOffsetsRequest({
    required int isolationLevel,
    required List<Topic> topics,
    int? correlationId,
    int apiVersion = 9,
    bool async = true,
    int leaderEpoch = -1,
    int limit = 10,
    int replicaId = 0,
    DateTime? timestamp,
  }) async {
    final List<Future<dynamic>> responses = [];

    for (final Topic topic in topics) {
      for (final Partition partition in topic.partitions ?? []) {
        final int finalCorrelationId =
            correlationId ?? utils.generateCorrelationId();

        final Uint8List message = _listOffsetApi.serialize(
          correlationId: finalCorrelationId,
          apiVersion: apiVersion,
          isolationLevel: isolationLevel,
          leaderEpoch: leaderEpoch,
          limit: limit,
          replicaId: replicaId,
          clientId: kafka.clientId,
          topics: [topic],
          timestamp: timestamp ?? DateTime.utc(1970),
        );

        // debugPrint("${DateTime.now()} || [APP] ListOffsetRequest: $message");
        final Future<dynamic> res = kafka.enqueueRequest(
          message: message,
          correlationId: finalCorrelationId,
          apiKey: LIST_OFFSETS,
          apiVersion: apiVersion,
          function: _listOffsetApi.deserialize,
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

  /// @Param groups in ApiVersion < 4 will consider only the FISRT element for the Kafka gets a single Key per request
  Future<dynamic> sendFindGroupCoordinatorRequest({
    required List<String> groups,
    int? correlationId,
    int apiVersion = 6,
    bool async = true,
    int coordinatorType = COORDINATOR_TYPE_GROUP,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _coordinatorApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      coordinatorType: coordinatorType,
      groups: groups,
      clientId: kafka.clientId,
    );

    // debugPrint("${DateTime.now()} || [APP] FindGroupCoordinatorRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: FIND_COORDINATOR,
      apiVersion: apiVersion,
      function: _coordinatorApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendHeartbeatRequest({
    required String groupId,
    required String memberId,
    int? correlationId,
    String? groupInstanceId,
    bool async = true,
    int apiVersion = 4,
    int generationId = -1,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _heartbeatApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      generationId: generationId,
      clientId: kafka.clientId,
    );

    // debugPrint("${DateTime.now()} || [APP] HeartbeatRequest: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: HEARTBEAT,
      apiVersion: apiVersion,
      function: _heartbeatApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendSyncGroupRequest({
    required String memberId,
    required String groupId,
    required List<AssignmentSyncGroup> assignment,
    int? correlationId,
    bool async = true,
    int apiVersion = 5,
    int generationId = -1,
    String? groupInstanceId,
    String? protocolName,
    String? protocolType,
    Socket? broker,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _syncGroupApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      clientId: kafka.clientId,
      generationId: generationId,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      protocolName: protocolName,
      protocolType: protocolType,
      assignments: assignment,
    );

    // debugPrint("${DateTime.now()} || [APP] SyncGroup: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: SYNC_GROUP,
      apiVersion: apiVersion,
      function: _syncGroupApi.deserialize,
      async: async,
      broker: broker,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendOffsetFetch({
    required List<RequestGroup> groups,
    int? correlationId,
    int apiVersion = 9,
    bool async = true,
    bool requireStable = false,
    Socket? broker,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _offsetFetchApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      clientId: kafka.clientId,
      requireStable: requireStable,
      groups: groups,
    );

    // debugPrint("${DateTime.now()} || [APP] OffsetFetch: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: OFFSET_FETCH,
      apiVersion: apiVersion,
      function: _offsetFetchApi.deserialize,
      async: async,
      broker: broker,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> sendOffsetCommit({
    required String groupId,
    required String memberId,
    required List<OffsetCommitTopic> topics,
    int? correlationId,
    int apiVersion = 9,
    bool async = true,
    String? groupInstanceId,
    int generationIdOrMemberEpoch = -1,
  }) async {
    final int finalCorrelationId =
        correlationId ?? utils.generateCorrelationId();

    final Uint8List message = _offsetCommitApi.serialize(
      correlationId: finalCorrelationId,
      apiVersion: apiVersion,
      clientId: kafka.clientId,
      generationIdOrMemberEpoch: generationIdOrMemberEpoch,
      groupId: groupId,
      memberId: memberId,
      groupInstanceId: groupInstanceId,
      topics: topics,
    );

    // debugPrint("${DateTime.now()} || [APP] OffsetFetch: $message");
    final Future<dynamic> res = kafka.enqueueRequest(
      message: message,
      correlationId: finalCorrelationId,
      apiKey: OFFSET_COMMIT,
      apiVersion: apiVersion,
      function: _offsetCommitApi.deserialize,
      async: async,
    );

    if (async) return;

    return res;
  }

  Future<dynamic> subscribe({
    required List<String> topicsToSubscribe,
    required String groupId,
    String? groupInstanceId,
  }) async {
    _groupInstanceId = groupInstanceId;
    _topicsToSubscribe.addAll(topicsToSubscribe);

    if (_oldTopicsQtd == _topicsToSubscribe.length) {
      print('Ignorado subscribe! qtd antiga: $_oldTopicsQtd'
          ' | qtd nova: ${_topicsToSubscribe.length}');
      return;
    }

    await kafka
        .getAdminClient()
        .updateTopicsMetadata(topics: topicsToSubscribe);

    _oldTopicsQtd = _topicsToSubscribe.length;

    final FindGroupCoordinatorResponse cResponse =
        await sendFindGroupCoordinatorRequest(
      groups: [groupId],
      async: false,
    );

    if (cResponse.coordinators == null || cResponse.coordinators!.isEmpty) {
      throw Exception('GroupCoordinator not found for groupId: $groupId');
    }

    final Coordinator c = cResponse.coordinators!.first;
    _brokerGroupLeader = kafka.getBrokerByHost(host: c.host, port: c.port);

    JoinGroupResponse joinRes = await sendJoinGroupRequest(
      groupId: groupId,
      sessionTimeoutMs: kafka.sessionTimeoutMs,
      rebalanceTimeoutMs: kafka.rebalanceTimeoutMs,
      memberId: _memberId ?? '',
      protocolType: 'consumer',
      protocols: Assigner.protocol(topics: _topicsToSubscribe.toList()),
      groupInstanceId: _groupInstanceId,
      broker: _brokerGroupLeader,
      async: false,
    );

    if (joinRes.errorCode == 79) {
      joinRes = await sendJoinGroupRequest(
        groupId: groupId,
        sessionTimeoutMs: kafka.sessionTimeoutMs,
        rebalanceTimeoutMs: kafka.rebalanceTimeoutMs,
        memberId: joinRes.memberId,
        protocolType: 'consumer',
        protocols: Assigner.protocol(topics: _topicsToSubscribe.toList()),
        groupInstanceId: groupInstanceId,
        broker: _brokerGroupLeader,
        async: false,
      );
    }

    if (joinRes.errorCode != 0) throw Exception(joinRes.errorMessage);

    _memberIdGroupLeader = joinRes.leader;
    _memberId ??= joinRes.memberId;
    _generationId = joinRes.generationId;

    final List<AssignmentSyncGroup> assignment = Assigner.assign(
      members: joinRes.members!,
      isLeader: _isLeader,
    );

    final SyncGroupResponse syncRes = await sendSyncGroupRequest(
      memberId: _memberId!,
      groupId: groupId,
      assignment: assignment,
      apiVersion: 3,
      async: false,
      generationId: _generationId!,
      broker: _brokerGroupLeader,
    );

    if (syncRes.errorCode != 0) throw Exception(syncRes.errorMessage);

    final List<GroupData> groupData = syncRes.assignment!.topics
        .map(
          (topic) => GroupData(
            topicName: topic.topicName,
            partitions: topic.partitions
                .map((partition) => GroupPartitionData(partitionId: partition))
                .toList(),
          ),
        )
        .toList();

    await sendOffsetFetchAndSync(groupId: groupId, groupData: groupData);

    _startHeartbeat(
      groupId: groupId,
      memberId: _memberId!,
      generationId: _generationId!,
      groupInstanceId: groupInstanceId,
    );

    _startFetchSchedule(
      groupId: groupId,
      memberId: _memberId!,
      groupInstanceId: groupInstanceId,
    );
  }

  Future<dynamic> unsubscribe({
    required List<String> topicsToUnsubscribe,
    required String groupId,
  }) async {
    bool hasUpdated = false;

    _topicsToSubscribe.removeWhere(
      (element) {
        final bool exists = topicsToUnsubscribe.contains(element);
        if (!hasUpdated && exists) hasUpdated = true;
        return exists;
      },
    );

    if (!hasUpdated) return;

    await subscribe(
      topicsToSubscribe: [],
      groupId: groupId,
    );
  }

  void _startHeartbeat({
    required String groupId,
    required String memberId,
    required int generationId,
    String? groupInstanceId,
  }) {
    final String key = '$groupId->$memberId';
    _heartbeatSchedules[key]?.cancel();

    sendHeartbeatRequest(
      groupId: groupId,
      memberId: _memberId!,
      generationId: _generationId!,
      groupInstanceId: groupInstanceId,
    );

    final int milliseconds = (kafka.sessionTimeoutMs / 3).toInt();
    final Duration duration = Duration(milliseconds: milliseconds);

    _heartbeatSchedules[key] = Timer.periodic(duration, (timer) {
      try {
        // print("${DateTime.now()} | [Heartbeat] Sending heartbeat for member: $key");
        sendHeartbeatRequest(
          groupId: groupId,
          memberId: _memberId!,
          generationId: _generationId!,
          groupInstanceId: groupInstanceId,
        );
      } catch (e) {
        throw Exception(e);
      }
    });
  }

  void _startFetchSchedule({
    required String groupId,
    required String memberId,
    required String? groupInstanceId,
  }) {
    final String key = '$groupId->$memberId';
    _fetchSchedules[key]?.cancel();

    final List<Topic> topics = [];
    for (final GroupData groupData in _memberIdPartitions[key] ?? []) {
      final List<Partition> partitions = [];

      for (final GroupPartitionData part in groupData.partitions) {
        partitions.add(
          Partition(
            id: part.partitionId,
            fetchOffset: (part.offset ?? -1) + 1,
          ),
        );
      }

      topics.add(Topic(topicName: groupData.topicName, partitions: partitions));
    }

    _fetchSchedules[key] = Timer.periodic(
      const Duration(seconds: 1),
      (timer) async {
        try {
          // print("${DateTime.now()} | [FetchRequest] Sending fetch for group: $key");
          await sendFetchRequest(
            apiVersion: 8,
            topics: topics,
            groupId: groupId,
            memberId: memberId,
            groupInstanceId: groupInstanceId,
            autoCommit: true,
          );
        } catch (e) {
          throw Exception(e);
        }
      },
    );
  }

  List<RequestGroup> _buildOffsetFetchGroups({
    required String groupId,
    required String memberId,
    required List<GroupData> groupData,
  }) {
    final List<RequestGroup> result = [];

    final topics = groupData
        .map(
          (groupData) => GroupTopic(
            name: groupData.topicName,
            partitions: groupData.partitions
                .map((partitionData) => partitionData.partitionId)
                .toList(),
          ),
        )
        .toList();

    result.add(
      RequestGroup(
        groupId: groupId,
        memberEpoch: -1,
        memberId: memberId,
        topics: topics,
      ),
    );

    return result;
  }

  Future<void> sendOffsetFetchAndSync({
    required String groupId,
    required List<GroupData> groupData,
  }) async {
    final OffsetFetchResponse ofRes = await sendOffsetFetch(
      groups: _buildOffsetFetchGroups(
        groupId: groupId,
        memberId: _memberId!,
        groupData: groupData,
      ),
      async: false,
      broker: _brokerGroupLeader,
    );
    print(ofRes);
    updateMemberMetadata(res: ofRes, groupId: groupId);
  }

  void updateMemberMetadata({
    required OffsetFetchResponse res,
    required String groupId,
  }) {
    for (final ResponseGroup group in res.groups) {
      if (group.errorCode != 0) {
        throw Exception(group.errorMessage);
      }

      final List<GroupData> groupData = [];
      for (final OffsetFetchTopic topic in group.topics) {
        groupData.add(
          GroupData(
            topicName: topic.name,
            partitions: topic.partitions
                .map(
                  (partition) => GroupPartitionData(
                    partitionId: partition.id,
                    offset: partition.commitedOffset,
                  ),
                )
                .toList(),
          ),
        );
      }

      _memberIdPartitions['$groupId->$_memberId'] = groupData;
    }
  }

  void updateMemberOffsetFromLocal({
    required String groupId,
    required String memberId,
    required List<Topic> topics,
  }) {
    final String key = '$groupId->$memberId';
    if (!_memberIdPartitions.containsKey(key)) return;

    final Map<String, Map<int, int>> topicAndPartitionOffset = {
      for (final Topic topic in topics)
        topic.topicName: {
          for (final Partition part in topic.partitions ?? [])
            part.id: part.baseOffset ?? 0,
        },
    };

    final List<GroupData> cachedTopics = _memberIdPartitions[key]!;

    final List<GroupData> updatedTopics =
        cachedTopics.map((GroupData groupData) {
      if (topicAndPartitionOffset.containsKey(groupData.topicName)) {
        final Map<int, int> updatedOffsets =
            topicAndPartitionOffset[groupData.topicName]!;

        final List<GroupPartitionData> updatedPartitions =
            groupData.partitions.map((GroupPartitionData partition) {
          if (updatedOffsets.containsKey(partition.partitionId)) {
            return GroupPartitionData(
              partitionId: partition.partitionId,
              offset: updatedOffsets[partition.partitionId],
            );
          }
          return partition;
        }).toList();

        return GroupData(
          topicName: groupData.topicName,
          partitions: updatedPartitions,
        );
      }
      return groupData;
    }).toList();

    _memberIdPartitions[key] = updatedTopics;
  }
}

class GroupData {
  GroupData({required this.topicName, required this.partitions});
  final String topicName;
  final List<GroupPartitionData> partitions;
}

class GroupPartitionData {
  GroupPartitionData({required this.partitionId, this.offset});
  final int partitionId;
  final int? offset;
}
