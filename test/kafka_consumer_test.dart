import 'dart:typed_data';

import 'package:dart_kafka/dart_kafka.dart';
import 'package:test/test.dart';

import 'mock_kafka_client.dart';

void main() {
  group('KafkaConsumer', () {
    late MockKafkaClient mockKafkaClient;
    late KafkaConsumer consumer;

    setUp(() {
      mockKafkaClient = MockKafkaClient();
      consumer = KafkaConsumer(kafka: mockKafkaClient);
    });

    test('sendFetchRequest should enqueue a FETCH request', () async {
      final topics = [
        Topic(topicName: 'test_topic', partitions: [Partition(id: 0)]),
      ];
      await consumer.sendFetchRequest(topics: topics);

      expect(mockKafkaClient.enqueuedRequests.length, 1);
      final request = mockKafkaClient.enqueuedRequests.first;
      expect(request['apiKey'], 1); // FETCH API key
      expect(request['topicName'], 'test_topic');
      expect(request['partition'], 0);
    });

    test('sendJoinGroupRequest should enqueue a JOIN_GROUP request', () async {
      await consumer.sendJoinGroupRequest(
        groupId: 'test_group',
        sessionTimeoutMs: 30000,
        rebalanceTimeoutMs: 30000,
        memberId: 'test_member',
        protocolType: 'consumer',
        protocols: [],
      );

      expect(mockKafkaClient.enqueuedRequests.length, 1);
      final request = mockKafkaClient.enqueuedRequests.first;
      expect(request['apiKey'], 11); // JOIN_GROUP API key
    });

    test('subscribe performs the full subscription flow successfully',
        () async {
      const groupId = 'test_group';
      final topics = ['topic1', 'topic2'];
      const memberId = 'test-member-id';
      const generationId = 1;

      // 1. Mock FindGroupCoordinator response
      mockKafkaClient
        ..setResponder(
          10,
          () => FindGroupCoordinatorResponse(
            coordinators: [
              Coordinator(
                nodeId: 1,
                host: 'localhost',
                port: 9092,
                key: groupId,
                errorCode: 0,
              ),
            ],
            errorCode: 0,
            throttleTimeMs: 0,
          ),
        )

        // 2. Mock JoinGroup response
        ..setResponder(
          11,
          () => JoinGroupResponse(
            errorCode: 0,
            generationId: generationId,
            protocolType: 'consumer',
            protocolName: 'range',
            leader: memberId,
            memberId: memberId,
            members: [
              Member(
                memberId: memberId,
                metadata: MemberMetadata(version: 0, topics: topics),
              ),
            ],
            throttleTimeMs: 0,
          ),
        )

        // 3. Mock SyncGroup response
        ..setResponder(
          14,
          () => SyncGroupResponse(
            errorCode: 0,
            assignment: Assignment(
              version: 0,
              topics: [
                AssignmentTopicMetadata(
                  topicName: 'topic1',
                  partitions: [0, 1],
                ),
                AssignmentTopicMetadata(topicName: 'topic2', partitions: [0]),
              ],
              userData: Uint8List(0),
            ),
            throttleTimeMs: 0,
          ),
        )

        // 4. Mock OffsetFetch response
        ..setResponder(
          9,
          () => OffsetFetchResponse(groups: [], throttleTimeMs: 0),
        );

      await consumer.subscribe(topicsToSubscribe: topics, groupId: groupId);

      // Verify that all necessary requests were made in order
      final requestKeys =
          mockKafkaClient.enqueuedRequests.map((r) => r['apiKey']).toList();

      // FIND_COORDINATOR, JOIN_GROUP, SYNC_GROUP, OFFSET_FETCH
      expect(requestKeys, containsAllInOrder([10, 11, 14, 9]));
    });
  });
}
