import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.10.57', port: 29092),
    // Broker(host: '192.168.200.131', port: 29092),
    // Broker(host: '192.168.200.131', port: 29093),
    // Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(brokers: brokers);
  await kafka.connect();

  // Listen to the Streaming events received from Async requests
  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("[ASYNC request]: $event"),
    ),
  );

  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);

  // consumer.sendFetchRequest(
  //     clientId: 'dart-kafka',
  //     apiVersion: 8,
  //     async: true,
  //     topics: [
  //       Topic(topicName: 'test-topic', partitions: [
  //         Partition(id: 0, fetchOffset: 0),
  //       ])
  //     ]);

  KafkaAdmin admin = kafka.admin;

  await admin.updateTopicsMetadata(
      topics: [
        'testeomnilightvitaverse.status',
      ],
      async: false,
      apiVersion: 9,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  // dynamic res = await consumer.sendFetchRequest(
  //     clientId: 'dart-kafka',
  //     apiVersion: 8,
  //     async: false,
  //     topics: [
  //       Topic(topicName: 'notifications', partitions: [
  //         Partition(id: 0, fetchOffset: 0),
  //       ])
  //     ]);

  // print("*********************************************");
  // print("[SYNC Request]: $res");

  JoinGroupResponse res = await consumer.sendJoinGroupRequest(
    groupId: 'NODERED',
    sessionTimeoutMs: 1800000,
    rebalanceTimeoutMs: 1500,
    memberId: '',
    protocolType: 'consumer',
    protocols: [
      Protocol(
        name: 'RoundRobinAssigner',
        metadata: ProtocolMetadata(
          version: 0,
          topics: ['teste  omnilightvitaverse.status'],
        ),
      )
    ],
    apiVersion: 9,
    async: false,
    correlationId: null,
    groupInstanceId: null,
    reason: null,
  );

  print(res);

  if (res.errorCode != 0) {
    res = await consumer.sendJoinGroupRequest(
      groupId: 'NODERED',
      sessionTimeoutMs: 1800000,
      rebalanceTimeoutMs: 1500,
      memberId: res.memberId,
      protocolType: 'consumer',
      protocols: [
        Protocol(
          name: 'RoundRobinAssigner',
          metadata: ProtocolMetadata(
            version: 1,
            topics: ['testeomnilightvitaverse.status'],
          ),
        )
      ],
      apiVersion: 9,
      async: false,
      correlationId: null,
      groupInstanceId: null,
      reason: null,
    );
  }

  print("*****************************************");
  print(res);

  consumer.sendHeartbeatRequest(
    apiVersion: 4,
    groupId: 'NODERED',
    generationId: 1,
    memberId: res.memberId,
    groupInstanceId: null,
  );

  // consumer.sendSyncGroupRequest(
  //   memberId: '',
  //   groupId: 'tchutchuco',
  //   assignment: [
  //     AssignmentSyncGroup(
  //       memberId: 'dagon',
  //       assignment: Assignment(
  //         version: 1,
  //         topics: [
  //           AssignmentTopicData(
  //             topicName: 'testeomnilightvitaverse.status',
  //             partitions: [0],
  //           ),
  //         ],
  //       ),
  //     )
  //   ],
  //   apiVersion: 5,
  //   async: true,
  //   clientId: null,
  //   correlationId: null,
  //   generationId: 0,
  // );

  // if (res is JoinGroupResponse) {
  //   if (res.errorCode == 79) {
  //     res = await consumer.sendJoinGroupRequest(
  //       groupId: 'testeomnilightvitaverse',
  //       sessionTimeoutMs: 1800000,
  //       rebalanceTimeoutMs: 1500,
  //       memberId: res.memberId,
  //       protocolType: 'consumer',
  //       protocols: [
  //         Protocol(
  //             name: 'roundrobin',
  //             metadata: ProtocolMetadata(
  //                 version: 1, topics: ['testeomnilightvitaverse.status']))
  //       ],
  //       apiVersion: 9,
  //       async: false,
  //       correlationId: null,
  //       groupInstanceId: 'nyarlathothep',
  //       reason: null,
  //     );

  //     print(res);
  //   }
  // }

  kafka.close();
  return;
}
