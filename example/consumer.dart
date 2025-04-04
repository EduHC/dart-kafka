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

  KafkaConsumer consumer = KafkaConsumer(
    kafka: kafka,
    sessionTimeoutMs: 1800000,
    rebalanceTimeoutMs: 1500,
  );
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
    correlationId: null,
  );

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

  // consumer.sendHeartbeatRequest(
  //   apiVersion: 4,
  //   groupId: 'tchutchuco',
  //   generationId: res.generationId,
  //   memberId: res.memberId,
  //   groupInstanceId: null,
  // );

  await consumer.subscribe(
    topicsToSubscribe: [
      'testeomnilightvitaverse.status',
      // 'testeomnilightvitaverse.sensors'
    ],
    groupId: 'norman',
  );
  await consumer.subscribe(
    topicsToSubscribe: ['testeomnilightvitaverse.sensors'],
    groupId: 'norman',
  );
  print("**********************************************************");
  await consumer.unsubscribe(
    topicsToUnsubscribe: ['testeomnilightvitaverse.sensors'],
    groupId: 'norman',
  );

  kafka.close();
  return;
}
