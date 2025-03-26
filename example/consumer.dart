import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
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

  consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: true,
      topics: [
        Topic(topicName: 'test-topic', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  KafkaAdmin admin = kafka.admin;
  await admin.updateTopicsMetadata(
      topics: [
        'test-topic',
        'notifications',
      ],
      async: false,
      apiVersion: 9,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  dynamic res = await consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: false,
      topics: [
        Topic(topicName: 'notifications', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  print("*********************************************");
  print("[SYNC Request]: $res");

  kafka.close();
  return;
}
