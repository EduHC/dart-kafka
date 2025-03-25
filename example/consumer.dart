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
        Topic(topicName: 'testeomnilightvitaverse.status', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  dynamic res = await consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: false,
      topics: [
        Topic(topicName: 'testeomnilightvitaverse.status', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);
  print("*********************************************");
  print("[SYNC Request]: $res");

  // KafkaAdmin admin = kafka.admin;
  // await admin.updateTopicsMetadata(
  //     topics: [
  //       'testeomnilightvitaverse.sensor',
  //       'testeomnilightvitaverse.status',
  //       'testeomnilightvitaverse.machine_config',
  //       'testeomnilightvitaverse.machine_logs',
  //       'testeomnilightvitaverse.api_logs',
  //     ],
  //     async: false,
  //     apiVersion: 9,
  //     clientId: 'dart-kafka',
  //     allowAutoTopicCreation: false,
  //     includeClusterAuthorizedOperations: false,
  //     includeTopicAuthorizedOperations: false,
  //     correlationId: null);

  // dynamic
  // res = await consumer.sendFetchRequest(
  //     clientId: 'dart-kafka',
  //     apiVersion: 8,
  //     async: false,
  //     topics: [
  //       Topic(topicName: 'testeomnilightvitaverse.sensor', partitions: [
  //         Partition(id: 0, fetchOffset: 00),
  //       ])
  //     ]);

  // print("*********************************************");
  // print(res);
  kafka.close();
  return;
}
