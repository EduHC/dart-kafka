import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    // Broker(host: '192.168.200.131', port: 29092),
    // Broker(host: '192.168.200.131', port: 29093),
    // Broker(host: '192.168.200.131', port: 29094),
    Broker(host: '192.168.3.55', port: 29092),
  ];
  final KafkaClient kafka = KafkaClient(brokers: brokers);
  await kafka.connect();

  // Listen to the Streaming events received from Async requests
  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("Received from Stream: $event"),
    ),
  );

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);

  MetadataResponse metadata = await admin.sendMetadataRequest(
      topics: [
        'testeomnilightvitaverse.sensors',
        'testeomnilightvitaverse.status',
        'testeomnilightvitaverse.machine_config',
        'testeomnilightvitaverse.machine_logs',
        'testeomnilightvitaverse.api_logs',
      ],
      async: false,
      apiVersion: 9,
      clientId: 'dart-kafka',
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  print(metadata);
  kafka.updateTopicsBroker(metadata: metadata);

  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);

  dynamic res = await consumer.sendFetchRequest(
      clientId: 'dart-kafka',
      apiVersion: 8,
      async: false,
      topics: [
        Topic(topicName: 'testeomnilightvitaverse.status', partitions: [
          Partition(id: 0, fetchOffset: 0),
        ])
      ]);

  print("------------------------------------");
  print("------------------------------------");
  print(res);
}
