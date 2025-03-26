import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(brokers: brokers);
  await kafka.connect();

  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("Received from Stream: $event"),
    ),
  );

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
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

  admin.sendApiVersionRequest(sock: kafka.getAnyBroker(), apiVersion: 2);
}
