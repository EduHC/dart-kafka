import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
    // Broker(host: '192.168.10.57', port: 29092),
  ];
  final KafkaClient kafka = KafkaClient(
    brokers: brokers,
    sessionTimeoutMs: 1800000,
    rebalanceTimeoutMs: 1500,
  );
  await kafka.connect();

  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("[ASYNC] Received from Stream: $event"),
    ),
  );

  final List<String> topicsOmg = [
    "TESTE_EDUARDO",
  ];

  final List<String> test = ['test-topic'];

  final List<String> topics = topicsOmg;

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);

  // admin.sendMetadataRequest(
  //   topics: topics,
  //   async: true,
  //   apiVersion: 9,
  //   clientId: 'dart-kafka',
  //   allowAutoTopicCreation: false,
  //   includeClusterAuthorizedOperations: false,
  //   includeTopicAuthorizedOperations: false,
  //   correlationId: null,
  // );

  await admin.updateTopicsMetadata(
    topics: topics,
    apiVersion: 9,
    allowAutoTopicCreation: false,
    includeClusterAuthorizedOperations: false,
    includeTopicAuthorizedOperations: false,
    correlationId: null,
  );

  // admin.sendApiVersionRequest(sock: kafka.getAnyBroker(), apiVersion: 2);

  kafka.close();
  return;
}
