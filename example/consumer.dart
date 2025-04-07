import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    // Broker(host: '192.168.10.57', port: 29092),
    Broker(host: '192.168.200.131', port: 29092),
    Broker(host: '192.168.200.131', port: 29093),
    Broker(host: '192.168.200.131', port: 29094),
  ];
  final KafkaClient kafka = KafkaClient(
    brokers: brokers,
    sessionTimeoutMs: 1800000,
    rebalanceTimeoutMs: 1500,
  );
  await kafka.connect();
  KafkaConsumer consumer = kafka.getConsumerClient();

  // Listen to the Streaming events received from Async requests
  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("[ASYNC request]: $event"),
    ),
  );

  await consumer.subscribe(
    topicsToSubscribe: [
      'testeomnilightvitaverse.status',
      'testeomnilightvitaverse.sensors',
    ],
    groupId: 'norman',
  );

  // kafka.close();
  // return;
}
