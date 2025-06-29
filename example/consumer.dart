import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
    Broker(host: '192.168.10.57', port: 29092),
    // Broker(host: '192.168.200.131', port: 29092),
    // Broker(host: '192.168.200.131', port: 29093),
    // Broker(host: '192.168.200.131', port: 29094),
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

  KafkaAdmin admin = kafka.getAdminClient();
  final String serialNumber = 'testeomnilightvitaverse';
  final List<String> topicsToConsume = [
    "$serialNumber.location",
    "$serialNumber.status",
    "$serialNumber.sensors",
    "$serialNumber.machine_config",
    "$serialNumber.control",
    "$serialNumber.control_key",
    "$serialNumber.api_logs",
    "$serialNumber.machine_logs",
  ];
  await admin.updateTopicsMetadata(
    topics: topicsToConsume,
    apiVersion: 9,
    allowAutoTopicCreation: false,
    includeClusterAuthorizedOperations: false,
    includeTopicAuthorizedOperations: false,
    correlationId: null,
  );

  int offset = 0;
  bool hasMore = true;

  while (hasMore) {
    FetchResponse res = await consumer.sendFetchRequest(
      async: false,
      apiVersion: 8,
      isolationLevel: 0,
      topics: [
        Topic(
          topicName: "$serialNumber.control",
          partitions: [
            Partition(id: 0, fetchOffset: offset),
          ],
        ),
      ],
    );

    print(res);

    if (res.topics.isEmpty) continue;
    Topic topic = res.topics.first;
    
    if (topic.partitions == null || topic.partitions!.isEmpty) continue;
    Partition part = topic.partitions!.first;

    if (part.batch == null) continue;
    if (part.batch!.records == null || part.batch!.records!.isEmpty) continue;

    offset += 1;
  }

  // await consumer.subscribe(
  //   topicsToSubscribe: [
  //     'testeomnilightvitaverse.status',
  //     'testeomnilightvitaverse.sensors',
  //   ],
  //   groupId: 'norman',
  // );

  // kafka.close();
  // return;
}
