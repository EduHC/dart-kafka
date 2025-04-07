import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  List<Broker> brokers = [
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
      apiVersion: 9,
      allowAutoTopicCreation: false,
      includeClusterAuthorizedOperations: false,
      includeTopicAuthorizedOperations: false,
      correlationId: null);

  KafkaProducer producer = KafkaProducer(kafka: kafka);

  producer.produce(
      acks: -1,
      timeoutMs: 1500,
      topics: [
        Topic(topicName: 'notifications', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value: '{"id": 1}')
              ]))
        ]),
      ],
      async: true,
      apiVersion: 11,
      clientId: 'dart-kafka',
      producerId: -1,
      attributes: 0);

  dynamic res = await producer.produce(
      acks: -1,
      timeoutMs: 1500,
      topics: [
        Topic(topicName: 'test-topic', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value: '{"test": "This is a test!"}')
              ]))
        ]),
      ],
      async: false,
      apiVersion: 11,
      clientId: 'dart-kafka',
      producerId: -1,
      attributes: 0);

  print("Sync deu: $res");

  kafka.close();
  return;
}
