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

  MetadataResponse metadata = await admin.sendMetadataRequest(
      topics: [
        'testeomnilightvitaverse.location',
        'testeomnilightvitaverse.sensor',
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

  kafka.updateTopicsBroker(metadata: metadata);
  admin.sendApiVersionRequest(sock: kafka.getAnyBroker(), apiVersion: 2);

  KafkaProducer producer = KafkaProducer(kafka: kafka);

  producer.produce(
      acks: -1,
      timeoutMs: 1500,
      topics: [
        Topic(topicName: 'testeomnilightvitaverse.status', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value: '{"teste": "tesssssskkepokasdoiasjdaisohte"}')
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
        Topic(topicName: 'testeomnilightvitaverse.sensor', partitions: [
          Partition(
              id: 0,
              batch: RecordBatch(records: [
                Record(
                    attributes: 0,
                    timestampDelta: 0,
                    offsetDelta: 0,
                    timestamp: DateTime.now().millisecondsSinceEpoch,
                    value:
                        '{"FukingSensor": "ssssssssssssensorrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"}')
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
