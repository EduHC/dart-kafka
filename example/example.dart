import 'package:dart_kafka/dart_kafka.dart';

void main() async {
  // final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
  final KafkaClient kafka = KafkaClient(host: '192.168.4.163', port: 29092);
  await kafka.connect();

  if (kafka.server == null) {
    print("Socket não conectado");
    return;
  }

  // TESTING THE ADMIN COMMANDS
  // KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  // dynamic res = await admin.sendApiVersionRequest(
  //     apiVersion: 0, clientId: 'test', async: true);

  Future.microtask(
    () => kafka.eventStream.listen(
      (event) => print("Received from Stream: $event"),
    ),
  );

  print("");
  print("");
  // print("Printed in the await response: $res");
  // await admin.sendMetadataRequest(
  //     topics: ['test-topic'],
  //     allowAutoTopicCreation: true,
  //     includeClusterAuthorizedOperations: true,
  //     includeTopicAuthorizedOperations: true,
  //     clientId: 'dart-kafka',
  //     apiVersion: 12);

  // TESTING THE CONSUMER COMMANDS
  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);
  List<Topic> topics = [
    Topic(
        topicName: 'testeomnilightvitaverse.machine_config',
        partitions: [Partition(id: 0, fetchOffset: 11)]),
    // Topic(topicName: 'testeomnilightvitaverse.sensors', partitions: [Partition(id: 0)]),
  ];
  var res = await consumer.sendFetchRequest(
      clientId: 'testeomnilightvitaverse',
      topics: topics,
      isolationLevel: 0,
      apiVersion: 8,
      async: false);

  print("$res");

  // topics = [
  //   // Topic(topicName: 'testeomnilightvitaverse.status', partitions: [Partition(id: 0)]),
  //   Topic(topicName: 'testeomnilightvitaverse.sensors', partitions: [Partition(id: 0)]),
  // ];
  // res = await consumer.sendFetchRequest(
  //     clientId: 'consumer',
  //     topics: topics,
  //     isolationLevel: 0,
  //     apiVersion: 8,
  //     async: false);

  // res = await consumer.sendListOffsetsRequest(
  //   isolationLevel: 0,
  //   topics: topics,
  //   apiVersion: 9,
  //   async: false,
  // );

  // print("$res");

  // List<Protocol> protocols = [
  //   Protocol(
  //       name: 'range',
  //       metadata: ProtocolMetadata(version: 0, topics: ['test-topic'])),
  //   Protocol(
  //       name: 'roundrobin',
  //       metadata: ProtocolMetadata(version: 0, topics: ['test-topic']))
  // ];
  // await consumer.sendJoinGroupRequest(
  //     apiVersion: 0,
  //     groupId: 'nyarlathotep',
  //     sessionTimeoutMs: 30000,
  //     rebalanceTimeoutMs: 30000,
  //     memberId: 'nyarlathotep',
  //     groupInstanceId: null,
  //     protocolType: 'consumer',
  //     correlationId: null,
  //     protocols: protocols,
  //     reason: 'Testing');

  // TESTING THE PRODUCER COMMANDS
  KafkaProducer producer = KafkaProducer(kafka: kafka);

  // await producer.initProduceId(
  //     correlationId: null,
  //     clientId: 'dart-kafka',
  //     apiVersion: 5,
  //     transactionTimeoutMs: 1500,
  //     producerId: 1,
  //     producerEpoch: 0);
  // List<Topic> topics = List.empty(growable: true);
  // List<Partition> partitions = List.empty(growable: true);
  // List<Record> records = List.empty(growable: true);
  // List<RecordHeader> headers = List.empty(growable: true);
  // headers.add(RecordHeader(key: 'request', value: 'true'));
  // headers.add(RecordHeader(key: 'persist', value: 'true'));
  // records.add(Record(
  //     attributes: 0,
  //     timestampDelta: 0,
  //     offsetDelta: 0,
  //     key: 'key',
  //     value: '{\'kartoffell\': \'potato\'}',
  //     headers: headers,
  //     timestamp: DateTime.now().millisecondsSinceEpoch));
  // records.add(Record(
  //     attributes: 0,
  //     timestampDelta: 0,
  //     offsetDelta: 0,
  //     timestamp: DateTime.now().millisecondsSinceEpoch,
  //     headers: headers,
  //     key: null,
  //     value: '{\'deutsch\': \'land\'}'));
  // RecordBatch batch = RecordBatch(
  //   producerId: 27,
  //   partitionLeaderEpoch: -1,
  //   attributes: 0,
  //   baseOffset: 0,
  //   lastOffsetDelta: 0,
  //   baseTimestamp: 0,
  //   magic: 2,
  //   records: records,
  // );
  // partitions.add(Partition(id: 0, batch: batch));
  // topics.add(Topic(topicName: 'test-topic', partitions: partitions));
  // await producer.produce(
  //     acks: -1,
  //     timeoutMs: 1500,
  //     topics: topics,
  //     apiVersion: 11,
  //     clientId: 'dart-kafka',
  //     correlationId: null,
  //     transactionalId: null,
  //     baseSequence: 3,
  //     producerId: -1);
  // await producer.describeProducer(
  //     apiVersion: 0,
  //     clientId: 'dart-kafka',
  //     correlationId: null,
  //     topics: topics);

  await kafka.close();
  return;
}
