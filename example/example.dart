import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/models/components/protocol.dart';
import 'package:dart_kafka/src/models/components/protocol_metadata.dart';
import 'package:dart_kafka/src/models/components/record_batch.dart';
import 'package:dart_kafka/src/models/components/record_header.dart';
import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/topic.dart';
import 'package:dart_kafka/src/models/components/record.dart';

void main() async {
  // final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
  final KafkaClient kafka = KafkaClient(host: '192.168.4.163', port: 29092);
  await kafka.connect();

  if (kafka.server == null) {
    print("Socket não conectado");
    return;
  }

  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);

  // TESTING THE ADMIN COMMANDS

  // KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  // await admin.sendApiVersionRequest(apiVersion: 0, clientId: 'test');
  // await admin.sendMetadataRequest(
  //     topics: ['test-topic'],
  //     allowAutoTopicCreation: true,
  //     includeClusterAuthorizedOperations: true,
  //     includeTopicAuthorizedOperations: true,
  //     clientId: 'test',
  //     apiVersion: 12);

  // TESTING THE CONSUMER COMMANDS
  // List<Topic> topics = [
  //   Topic(
  //       topicName: 'test-topic',
  //       partitions: [Partition(partitionId: 0, logStartOffset: 0, fetchOffset: 1)])
  // ];
  // await consumer.sendFetchRequest(
  //   clientId: 'consumer',
  //   topics: topics,
  //   isolationLevel: 0,
  //   apiVersion: 8,
  // );

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

  List<Topic> topics = List.empty(growable: true);
  List<Partition> partitions = List.empty(growable: true);
  List<Record> records = List.empty(growable: true);
  List<RecordHeader> headers = List.empty(growable: true);

  records.add(Record(
      attributes: 0,
      timestampDelta: 0,
      offsetDelta: 0,
      key: 'chave',
      value: 'valoooor',
      headers: headers,
      timestamp: DateTime.now().millisecondsSinceEpoch));

  RecordBatch batch = RecordBatch(
    attributes: 0,
    baseOffset: 0,
    lastOffsetDelta: 0,
    baseTimestamp: 0,
    magic: 2,
    records: records,
  );

  partitions.add(Partition(partitionId: 0, logStartOffset: 0, batch: batch));
  topics.add(Topic(topicName: 'test-topic', partitions: partitions));
  await producer.produce(
      acks: 0, timeoutMs: 30000, topics: topics, apiVersion: 7);

  return;
  // await kafka.close();
}
