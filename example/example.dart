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

  // TESTING THE ADMIN COMMANDS
  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  // await admin.sendApiVersionRequest(apiVersion: 0, clientId: 'test');
  await admin.sendMetadataRequest(
      topics: ['test-topic'],
      allowAutoTopicCreation: true,
      includeClusterAuthorizedOperations: true,
      includeTopicAuthorizedOperations: true,
      clientId: 'dart-kafka',
      apiVersion: 12);

  // TESTING THE CONSUMER COMMANDS
  // KafkaConsumer consumer = KafkaConsumer(kafka: kafka);
  // List<Topic> topics = [
  //   Topic(topicName: 'test-topic', partitions: [
  //     Partition(partitionId: 0, logStartOffset: 0, fetchOffset: 0)
  //   ])
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
  // KafkaProducer producer = KafkaProducer(kafka: kafka);

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
  //   producerId: 15,
  //   partitionLeaderEpoch: -1,
  //   attributes: 0,
  //   baseOffset: 0,
  //   lastOffsetDelta: 0,
  //   baseTimestamp: 0,
  //   magic: 2,
  //   records: records,
  // );
  // partitions.add(Partition(partitionId: 0, logStartOffset: 0, batch: batch));
  // topics.add(Topic(topicName: 'alt', partitions: partitions));
  // await producer.produce(
  //     acks: -1,
  //     timeoutMs: 1500,
  //     topics: topics,
  //     apiVersion: 11,
  //     clientId: 'dart-kafka',
  //     correlationId: null,
  //     transactionalId: null);

  await kafka.close();
  return;
}
