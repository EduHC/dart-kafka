import 'package:dart_kafka/dart_kafka.dart';
import 'package:dart_kafka/src/models/partition.dart';
import 'package:dart_kafka/src/models/topic.dart';

void main() async {
  final KafkaClient kafka = KafkaClient(host: '192.168.3.55', port: 29092);
  await kafka.connect();

  if (kafka.server == null) {
    print("Socket não conectado");
    return;
  }

  KafkaAdmin admin = KafkaAdmin(kafka: kafka);
  KafkaConsumer consumer = KafkaConsumer(kafka: kafka);

  // await admin.sendApiVersionRequest(apiVersion: 0, clientId: 'test');
  // await admin.sendMetadataRequest(
  //     topics: ['test-topic'],
  //     allowAutoTopicCreation: true,
  //     includeClusterAuthorizedOperations: true,
  //     includeTopicAuthorizedOperations: true,
  //     clientId: 'test',
  //     apiVersion: 12);

  List<Topic> topics = [
    Topic(
        topicName: 'test-topic',
        partitions: [Partition(partitionId: 0, logStartOffset: 0)])
  ];
  await consumer.sendFetchRequest(
    clientId: 'consumer',
    topics: topics,
    isolationLevel: 0,
    apiVersion: 8,
  );

  // await kafka.close();
}
