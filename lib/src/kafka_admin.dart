import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_version_api.dart';
import 'package:dart_kafka/src/kafka_client.dart';

class KafkaAdmin {
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();

  KafkaAdmin({required this.kafka});

  Future<void> sendApiVersionRequest() async {
    if (kafka.server == null) return;
    Socket server = kafka.server!;

    Uint8List message =
        versionApi.serialize(correlationId: 1, clientId: 'test');

    print("${DateTime.now()} || [APP] Message sent: $message");
    server.add(message);
    await server.flush();

    kafka.addPendingRequest(correlationId: 1, deserializer: versionApi.deserialize);
  }
}
