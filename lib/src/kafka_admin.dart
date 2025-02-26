import 'dart:io';
import 'dart:typed_data';

import 'package:dart_kafka/src/apis/kafka_version_api.dart';
import 'package:dart_kafka/src/kafka_client.dart';
import 'package:dart_kafka/src/models/api_version_response.dart';

class KafkaAdmin {
  final KafkaClient kafka;
  final KafkaVersionApi versionApi = KafkaVersionApi();

  KafkaAdmin({required this.kafka});

  Future<void> sendApiVersionRequest() async {
    if (kafka.server == null) return;
    Socket server = kafka.server!;

    Uint8List message = versionApi.serialize(1, 'test');

    server.add(message);
    await server.flush();

    final responseBytes = await server.fold<Uint8List>(
      Uint8List(0),
      (previous, element) => Uint8List.fromList([...previous, ...element]),
    );

    KafkaApiVersionResponse versionResponse =
        versionApi.deserialize(responseBytes);
    print("Retorno: $versionResponse");
  }
}
