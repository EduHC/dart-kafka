import 'package:dart_kafka/src/models/metadata/kafka_protocol_metadata.dart';

class Protocol {
  final String name;
  final ProtocolMetadata metadata;

  Protocol({required this.name, required this.metadata});

  @override
  String toString() {
    return "Protocol -> name: $name, metadata: $metadata";
  }
}
