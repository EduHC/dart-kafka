import 'metadata/kafka_protocol_metadata.dart';

class Protocol {
  Protocol({required this.name, required this.metadata});
  final String name;
  final ProtocolMetadata metadata;

  @override
  String toString() => 'Protocol -> name: $name, metadata: $metadata';
}
