import 'package:dart_kafka/src/models/components/record_header.dart';

class Record {
  final int length;
  final int attributes;
  final int timestampDelta;
  final int offsetDelta;
  final String? key;
  final String? value;
  final List<RecordHeader>? headers;

  Record(
      {required this.length,
      required this.attributes,
      required this.timestampDelta,
      required this.offsetDelta,
      this.key,
      this.value,
      this.headers});

  @override
  String toString() {
    return "Record -> length: $length, attributes: $attributes, timestampDelta: $timestampDelta, "
        "offsetDelta: $offsetDelta, key: $key, value: $value, headers: $headers";
  }
}
