import 'dart:convert';

import 'record_header.dart';

class Record {
  Record({
    required this.attributes,
    required this.timestampDelta,
    required this.offsetDelta,
    required this.timestamp,
    this.length,
    this.key,
    this.value,
    this.headers,
  });

  factory Record.fromMap(Map<String, dynamic> map) => Record(
        length: map['length'] != null ? map['length'] as int : null,
        attributes: map['attributes'] as int,
        timestampDelta: map['timestampDelta'] as int,
        offsetDelta: map['offsetDelta'] as int,
        key: map['key'] != null ? map['key'] as String : null,
        value: map['value'] != null ? map['value'] as String : null,
        headers: map['headers'] != null
            ? List<RecordHeader>.from(
                (map['headers'] as List<dynamic>).map<RecordHeader?>(
                  (x) => RecordHeader.fromMap(x as Map<String, dynamic>),
                ),
              )
            : null,
        timestamp: map['timestamp'] as int,
      );

  factory Record.fromJson(String source) =>
      Record.fromMap(json.decode(source) as Map<String, dynamic>);
  final int? length;
  final int attributes;
  final int timestampDelta;
  final int offsetDelta;
  final String? key;
  final String? value;
  final List<RecordHeader>? headers;
  final int timestamp;

  @override
  String toString() =>
      'Record -> length: $length, attributes: $attributes, timestampDelta: $timestampDelta, '
      'offsetDelta: $offsetDelta, key: $key, value: $value, headers: $headers, timestamp: $timestamp';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'length': length,
        'attributes': attributes,
        'timestampDelta': timestampDelta,
        'offsetDelta': offsetDelta,
        'key': key,
        'value': value,
        'headers': headers?.map((x) => x.toMap()).toList(),
        'timestamp': timestamp,
      };

  String toJson() => json.encode(toMap());
}
