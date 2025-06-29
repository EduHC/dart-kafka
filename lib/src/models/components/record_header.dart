// ignore_for_file: public_member_api_docs, sort_constructors_first
import 'dart:convert';

class RecordHeader {
  final String key;
  final dynamic value;

  RecordHeader({required this.key, required this.value});

  @override
  String toString() {
    return "RecordHeader -> headerKey: $key, headerValue: $value";
  }

  Map<String, dynamic> toMap() {
    return <String, dynamic>{
      'key': key,
      'value': value,
    };
  }

  factory RecordHeader.fromMap(Map<String, dynamic> map) {
    return RecordHeader(
      key: map['key'] as String,
      value: map['value'] as dynamic,
    );
  }

  String toJson() => json.encode(toMap());

  factory RecordHeader.fromJson(String source) =>
      RecordHeader.fromMap(json.decode(source) as Map<String, dynamic>);
}
