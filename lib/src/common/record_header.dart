import 'dart:convert';

class RecordHeader {
  RecordHeader({required this.key, required this.value});

  factory RecordHeader.fromMap(Map<String, dynamic> map) => RecordHeader(
        key: map['key'] as String,
        value: map['value'] as dynamic,
      );

  factory RecordHeader.fromJson(String source) =>
      RecordHeader.fromMap(json.decode(source) as Map<String, dynamic>);
  final String key;
  final dynamic value;

  @override
  String toString() => 'RecordHeader -> headerKey: $key, headerValue: $value';

  Map<String, dynamic> toMap() => <String, dynamic>{
        'key': key,
        'value': value,
      };

  String toJson() => json.encode(toMap());
}
