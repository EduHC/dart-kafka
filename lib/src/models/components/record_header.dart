class RecordHeader {
  final String key;
  final dynamic value;

  RecordHeader({required this.key, required this.value});

  @override
  String toString() {
    return "RecordHeader -> headerKey: $key, headerValue: $value";
  }
}
