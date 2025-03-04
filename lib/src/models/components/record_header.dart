class RecordHeader {
  final int headerKeyLength;
  final String headerKey;
  final int headerValueLength;
  final String headerValue;

  RecordHeader(
      {required this.headerKey,
      required this.headerKeyLength,
      required this.headerValueLength,
      required this.headerValue});

  @override
  String toString() {
    return "RecordHeader -> headerKey: $headerKey, headerValue: $headerValue";
  }
}
