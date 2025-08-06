class MessageHeader {
  MessageHeader({
    required this.offset,
    this.apiKey,
    this.apiVersion,
    this.messageLength,
    this.correlationId,
  });
  final int? correlationId;
  final int? apiKey;
  final int? apiVersion;
  final int? messageLength;
  final int offset;
}
