class MessageHeader {
  final int correlationId;
  final int? apiKey;
  final int? apiVersion;
  final int messageLength;
  final int offset;

  MessageHeader(
      {this.apiKey,
      this.apiVersion,
      required this.messageLength,
      required this.correlationId,
      required this.offset});
}
