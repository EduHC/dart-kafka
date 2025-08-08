import 'dart:convert';

class MessageHeader {
  MessageHeader({
    required this.offset,
    this.correlationId,
    this.apiKey,
    this.apiVersion,
    this.messageLength,
  });

  factory MessageHeader.fromMap(Map<String, dynamic> map) => MessageHeader(
        correlationId:
            map['correlationId'] != null ? map['correlationId'] as int : null,
        apiKey: map['apiKey'] != null ? map['apiKey'] as int : null,
        apiVersion: map['apiVersion'] != null ? map['apiVersion'] as int : null,
        messageLength:
            map['messageLength'] != null ? map['messageLength'] as int : null,
        offset: map['offset'] as int,
      );

  factory MessageHeader.fromJson(String source) =>
      MessageHeader.fromMap(json.decode(source) as Map<String, dynamic>);
  final int? correlationId;
  final int? apiKey;
  final int? apiVersion;
  final int? messageLength;
  final int offset;

  Map<String, dynamic> toMap() => <String, dynamic>{
        'correlationId': correlationId,
        'apiKey': apiKey,
        'apiVersion': apiVersion,
        'messageLength': messageLength,
        'offset': offset,
      };

  String toJson() => json.encode(toMap());

  @override
  String toString() =>
      'MessageHeader(correlationId: $correlationId, apiKey: $apiKey, apiVersion: $apiVersion, messageLength: $messageLength, offset: $offset)';
}
